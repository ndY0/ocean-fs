"""
Indexer: orchestrates LSP queries to build the full dependency graph.

Strategy for full workspace index:
  1. Walk all source files (using plugin.file_patterns)
  2. For each file: send didOpen + query documentSymbol → upsert symbols
  3. For each function/method symbol: query call hierarchy → upsert call edges
  4. For each type symbol: query type hierarchy → upsert implements edges
  5. Infer test edges from naming conventions

Strategy for incremental file index:
  1. Delete existing symbols for the file
  2. Run steps 2–5 above for just that file

The LSP client is shared across all index operations (one server process).
File batching is used to avoid overwhelming the LSP server and DGraph.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from .config import settings
from .dgraph import DGraphClient
from .graph_builder import (
    symbols_from_document,
    edges_from_call_hierarchy,
    edges_from_type_hierarchy,
    edges_from_references,
    infer_test_edges,
    make_symbol_id,
    path_to_uri,
    uri_to_path,
)
from .lsp_client import LspClient, LspError

if TYPE_CHECKING:
    from .plugin_base import LanguagePlugin

log = structlog.get_logger()

# Concurrency limits — LSP servers are sensitive to query floods
_FILE_BATCH_SIZE = 10
_EDGE_BATCH_SIZE = 10

# Serialize all indexing to avoid concurrent DGraph mutations
_index_lock = asyncio.Lock()

# Symbol kinds that support call hierarchy (functions, methods, constructors)
_CALL_HIERARCHY_KINDS = {
    "function", "method", "constructor",
}

# Symbol kinds that support type hierarchy (classes, structs, traits, interfaces)
_TYPE_HIERARCHY_KINDS = {
    "class", "struct", "trait", "interface", "enum",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def index_file(
    client: DGraphClient,
    lsp: LspClient,
    plugin: "LanguagePlugin",
    file_path: str,
) -> None:
    """(Re-)index a single source file."""
    async with _index_lock:
        await _index_file(client, lsp, plugin, file_path)


async def _index_file(
    client: DGraphClient,
    lsp: LspClient,
    plugin: "LanguagePlugin",
    file_path: str,
) -> None:
    """Internal: (re-)index a single source file (caller must hold _index_lock)."""
    log.info("indexer.indexing_file", file=file_path, language=plugin.name)
    file_uri = path_to_uri(file_path)

    # Clear stale data first
    await client.delete_file_symbols(file_path)

    symbols = await _extract_symbols(lsp, plugin, file_uri, file_path)
    if not symbols:
        log.debug("indexer.no_symbols", file=file_path)
        return

    # Upsert symbols → build lookup maps
    sym_id_map, name_to_sym_id, uri_name_to_id = await _upsert_symbols(client, symbols)

    # Build and upsert edges
    edges = await _extract_edges(lsp, plugin, symbols, name_to_sym_id, uri_name_to_id, warm_start=True)
    await _upsert_edges(client, edges)

    # Close the file that was opened by _extract_symbols
    try:
        await lsp.did_close(file_uri)
    except Exception:
        pass

    log.info(
        "indexer.file_done",
        file=file_path,
        symbols=len(symbols),
        edges=len(edges),
    )


async def index_workspace(
    client: DGraphClient,
    lsp: LspClient,
    plugin: "LanguagePlugin",
    workspace: str | None = None,
) -> None:
    """Full workspace re-index."""
    async with _index_lock:
        workspace = workspace or settings.workspace
        source_files = _find_source_files(workspace, plugin)

        log.info(
            "indexer.full_index_start",
            file_count=len(source_files),
            workspace=workspace,
            language=plugin.name,
        )

        # Phase 0: nuke all symbols so stale data from deleted/moved/renamed
        # files doesn't persist. Single-file re-indexes use delete_file_symbols
        # instead — this is only for full workspace rebuilds.
        await client.delete_all_symbols()

        # Cap concurrent LSP file-open operations.  247 files open
        # simultaneously kills rust-analyzer (memory exhaustion /
        # "invalid state" crashes during call-hierarchy queries).
        _PHASE_SEMAPHORE = asyncio.Semaphore(4)

        # Phase 1: extract all symbols — open / query / close per file.
        # Symbols are gathered first because edge resolution (Phase 2)
        # needs the full (uri, name) → id map.
        all_symbols: list[dict] = []
        name_to_sym_id: dict[str, str] = {}

        for i in range(0, len(source_files), _FILE_BATCH_SIZE):
            batch = source_files[i: i + _FILE_BATCH_SIZE]
            batch_results = await asyncio.gather(
                *[_extract_symbols_one(lsp, plugin, str(f), _PHASE_SEMAPHORE) for f in batch],
                return_exceptions=True,
            )
            for file_path, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    log.warning("indexer.symbol_extract_failed", file=str(file_path), error=str(result))
                    continue
                all_symbols.extend(result)

        _, name_to_sym_id, uri_name_to_id = await _upsert_symbols(client, all_symbols)
        log.info("indexer.symbols_upserted", count=len(all_symbols))

        # Phase 2: extract edges — re-open / query / close per file.
        all_edges: list[tuple[str, str, str]] = []

        by_file: dict[str, list[dict]] = {}
        for sym in all_symbols:
            by_file.setdefault(sym["file_uri"], []).append(sym)

        file_uris = list(by_file.keys())
        for i in range(0, len(file_uris), _FILE_BATCH_SIZE):
            batch_uris = file_uris[i: i + _FILE_BATCH_SIZE]
            batch_results = await asyncio.gather(
                *[
                    _extract_edges_one(lsp, plugin, uri, by_file[uri], name_to_sym_id, uri_name_to_id, _PHASE_SEMAPHORE)
                    for uri in batch_uris
                ],
                return_exceptions=True,
            )
            for uri, result in zip(batch_uris, batch_results):
                if isinstance(result, Exception):
                    log.warning("indexer.edge_extract_failed", file=uri, error=str(result))
                    continue
                all_edges.extend(result)

            # If the LSP subprocess died during this batch (e.g. a generated
            # file's deep trait resolution crashed rust-analyzer), restart it
            # so remaining files can still get call-hierarchy edges.
            if not lsp._alive:
                log.warning("indexer.lsp_died_restarting")
                try:
                    await lsp.restart(init_options=plugin.lsp_init_options)
                except Exception as e:
                    log.error("indexer.lsp_restart_failed", error=str(e))

        await _upsert_edges(client, all_edges)

        stats = await client.stats()
        log.info("indexer.full_index_done", edges=len(all_edges), **stats)


# ---------------------------------------------------------------------------
# Symbol extraction
# ---------------------------------------------------------------------------

async def _extract_symbols(
    lsp: LspClient,
    plugin: "LanguagePlugin",
    file_uri: str,
    file_path: str,
) -> list[dict]:
    """Open a file in the LSP and query its document symbols.
    The caller is responsible for calling did_close after edge extraction."""
    try:
        text = Path(file_path).read_text(encoding="utf-8", errors="replace")
        await lsp.did_open(file_uri, plugin.language_id, text)

        # Small delay to let the LSP server parse the file
        await asyncio.sleep(0.1)

        raw_symbols = await lsp.document_symbols(file_uri)
    except LspError as e:
        log.warning("indexer.lsp_error", file=file_path, error=str(e))
        return []
    except Exception as e:
        log.warning("indexer.extract_error", file=file_path, error=str(e))
        return []

    if not raw_symbols:
        return []

    symbols = symbols_from_document(raw_symbols, file_uri, plugin)

    # Attach file_uri to each symbol (graph_builder already sets file_path)
    for sym in symbols:
        sym["file_uri"] = file_uri

    return symbols


async def _extract_symbols_one(
    lsp: LspClient,
    plugin: "LanguagePlugin",
    file_path: str,
    sem: asyncio.Semaphore,
) -> list[dict]:
    """Open → extract symbols → close.  Called with limited concurrency."""
    file_uri = path_to_uri(file_path)
    async with sem:
        try:
            return await _extract_symbols(lsp, plugin, file_uri, file_path)
        finally:
            try:
                await lsp.did_close(file_uri)
            except Exception:
                pass


async def _extract_edges_one(
    lsp: LspClient,
    plugin: "LanguagePlugin",
    file_uri: str,
    symbols: list[dict],
    name_to_sym_id: dict[str, str],
    uri_name_to_id: dict,
    sem: asyncio.Semaphore,
) -> list[tuple[str, str, str]]:
    """Re-open → extract edges → close.  Called with limited concurrency.
    Returns [] if the LSP is already dead (crashed during a prior file)."""
    file_path = symbols[0]["file_path"]
    async with sem:
        if not lsp._alive:
            log.warning("indexer.lsp_dead_skipping", file=file_path)
            return []
        try:
            text = Path(file_path).read_text(encoding="utf-8", errors="replace")
            await lsp.did_open(file_uri, plugin.language_id, text)
            await asyncio.sleep(0.1)
            return await _extract_edges(lsp, plugin, symbols, name_to_sym_id, uri_name_to_id)
        except ConnectionError:
            log.warning("indexer.lsp_crashed_during", file=file_path)
            return []
        finally:
            try:
                await lsp.did_close(file_uri)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Edge extraction
# ---------------------------------------------------------------------------

def _is_generated_file(file_path: str) -> bool:
    """Return True if the file is auto-generated (prost, tonic, etc.)."""
    try:
        with open(file_path, encoding="utf-8", errors="replace") as f:
            first_line = f.readline()
        return "generated by" in first_line
    except Exception:
        return False


async def _extract_edges(
    lsp: LspClient,
    plugin: "LanguagePlugin",
    symbols: list[dict],
    name_to_sym_id: dict[str, str],
    uri_name_to_id: dict | None = None,
    *,
    warm_start: bool = False,
) -> list[tuple[str, str, str]]:
    """Extract call and type hierarchy edges for a list of symbols.
    
    warm_start=True skips the analysis-readiness probe and uses minimal
    delays, suitable for incremental (single-file) re-indexes where the LSP
    server is already running and analysis is fast.
    """
    edges: list[tuple[str, str, str]] = []

    if uri_name_to_id is None:
        uri_name_to_id = {}

    def resolve_id(name: str, uri: str) -> str | None:
        return uri_name_to_id.get((uri, name))

    if not symbols:
        return edges

    file_uri = symbols[0]["file_uri"]
    file_path = symbols[0]["file_path"]

    # rust-analyzer's call-hierarchy resolver crashes on deeply-nested
    # generic types in generated code (tonic, prost).  Skip call hierarchy
    # for those files — the references fallback below will still produce
    # intra-file call edges without hitting the fragile trait solver.
    generated = _is_generated_file(file_path)
    if generated:
        log.info("indexer.generated_skip_call_hierarchy", file=file_path)

    # Call hierarchy edges (functions/methods)
    if lsp.has_call_hierarchy and not generated:
        call_syms = [s for s in symbols if s["kind"] in _CALL_HIERARCHY_KINDS]
        log.info("indexer.call_hierarchy_start", file=file_path, candidates=len(call_syms))

        # Probe: wait for analysis to be ready.
        # prepare_call_hierarchy can return syntax-level items before
        # semantic analysis completes — verify with outgoing_calls.
        # Use the last callable (impl method / body fn), not the first
        # (which may be a trait declaration with no edges).
        if call_syms:
            probe = call_syms[-1]
            probe_line = probe["line"] - 1
            probe_col = probe["col"]
            ready = False

            if warm_start:
                # Incremental: LSP is already hot. Try once with a tiny delay.
                await asyncio.sleep(0.1)
                items = await lsp.prepare_call_hierarchy(file_uri, probe_line, probe_col, timeout=5)
                if items:
                    try:
                        outgoing, incoming = await asyncio.gather(
                            asyncio.wait_for(lsp.outgoing_calls(items[0]), timeout=5),
                            asyncio.wait_for(lsp.incoming_calls(items[0]), timeout=5),
                        )
                    except asyncio.TimeoutError:
                        outgoing, incoming = [], []
                    if outgoing or incoming:
                        ready = True
                    else:
                        log.debug("indexer.call_hierarchy_no_edges",
                                  sym=probe["name"], warm_start=True)
                else:
                    log.debug("indexer.call_hierarchy_empty",
                              sym=probe["name"], warm_start=True)
            else:
                # Full index: exponential backoff to let analysis stabilise.
                for delay in (0.1, 0.2, 0.5, 1):
                    await asyncio.sleep(delay)
                    items = await lsp.prepare_call_hierarchy(file_uri, probe_line, probe_col, timeout=5)
                    if items:
                        for extra in (0.2, 0.5, 1, 2):
                            await asyncio.sleep(extra)
                            try:
                                outgoing, incoming = await asyncio.gather(
                                    asyncio.wait_for(lsp.outgoing_calls(items[0]), timeout=5),
                                    asyncio.wait_for(lsp.incoming_calls(items[0]), timeout=5),
                                )
                            except asyncio.TimeoutError:
                                outgoing, incoming = [], []
                            if outgoing or incoming:
                                ready = True
                                break
                            log.debug("indexer.call_hierarchy_stale",
                                      sym=probe["name"], extra=extra)
                        else:
                            log.debug("indexer.call_hierarchy_no_edges",
                                      sym=probe["name"])
                        ready = True
                        break
                    log.debug("indexer.call_hierarchy_waiting",
                              sym=probe["name"], delay=delay)
            if ready:
                log.debug("indexer.call_hierarchy_ready", sym=probe["name"])
            else:
                log.warning("indexer.call_hierarchy_timeout", sym=probe["name"])

        for i in range(0, len(call_syms), _EDGE_BATCH_SIZE):
            batch = call_syms[i: i + _EDGE_BATCH_SIZE]
            results = await asyncio.gather(
                *[_call_edges_for_symbol(lsp, sym, resolve_id) for sym in batch],
                return_exceptions=True,
            )
            for sym, result in zip(batch, results):
                if isinstance(result, Exception):
                    log.warning("indexer.call_edge_failed", sym=sym["name"], error=str(result))
                    continue
                edges.extend(result)
        log.info("indexer.call_hierarchy_done", file=file_path,
                 edges=len([e for e in edges if e[1] == "calls"]))
    else:
        log.info("indexer.call_hierarchy_unsupported", file=file_path)

    # Type hierarchy edges (classes/structs/traits)
    if lsp.has_type_hierarchy:
        type_syms = [s for s in symbols if s["kind"] in _TYPE_HIERARCHY_KINDS]
        log.info("indexer.type_hierarchy_start", file=file_path, candidates=len(type_syms))
        for i in range(0, len(type_syms), _EDGE_BATCH_SIZE):
            batch = type_syms[i: i + _EDGE_BATCH_SIZE]
            results = await asyncio.gather(
                *[_type_edges_for_symbol(lsp, sym, resolve_id) for sym in batch],
                return_exceptions=True,
            )
            for sym, result in zip(batch, results):
                if isinstance(result, Exception):
                    log.warning("indexer.type_edge_failed", sym=sym["name"], error=str(result))
                    continue
                edges.extend(result)
        log.info("indexer.type_hierarchy_done", file=file_path,
                 edges=len([e for e in edges if e[1] == "implements"]))
    else:
        log.info("indexer.type_hierarchy_unsupported", file=file_path)

    # Fallback: use textDocument/references for any callable where call
    # hierarchy produced zero edges (or is unsupported).
    callable_syms = [s for s in symbols if s["kind"] in _CALL_HIERARCHY_KINDS]
    if callable_syms:
        existing_calls = {e[0] for e in edges if e[1] == "calls"}
        miss_syms = [s for s in callable_syms if s["id"] not in existing_calls]
        if miss_syms:
            log.info("indexer.references_fallback",
                     file=file_path, callable=len(callable_syms),
                     covered=len(callable_syms) - len(miss_syms),
                     fallback=len(miss_syms))
            for i in range(0, len(miss_syms), _EDGE_BATCH_SIZE):
                batch = miss_syms[i: i + _EDGE_BATCH_SIZE]
                results = await asyncio.gather(
                    *[_ref_edges_for_symbol(lsp, sym, symbols) for sym in batch],
                    return_exceptions=True,
                )
                for sym, result in zip(batch, results):
                    if isinstance(result, Exception):
                        log.warning("indexer.ref_edge_failed",
                                    sym=sym["name"], error=str(result))
                        continue
                    edges.extend(result)

    # Test edges (inferred from naming convention)
    test_edges = infer_test_edges(symbols, plugin, name_to_sym_id)
    edges.extend(test_edges)

    return edges


async def _ref_edges_for_symbol(
    lsp: LspClient, sym: dict, all_symbols: list[dict],
) -> list[tuple[str, str, str]]:
    """Use textDocument/references as fallback to build call edges."""
    file_uri = sym["file_uri"]
    line = sym["line"] - 1
    col = sym["col"]

    refs = await lsp.references(file_uri, line, col)
    if not refs:
        log.info("indexer.references_empty", sym=sym["name"],
                 line=sym.get("line"), col=sym.get("col"))
        return []

    result = edges_from_references(refs, all_symbols, sym["id"])
    log.info("indexer.references_result",
             sym=sym["name"], refs=len(refs), edges=len(result))
    if refs and not result:
        sym_uris = sorted({s["file_uri"] for s in all_symbols})
        ref_uris = sorted({r.get("uri", "") for r in refs})
        ref_lines = sorted(r.get("range", {}).get("start", {}).get("line", 0) + 1
                          for r in refs)
        log.info("indexer.references_no_match",
                 sym=sym["name"], ref_count=len(refs),
                 ref_uris=ref_uris, ref_lines=ref_lines,
                 sym_uris=sym_uris, target_line=sym.get("line"))
    return result


async def _call_edges_for_symbol(
    lsp: LspClient, sym: dict, resolve_id=None
) -> list[tuple[str, str, str]]:
    """Query call hierarchy for a single symbol and return edge tuples."""
    file_uri = sym["file_uri"]
    # LSP positions are 0-based — use selectionRange (identifier) position
    line = sym["line"] - 1
    col = sym["col"]

    items = await lsp.prepare_call_hierarchy(file_uri, line, col)
    if not items:
        log.info("indexer.call_hierarchy_empty",
                 sym=sym["name"], line=line, col=col,
                 file=file_uri, kind=sym.get("kind"))
        return []

    item = items[0]
    outgoing, incoming = await asyncio.gather(
        lsp.outgoing_calls(item),
        lsp.incoming_calls(item),
        return_exceptions=False,
    )

    result = edges_from_call_hierarchy(item, outgoing or [], incoming or [], resolve_id)
    log.info("indexer.call_hierarchy_success",
             sym=sym["name"], outgoing=len(outgoing or []),
             incoming=len(incoming or []), edges=len(result))
    return result


async def _type_edges_for_symbol(
    lsp: LspClient, sym: dict, resolve_id=None
) -> list[tuple[str, str, str]]:
    """Query type hierarchy for a single symbol and return edge tuples."""
    file_uri = sym["file_uri"]
    line = sym["line"] - 1
    col = sym["col"]

    items = await lsp.prepare_type_hierarchy(file_uri, line, col)
    if not items:
        return []

    item = items[0]
    supers, subs = await asyncio.gather(
        lsp.supertypes(item),
        lsp.subtypes(item),
        return_exceptions=False,
    )

    return edges_from_type_hierarchy(item, supers or [], subs or [], resolve_id)


# ---------------------------------------------------------------------------
# DGraph upsert
# ---------------------------------------------------------------------------

async def _upsert_symbols(
    client: DGraphClient,
    symbols: list[dict],
) -> tuple[dict[str, str], dict[str, str], dict]:
    """
    Upsert all symbols to DGraph.
    Returns (sym_id → uid, name → sym_id, (file_uri, name) → sym_id) maps.
    """
    sym_id_map: dict[str, str] = {}
    name_to_sym_id: dict[str, str] = {}
    uri_name_to_id: dict = {}

    for sym in symbols:
        try:
            uid = await client.upsert_symbol(sym)
            sym_id_map[sym["id"]] = uid
            # Last writer wins for name → id (prefer pub symbols)
            existing = name_to_sym_id.get(sym["name"])
            if existing is None or sym.get("visibility") == "pub":
                name_to_sym_id[sym["name"]] = sym["id"]
            uri_name_to_id[(sym["file_uri"], sym["name"])] = sym["id"]
        except Exception as e:
            log.warning("indexer.upsert_failed", sym=sym["name"], error=str(e))

    return sym_id_map, name_to_sym_id, uri_name_to_id


async def _upsert_edges(
    client: DGraphClient,
    edges: list[tuple[str, str, str]],
) -> None:
    """Upsert all edges to DGraph, resolving symbol IDs to UIDs."""
    # Batch resolve symbol IDs → UIDs
    all_sym_ids = set()
    for from_id, _, to_id in edges:
        all_sym_ids.add(from_id)
        all_sym_ids.add(to_id)

    uid_map: dict[str, str] = {}
    for sym_id in all_sym_ids:
        uid = await client.resolve_uid(sym_id)
        if uid:
            uid_map[sym_id] = uid

    # Upsert edges
    for from_id, predicate, to_id in edges:
        from_uid = uid_map.get(from_id)
        to_uid = uid_map.get(to_id)
        if from_uid and to_uid and from_uid != to_uid:
            try:
                await client.add_edge(from_uid, predicate, to_uid)
            except Exception as e:
                log.debug("indexer.edge_upsert_failed", predicate=predicate, error=str(e))


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def _find_source_files(workspace: str, plugin: "LanguagePlugin") -> list[Path]:
    """Find all source files matching the plugin's file patterns."""
    root = Path(workspace)
    exclude = set(plugin.exclude_dirs)
    files: list[Path] = []

    for pattern in plugin.file_patterns:
        for f in root.rglob(pattern):
            # Skip excluded directories anywhere in the path
            if any(part in exclude for part in f.parts):
                continue
            files.append(f)

    return sorted(set(files))  # deduplicate (a file can match multiple patterns)
