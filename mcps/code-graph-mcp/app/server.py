"""
MCP server exposing the code graph to agents.

Unchanged tools (17):
  find_symbol, fuzzy_find, get_file_symbols, get_module_api, get_module_tree,
  get_callers, get_callees, get_type_usages, get_implementors,
  get_trait_dependents, get_tests_for, get_edit_surface, get_signature,
  get_coupling_hotspots, get_cross_module_boundary, get_stats,
  index_file, index_workspace

New tool:
  list_languages  → available plugin languages and which is active
"""

from __future__ import annotations

import asyncio
import json
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import structlog
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.context import Context

# Import plugins package to trigger @register decorators
from . import plugins  # noqa: F401

from .config import settings
from .dgraph import DGraphClient
from .graph_builder import path_to_uri
from .lsp_client import LspClient
from .plugin_base import get_plugin, available_languages
from .indexer import (
    index_file as _index_file,
    index_workspace as _index_workspace,
)
from .watcher import watch_workspace

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Singletons — created once, shared across all MCP sessions
# ---------------------------------------------------------------------------

_lsp_instance: LspClient | None = None
_dgraph_instance: DGraphClient | None = None
_plugin_instance: "LanguagePlugin | None" = None
_watcher_task: asyncio.Task | None = None
_index_task: asyncio.Task | None = None
_index_progress: dict = {"state": "idle", "files_done": 0, "files_total": 0}


async def _background_index(client: DGraphClient, lsp: LspClient, plugin: "LanguagePlugin") -> None:
    """Run _index_workspace in background, updating _index_progress."""
    global _index_progress
    _index_progress = {"state": "indexing", "files_done": 0, "files_total": 0}
    try:
        await _index_workspace(client, lsp, plugin)
        _index_progress["state"] = "done"
        log.info("server.background_index_done")
    except Exception as e:
        _index_progress["state"] = "failed"
        _index_progress["error"] = str(e)
        log.error("server.background_index_failed", error=str(e))


async def _smoke_test_hierarchy(lsp: LspClient, plugin: "LanguagePlugin") -> None:
    """After cargo-check, verify call hierarchy actually returns data."""
    log.info("server.smoke_test_start")
    workspace = settings.workspace
    for pattern in plugin.file_patterns:
        for f in Path(workspace).rglob(pattern):
            if not any(p in f.parts for p in plugin.exclude_dirs):
                probe_path = f
                break
        else:
            continue
        break
    else:
        return

    file_uri = path_to_uri(str(probe_path))
    text = probe_path.read_text(encoding="utf-8", errors="replace")
    await lsp.did_open(file_uri, plugin.language_id, text)
    try:
        raw = await lsp.document_symbols(file_uri)
        callable_kinds = {5, 6, 9, 12}
        found = False
        for sym in (raw or []):
            if sym.get("kind") in callable_kinds:
                start = sym.get("selectionRange", sym.get("range", {})).get("start", {})
                line, col = start.get("line", 0), start.get("character", 0)
                items = await lsp.prepare_call_hierarchy(file_uri, line, col)
                if items:
                    outgoing = await lsp.outgoing_calls(items[0])
                    log.info("server.smoke_test_ok",
                             sym=sym.get("name"), line=line, col=col,
                             outgoing=len(outgoing or []))
                else:
                    log.warning("server.smoke_test_null",
                                sym=sym.get("name"), line=line, col=col,
                                kind=sym.get("kind"))
                found = True
                break
        if not found:
            log.warning("server.smoke_test_no_callable",
                        symbol_count=len(raw or []),
                        kinds=[s.get("kind") for s in (raw or [])])
    finally:
        try:
            await lsp.did_close(file_uri)
        except Exception:
            pass


async def _wait_for_analysis(
    lsp: LspClient, plugin: "LanguagePlugin", max_wait: int = 300
) -> None:
    """Run cargo-check directly so rust-analyzer finds pre-built analysis.

    Falls back to passive wait if cargo is unavailable.
    """
    if not lsp.has_call_hierarchy:
        return

    workspace = settings.workspace

    # Try running cargo check directly — the most reliable approach.
    cargo_bin = shutil.which("cargo")

    if cargo_bin:
        log.info("server.running_cargo_check", workspace=workspace)
        try:
            # Workspace is readonly — write artifacts to a temp dir
            target_dir = "/tmp/cargo-target"
            proc = await asyncio.create_subprocess_exec(
                cargo_bin, "check",
                "--target-dir", target_dir,
                cwd=workspace,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=max_wait
            )
            if proc.returncode == 0:
                log.info("server.cargo_check_done")
                await _smoke_test_hierarchy(lsp, plugin)
                return
            else:
                err = (stderr or stdout or b"").decode(errors="replace")[:500]
                log.warning("server.cargo_check_failed",
                            returncode=proc.returncode, stderr=err)
        except asyncio.TimeoutError:
            log.warning("server.cargo_check_timeout")
        except FileNotFoundError:
            log.warning("server.cargo_not_found")
    else:
        log.info("server.cargo_unavailable")

    # Fallback: passive wait for rust-analyzer to finish on its own
    probe_path = None
    for pattern in plugin.file_patterns:
        for f in Path(workspace).rglob(pattern):
            if not any(p in f.parts for p in plugin.exclude_dirs):
                probe_path = f
                break
        if probe_path:
            break

    if probe_path is None:
        return

    file_uri = path_to_uri(str(probe_path))
    text = probe_path.read_text(encoding="utf-8", errors="replace")
    await lsp.did_open(file_uri, plugin.language_id, text)

    raw_symbols = await lsp.document_symbols(file_uri)
    probe_line, probe_col = 0, 0
    _CALLABLE = {5, 6, 9, 12, 23}
    for sym in (raw_symbols or []):
        if sym.get("kind") in _CALLABLE:
            start = sym.get("selectionRange", sym.get("range", {})).get("start", {})
            probe_line = start.get("line", 0)
            probe_col = start.get("character", 0)
            break

    log.info("server.waiting_for_analysis",
             file=str(probe_path), line=probe_line, col=probe_col)

    for delay in (1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0):
        await asyncio.sleep(delay)
        items = await lsp.prepare_call_hierarchy(file_uri, probe_line, probe_col)
        if items:
            # Verify semantic analysis is truly complete
            for extra in (2, 4, 8, 16):
                await asyncio.sleep(extra)
                outgoing = await lsp.outgoing_calls(items[0])
                incoming = await lsp.incoming_calls(items[0])
                if outgoing or incoming:
                    log.info("server.analysis_ready")
                    try:
                        await lsp.did_close(file_uri)
                    except Exception:
                        pass
                    return
                log.info("server.analysis_stale", extra=extra)
            log.info("server.analysis_ready_no_edges")
            try:
                await lsp.did_close(file_uri)
            except Exception:
                pass
            return
        log.info("server.analysis_not_ready", delay=delay)

    log.warning("server.analysis_timeout")
    try:
        await lsp.did_close(file_uri)
    except Exception:
        pass


async def _ensure_resources():
    """Create shared resources on first call; no-op on subsequent calls."""
    global _lsp_instance, _dgraph_instance, _plugin_instance, _watcher_task

    if _dgraph_instance is None:
        _dgraph_instance = DGraphClient()
        for attempt in range(30):
            if await _dgraph_instance.health():
                break
            log.info("dgraph.waiting", attempt=attempt)
            await asyncio.sleep(2)
        else:
            raise RuntimeError("DGraph did not become healthy in time")
        await _dgraph_instance.apply_schema()

    if _plugin_instance is None:
        _plugin_instance = get_plugin(settings.code_language, settings.workspace)
        log.info("plugin.loaded", language=_plugin_instance.name)

    if _lsp_instance is None:
        _lsp_instance = LspClient(_plugin_instance.lsp_command, settings.workspace)
        await _lsp_instance.start(
            init_options=_plugin_instance.lsp_init_options,
            timeout=settings.lsp_init_timeout,
        )

        # Wait for cargo-check to complete so call hierarchy /
        # references are available during indexing.
        await _wait_for_analysis(_lsp_instance, _plugin_instance)

        # Run initial index in background — server reports ready immediately
        _index_task = asyncio.create_task(
            _background_index(_dgraph_instance, _lsp_instance, _plugin_instance)
        )

    if _watcher_task is None:
        _watcher_task = asyncio.create_task(watch_workspace(_dgraph_instance, _lsp_instance, _plugin_instance))
        log.info("mcp.ready", language=_plugin_instance.name)


# ---------------------------------------------------------------------------
# Lifespan: DGraph + LSP + initial index + watcher
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(server: Any) -> AsyncIterator[dict]:
    await _ensure_resources()
    yield {"dgraph": _dgraph_instance, "lsp": _lsp_instance, "plugin": _plugin_instance}


mcp = MCPServer(
    "code-graph",
    lifespan=lifespan,
)


async def _dgraph(context: Context) -> DGraphClient:
    return context.request_context.lifespan_context["dgraph"]

async def _lsp(context: Context) -> LspClient:
    return context.request_context.lifespan_context["lsp"]

async def _plugin(context: Context):
    return context.request_context.lifespan_context["plugin"]


def _resolve_path(file_path: str, workspace: str) -> str:
    """Resolve a user-supplied path against the workspace root."""
    p = Path(file_path)
    ws = Path(workspace)
    if p.is_absolute():
        try:
            p.relative_to(ws)
            return str(p)
        except ValueError:
            rel = str(p).lstrip("/")
            return str(ws / rel) if rel else str(ws)
    return str(ws / p)


# ---------------------------------------------------------------------------
# Meta tool
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_languages(ctx: Context) -> str:
    """
    Return the available language plugins and the currently active one.

    Use to verify which language the graph server is indexing.
    """
    return json.dumps({
        "active": (await _plugin(ctx)).name,
        "available": available_languages(),
    }, indent=2)


# ---------------------------------------------------------------------------
# Navigational tools  (unchanged signatures)
# ---------------------------------------------------------------------------

@mcp.tool()
async def find_symbol(name: str, ctx: Context) -> str:
    """
    Exact symbol lookup by name.
    Returns all symbols with that name (there may be multiple in different modules).

    Use this first when you know the exact name of a function, struct, trait, or type.
    """
    results = await (await _dgraph(ctx)).find_symbol(name)
    return json.dumps(results, indent=2)


@mcp.tool()
async def fuzzy_find(partial: str, ctx: Context) -> str:
    """
    Find symbols whose name contains `partial` (case-insensitive, trigram match).
    Returns up to 20 results.

    Use when you have a partial name or aren't sure of the exact spelling.
    """
    results = await (await _dgraph(ctx)).fuzzy_find(partial)
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_file_symbols(file_path: str, ctx: Context) -> str:
    """
    List all symbols defined in a file, with their signatures.
    Does NOT return file content — only symbol metadata.

    Useful for getting an overview of a file before deciding which symbol to edit.
    """
    plugin = await _plugin(ctx)
    resolved = _resolve_path(file_path, plugin.workspace)
    results = await (await _dgraph(ctx)).get_file_symbols(resolved)
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_module_api(module_path: str, ctx: Context) -> str:
    """
    Return all public symbols in a module path.
    Signatures only, no bodies.

    Use to understand a module's public contract before writing code that depends on it.
    """
    results = await (await _dgraph(ctx)).get_module_api(module_path)
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_module_tree(ctx: Context) -> str:
    """
    Return the full module hierarchy of the codebase.

    Use for high-level project structure understanding (Architect / Orchestrator).
    """
    results = await (await _dgraph(ctx)).get_module_tree()
    return json.dumps(results, indent=2)


# ---------------------------------------------------------------------------
# Impact analysis tools  (unchanged)
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_callers(symbol_id: str, ctx: Context, depth: int = 1) -> str:
    """
    Return all symbols that call this one, up to `depth` hops transitively.

    Use before modifying a function signature to understand what will break.
    symbol_id comes from find_symbol or get_edit_surface.
    """
    results = await (await _dgraph(ctx)).get_callers(symbol_id, depth=min(depth, 3))
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_callees(symbol_id: str, ctx: Context, depth: int = 1) -> str:
    """
    Return all symbols that this one calls, up to `depth` hops transitively.

    Use to understand what a function depends on before refactoring it.
    """
    results = await (await _dgraph(ctx)).get_callees(symbol_id, depth=min(depth, 3))
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_type_usages(symbol_id: str, ctx: Context) -> str:
    """
    Return all symbols that reference this type (struct, enum, trait, type alias).

    Use before changing a type's fields or variants to see all affected code.
    """
    results = await (await _dgraph(ctx)).get_type_usages(symbol_id)
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_implementors(trait_symbol_id: str, ctx: Context) -> str:
    """
    Return all structs/types that implement this trait or interface.

    Use before changing a trait definition to see all implementors.
    """
    results = await (await _dgraph(ctx)).get_implementors(trait_symbol_id)
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_trait_dependents(trait_symbol_id: str, ctx: Context) -> str:
    """
    Full blast radius of a trait/interface change: implementors AND type users.

    Use when changing method signatures or adding required methods.
    """
    results = await (await _dgraph(ctx)).get_trait_dependents(trait_symbol_id)
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_tests_for(symbol_id: str, ctx: Context) -> str:
    """
    Return all test functions that cover this symbol.

    Use before and after an edit to know which tests to run.
    """
    results = await (await _dgraph(ctx)).get_tests_for(symbol_id)
    return json.dumps(results, indent=2)


# ---------------------------------------------------------------------------
# Context assembly tools  (unchanged)
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_edit_surface(symbol_id: str, ctx: Context, depth: int = 1) -> str:
    """
    The recommended first call before making any edit.

    Returns a compact pre-edit read package:
      - the symbol itself (signature, doc, file, line)
      - its direct callees
      - its direct callers
      - the types it uses
      - tests covering it

    All as signatures only — no file content read.
    """
    result = await (await _dgraph(ctx)).get_edit_surface(symbol_id, depth=depth)
    return json.dumps(result, indent=2)


@mcp.tool()
async def get_signature(symbol_id: str, ctx: Context) -> str:
    """
    Return just the signature and doc comment of a symbol.

    Use when you need to reference a signature in generated code without
    reading the full file.
    """
    data = await (await _dgraph(ctx))._query(
        f'{{ q(func: eq(symbol_id, "{symbol_id}")) '
        f'{{ symbol_id symbol_name symbol_signature symbol_doc symbol_file symbol_line }} }}'
    )
    return json.dumps(data.get("q", []), indent=2)


# ---------------------------------------------------------------------------
# Structural tools  (unchanged)
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_coupling_hotspots(ctx: Context, top_n: int = 20) -> str:
    """
    Return the most-depended-upon symbols, ranked by total in-degree.

    High in-degree = high risk to modify. Use during architecture reviews.
    """
    results = await (await _dgraph(ctx)).get_coupling_hotspots(top_n=min(top_n, 50))
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_cross_module_boundary(module_a: str, module_b: str, ctx: Context) -> str:
    """
    Return all call and type-usage edges crossing from module_a into module_b.

    Use to understand interface contracts or detect excessive coupling.
    """
    results = await (await _dgraph(ctx)).get_cross_module_boundary(module_a, module_b)
    return json.dumps(results, indent=2)


@mcp.tool()
async def get_stats(ctx: Context) -> str:
    """
    Return graph size metrics: number of indexed symbols and files.

    Use to verify the index is populated before relying on graph queries.
    """
    result = await (await _dgraph(ctx)).stats()
    result["language"] = (await _plugin(ctx)).name
    result["indexing"] = (_index_progress.get("state") == "indexing")
    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# Indexing tools  (updated signatures — now language-agnostic)
# ---------------------------------------------------------------------------

@mcp.tool()
async def index_file(file_path: str, ctx: Context) -> str:
    """
    Re-index a single source file. Language is determined by the active plugin.

    The file watcher handles this automatically on save.
    Call manually if you suspect the index is stale for a specific file.
    """
    try:
        plugin = await _plugin(ctx)
        resolved = _resolve_path(file_path, plugin.workspace)
        await _index_file(await _dgraph(ctx), await _lsp(ctx), plugin, resolved)
        return json.dumps({"status": "ok", "file": resolved})
    except Exception as e:
        return json.dumps({"status": "error", "file": file_path, "error": str(e)})


@mcp.tool()
async def index_workspace(ctx: Context) -> str:
    """
    Trigger a full workspace re-index.

    Run automatically on server startup. Call manually after large refactors
    or if the graph seems inconsistent.

    Returns immediately — index runs in background. Poll get_stats to track
    progress (indexing field will be true while running). If a previous index
    is still in progress, the request is silently ignored.
    """
    global _index_task, _index_progress
    if _index_task and not _index_task.done():
        return json.dumps({"status": "already_indexing"})
    try:
        client = await _dgraph(ctx)
        lsp = await _lsp(ctx)
        plugin = await _plugin(ctx)
        _index_task = asyncio.create_task(
            _background_index(client, lsp, plugin)
        )
        return json.dumps({"status": "started"})
    except Exception as e:
        return json.dumps({"status": "error", "error": str(e)})