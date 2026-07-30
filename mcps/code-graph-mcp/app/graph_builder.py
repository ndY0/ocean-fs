"""
Graph builder: translates LSP protocol responses into the language-agnostic
symbol dicts and edge tuples that the indexer feeds into DGraph.

This module is the translation layer between the LSP world and the graph world.
It knows about LSP data shapes (DocumentSymbol, CallHierarchyItem, etc.)
but nothing about any specific language.

Outputs:
  Symbol dict keys: id, name, kind, file_path, line, col,
                    signature, doc, visibility, module
  Edge tuples: (from_symbol_id, predicate, to_symbol_id)
               All IDs are already resolved — no name-resolution pass needed,
               because the LSP gives us exact locations.

Edge predicates produced here:
  calls        — from callHierarchy/outgoingCalls
  ~calls       — from callHierarchy/incomingCalls (recorded as callee→caller)
  uses_type    — from typeHierarchy/supertypes (struct implements trait)
  implements   — from typeHierarchy/supertypes
  tests        — inferred from test naming convention via plugin
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .plugin_base import LanguagePlugin


def make_symbol_id(file_uri: str, name: str, line: int) -> str:
    """Stable, unique symbol ID from LSP location data."""
    raw = f"{file_uri}::{name}::{line}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def uri_to_path(uri: str) -> str:
    """file:///path/to/file → /path/to/file"""
    if uri.startswith("file://"):
        return uri[7:]
    return uri


def path_to_uri(path: str) -> str:
    return Path(path).as_uri()


# ---------------------------------------------------------------------------
# DocumentSymbol → symbol dicts
# ---------------------------------------------------------------------------

def symbols_from_document(
    doc_symbols: list[dict],
    file_uri: str,
    plugin: "LanguagePlugin",
    parent_name: str | None = None,
) -> list[dict]:
    """
    Recursively flatten LSP DocumentSymbol (hierarchical) into a list of
    symbol dicts suitable for DGraph upsert.

    LSP DocumentSymbol has:
      name, kind, range, selectionRange, detail, children (optional)
    """
    results = []
    file_path = uri_to_path(file_uri)
    module_path = plugin.module_path_for_file(file_path)

    for sym in doc_symbols:
        name = sym.get("name", "")
        lsp_kind = sym.get("kind", 12)  # default: Function
        detail = sym.get("detail", "")
        start = sym.get("selectionRange", sym.get("range", {})).get("start", {})
        line = start.get("line", 0) + 1  # LSP is 0-based
        col = start.get("character", 0)
        range_start = sym.get("range", {}).get("start", {})
        range_line = range_start.get("line", 0) + 1
        range_col = range_start.get("character", 0)

        kind = plugin.symbol_kind_to_graph_kind(lsp_kind)
        is_test = plugin.is_test_symbol(name, lsp_kind, detail)
        visibility = _infer_visibility(sym, kind, detail, is_test)
        signature = _build_signature(name, kind, detail, parent_name)
        doc = sym.get("documentation", "") or ""

        sym_id = make_symbol_id(file_uri, name, line)

        results.append({
            "id": sym_id,
            "name": name,
            "kind": kind,
            "file_path": file_path,
            "file_uri": file_uri,
            "line": line,
            "col": col,
            "range_line": range_line,
            "range_col": range_col,
            "signature": signature[:512],
            "doc": doc[:1024],
            "visibility": visibility,
            "module": module_path,
            "is_test": is_test,
            "lsp_kind": lsp_kind,
            "detail": detail,
            "parent_name": parent_name,
        })

        # Recurse into children (methods inside classes, etc.)
        children = sym.get("children", [])
        if children:
            results.extend(
                symbols_from_document(children, file_uri, plugin, parent_name=name)
            )

    return results


# ---------------------------------------------------------------------------
# CallHierarchyItem → edges
# ---------------------------------------------------------------------------

def edges_from_call_hierarchy(
    item: dict,
    outgoing_calls: list[dict],
    incoming_calls: list[dict],
    resolve_id=None,
) -> list[tuple[str, str, str]]:
    """
    Produce (from_id, predicate, to_id) edge tuples from call hierarchy results.

    outgoing_calls items have: { to: CallHierarchyItem, fromRanges: [...] }
    incoming_calls items have: { from: CallHierarchyItem, fromRanges: [...] }

    resolve_id(name, uri) returns a symbol_id from the index, or None.
    Uses position-based ID as fallback when resolver is not available.
    """
    edges = []
    item_uri = item.get("uri", "")
    item_name = item.get("name", "")

    def _lookup(uri: str, name: str) -> str | None:
        if resolve_id:
            return resolve_id(name, uri) or resolve_id(name, uri_to_path(uri))
        return None

    from_id = _lookup(item_uri, item_name)
    if not from_id:
        item_line = item.get("range", {}).get("start", {}).get("line", 0) + 1
        from_id = make_symbol_id(item_uri, item_name, item_line)

    for call in outgoing_calls:
        to_item = call.get("to", {})
        to_uri = to_item.get("uri", "")
        to_name = to_item.get("name", "")
        to_id = _lookup(to_uri, to_name)
        if not to_id:
            to_line = to_item.get("range", {}).get("start", {}).get("line", 0) + 1
            to_id = make_symbol_id(to_uri, to_name, to_line)
        if from_id != to_id and to_id:
            edges.append((from_id, "calls", to_id))

    for call in incoming_calls:
        caller_item = call.get("from", {})
        caller_uri = caller_item.get("uri", "")
        caller_name = caller_item.get("name", "")
        caller_id = _lookup(caller_uri, caller_name)
        if not caller_id:
            caller_line = caller_item.get("range", {}).get("start", {}).get("line", 0) + 1
            caller_id = make_symbol_id(caller_uri, caller_name, caller_line)
        if caller_id != from_id and caller_id:
            edges.append((caller_id, "calls", from_id))

    return edges


# ---------------------------------------------------------------------------
# TypeHierarchyItem → edges
# ---------------------------------------------------------------------------

def edges_from_type_hierarchy(
    item: dict,
    supertypes: list[dict],
    subtypes: list[dict],
    resolve_id=None,
) -> list[tuple[str, str, str]]:
    """
    Produce implements / uses_type edges from type hierarchy results.

    supertypes: parent classes or traits this type extends/implements
    subtypes:   types that extend/implement this type

    resolve_id(name, uri) returns a symbol_id from the index, or None.
    """
    edges = []
    item_uri = item.get("uri", "")
    item_name = item.get("name", "")

    def _lookup(uri: str, name: str) -> str | None:
        if resolve_id:
            return resolve_id(name, uri) or resolve_id(name, uri_to_path(uri))
        return None

    item_id = _lookup(item_uri, item_name)
    if not item_id:
        item_line = item.get("range", {}).get("start", {}).get("line", 0) + 1
        item_id = make_symbol_id(item_uri, item_name, item_line)

    for sup in supertypes:
        sup_uri = sup.get("uri", "")
        sup_name = sup.get("name", "")
        sup_id = _lookup(sup_uri, sup_name)
        if not sup_id:
            sup_line = sup.get("range", {}).get("start", {}).get("line", 0) + 1
            sup_id = make_symbol_id(sup_uri, sup_name, sup_line)
        if item_id != sup_id and sup_id:
            edges.append((item_id, "implements", sup_id))

    for sub in subtypes:
        sub_uri = sub.get("uri", "")
        sub_name = sub.get("name", "")
        sub_id = _lookup(sub_uri, sub_name)
        if not sub_id:
            sub_line = sub.get("range", {}).get("start", {}).get("line", 0) + 1
            sub_id = make_symbol_id(sub_uri, sub_name, sub_line)
        if sub_id != item_id and sub_id:
            edges.append((sub_id, "implements", item_id))

    return edges


# ---------------------------------------------------------------------------
# Test edge inference
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Reference-based call edges (fallback when call hierarchy is unavailable)
# ---------------------------------------------------------------------------

def edges_from_references(
    ref_locations: list[dict],
    all_symbols: list[dict],
    target_sym_id: str,
) -> list[tuple[str, str, str]]:
    """
    Build calls edges from textDocument/references results.
    Each reference location is treated as a call site.
    Finds the containing symbol by line containment.
    """
    # Group symbols by file_uri, sorted by line
    by_uri: dict[str, list[dict]] = {}
    for sym in all_symbols:
        by_uri.setdefault(sym["file_uri"], []).append(sym)
    for syms in by_uri.values():
        syms.sort(key=lambda s: s["line"])

    edges = []
    for loc in ref_locations:
        loc_uri = loc.get("uri", "")
        loc_line = loc.get("range", {}).get("start", {}).get("line", 0) + 1

        syms = by_uri.get(loc_uri, [])
        if not syms:
            continue

        # Find the symbol with the largest start_line <= loc_line
        container = None
        for sym in syms:
            if sym["line"] <= loc_line:
                container = sym
            else:
                break

        if container and container.get("id") and container["id"] != target_sym_id:
            edges.append((container["id"], "calls", target_sym_id))

    return edges


# ---------------------------------------------------------------------------
# Test edge inference
# ---------------------------------------------------------------------------

def infer_test_edges(
    symbols: list[dict],
    plugin: "LanguagePlugin",
    name_to_sym_id: dict[str, str],
) -> list[tuple[str, str, str]]:
    """
    For each test symbol, infer a 'tests' edge to the symbol under test
    using the plugin's naming convention heuristic.
    """
    edges = []
    for sym in symbols:
        if not sym.get("is_test"):
            continue
        target_name = plugin.infer_tested_symbol(sym["name"])
        if target_name and target_name in name_to_sym_id:
            edges.append((sym["id"], "tests", name_to_sym_id[target_name]))
    return edges


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _infer_visibility(sym: dict, kind: str, detail: str, is_test: bool = False) -> str:
    """
    Infer visibility from LSP symbol data.
    LSP doesn't have a dedicated visibility field, so we use heuristics:
    - detail field often contains 'pub', 'public', 'private', etc.
    - module-level symbols without parent are assumed public by default
      (the graph is most useful when public API is discoverable)
    """
    if is_test:
        return "private"
    detail_lower = detail.lower()
    if "pub " in detail_lower or "public " in detail_lower:
        return "pub"
    if "private" in detail_lower or "priv " in detail_lower:
        return "private"
    if "protected" in detail_lower:
        return "protected"
    # Default: treat non-method symbols as pub, methods as private
    # (conservative heuristic — override in plugin if needed)
    if kind in ("function", "class", "struct", "trait", "interface",
                "module", "const", "enum", "type_alias"):
        return "pub"
    return "private"


def _build_signature(name: str, kind: str, detail: str, parent_name: str | None) -> str:
    """Build a human-readable signature string from LSP symbol data."""
    if detail:
        # detail often IS the signature in most LSP servers
        return f"{detail}"
    if parent_name:
        return f"{kind} {parent_name}::{name}"
    return f"{kind} {name}"
