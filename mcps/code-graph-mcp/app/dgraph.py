"""
DGraph client wrapping HTTP DQL endpoint.

Node types:
  Symbol  — function, method, struct, enum, trait, type_alias, module, const, static
  File    — a source file in the workspace

Edge predicates (all on Symbol unless noted):
  calls          Symbol → Symbol   (function call)
  uses_type      Symbol → Symbol   (type reference)
  implements     Symbol → Symbol   (struct impl trait)
  defined_in     Symbol → File
  contains       File   → Symbol   (inverse index, set on File node)
  tests          Symbol → Symbol   (test fn → fn under test)
  re_exports     Symbol → Symbol   (pub use)
  belongs_to     Symbol → Symbol   (method → impl block owner)
"""

from __future__ import annotations

import asyncio
import json
import structlog
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import settings

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
# --- scalars on Symbol ---
symbol_id:       string  @index(exact, hash) .
symbol_name:     string  @index(term, trigram) .
symbol_kind:     string  @index(exact) .
symbol_file:     string  @index(exact) .
symbol_line:     int     .
symbol_col:      int     .
symbol_signature: string .
symbol_doc:      string  .
symbol_visibility: string @index(exact) .
symbol_module:   string  @index(term) .

# --- scalars on File ---
file_id:         string  @index(exact, hash) .
file_path:       string  @index(exact, term) .
file_language:   string  @index(exact) .
file_lines:      int     .

# --- edges ---
calls:           [uid]   @reverse .
uses_type:       [uid]   @reverse .
implements:      [uid]   @reverse .
defined_in:      uid     @reverse .
contains:        [uid]   @reverse .
tests:           [uid]   @reverse .
re_exports:      [uid]   @reverse .
belongs_to:      uid     @reverse .
"""

# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class DGraphClient:
    def __init__(self) -> None:
        self._base = settings.dgraph_url
        self._http = httpx.AsyncClient(timeout=30.0)

    async def apply_schema(self) -> None:
        resp = await self._http.post(
            f"{self._base}/alter",
            content=SCHEMA,
            headers={"Content-Type": "application/dql"},
        )
        resp.raise_for_status()
        log.info("dgraph.schema_applied")

    async def health(self) -> bool:
        try:
            resp = await self._http.get(f"{self._base}/health", timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False

    # -----------------------------------------------------------------------
    # Low-level helpers
    # -----------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5),
        retry=retry_if_exception_type(
            (httpx.HTTPStatusError, httpx.ConnectError,
             httpx.TimeoutException, httpx.RemoteProtocolError)
        ),
    )
    async def _mutate(self, nquads: str, commit: bool = True) -> dict:
        params = {"commitNow": "true"} if commit else {}
        resp = await self._http.post(
            f"{self._base}/mutate",
            content=nquads,
            headers={"Content-Type": "application/rdf"},
            params=params,
        )
        resp.raise_for_status()
        body = resp.json()
        if "errors" in body:
            err_msgs = [e.get("message", str(e)) for e in body["errors"]]
            raise RuntimeError(f"DGraph mutation error: {err_msgs}")
        return body

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5),
        retry=retry_if_exception_type(
            (httpx.HTTPStatusError, httpx.ConnectError,
             httpx.TimeoutException, httpx.RemoteProtocolError)
        ),
    )
    async def _mutate_json(self, payload: dict, commit: bool = True) -> dict:
        params = {"commitNow": "true"} if commit else {}
        resp = await self._http.post(
            f"{self._base}/mutate",
            json=payload,
            params=params,
        )
        resp.raise_for_status()
        body = resp.json()
        if "errors" in body:
            err_msgs = [e.get("message", str(e)) for e in body["errors"]]
            raise RuntimeError(f"DGraph mutation error: {err_msgs}")
        return body

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5),
        retry=retry_if_exception_type(
            (httpx.HTTPStatusError, httpx.ConnectError,
             httpx.TimeoutException, httpx.RemoteProtocolError)
        ),
    )
    async def _query(self, dql: str, variables: dict | None = None) -> dict:
        if variables:
            resp = await self._http.post(
                f"{self._base}/query",
                json={"query": dql, "variables": variables},
            )
        else:
            resp = await self._http.post(
                f"{self._base}/query",
                content=dql,
                headers={"Content-Type": "application/dql"},
            )
        resp.raise_for_status()
        return resp.json().get("data", {})

    # -----------------------------------------------------------------------
    # Upsert helpers
    # -----------------------------------------------------------------------

    async def upsert_file(self, file_id: str, file_path: str, language: str, lines: int) -> str:
        """Upsert a File node; return its uid."""
        query = f"""
        {{
            q(func: eq(file_id, "{file_id}")) {{
                uid
            }}
        }}
        """
        data = await self._query(query)
        existing = data.get("q", [])
        if existing:
            uid = existing[0]["uid"]
            await self._mutate_json({"set": [
                {"uid": uid, "file_path": file_path,
                 "file_language": language, "file_lines": lines}
            ]})
        else:
            result = await self._mutate_json({"set": [
                {"uid": "_:file", "file_id": file_id, "file_path": file_path,
                 "file_language": language, "file_lines": lines,
                 "dgraph.type": ["File"]}
            ]})
            uid = result["data"]["uids"]["file"]
        return uid

    async def upsert_symbol(self, sym: dict) -> str:
        """
        Upsert a Symbol node.
        sym keys: id, name, kind, file_path, line, col, signature, doc,
                  visibility, module
        Returns uid.
        """
        sid = sym["id"]
        query = f"""
        {{
            q(func: eq(symbol_id, "{sid}")) {{
                uid
            }}
        }}
        """
        data = await self._query(query)
        existing = data.get("q", [])

        node = {
            "symbol_id": sym["id"],
            "symbol_name": sym["name"],
            "symbol_kind": sym["kind"],
            "symbol_file": sym.get("file_path", ""),
            "symbol_line": sym.get("line", 0),
            "symbol_col": sym.get("col", 0),
            "symbol_signature": sym.get("signature", ""),
            "symbol_doc": sym.get("doc", ""),
            "symbol_visibility": sym.get("visibility", "private"),
            "symbol_module": sym.get("module", ""),
            "dgraph.type": ["Symbol"],
        }

        if existing:
            node["uid"] = existing[0]["uid"]
        else:
            node["uid"] = "_:sym"

        result = await self._mutate_json({"set": [node]})
        if existing:
            return existing[0]["uid"]

        uid = result.get("data", {}).get("uids", {}).get("sym", "")
        if not uid:
            raise RuntimeError(
                f"Failed to resolve blank node uid for symbol {sid!r}: {result}"
            )
        return uid

    async def add_edge(self, from_uid: str, predicate: str, to_uid: str) -> None:
        await self._mutate_json({
            "set": [
                {"uid": from_uid, predicate: {"uid": to_uid}}
            ]
        })

    async def resolve_uid(self, symbol_id: str) -> str | None:
        # Retry: DGraph indexes may not be immediately ready after mutation
        for _ in range(5):
            data = await self._query(
                f'{{ q(func: eq(symbol_id, "{symbol_id}")) {{ uid }} }}'
            )
            q = data.get("q", [])
            if q:
                return q[0]["uid"]
            await asyncio.sleep(0.1)
        return None

    async def delete_all_symbols(self) -> int:
        """Remove ALL Symbol nodes — used before a full workspace rebuild."""
        data = await self._query("{ q(func: type(Symbol)) { uid } }")
        uids = [n["uid"] for n in data.get("q", [])]
        if not uids:
            return 0
        del_nquads = "\n".join(f"<{uid}> * * ." for uid in uids)
        await self._http.post(
            f"{self._base}/mutate",
            content=json.dumps({"delete": del_nquads}),
            headers={"Content-Type": "application/json"},
            params={"commitNow": "true"},
        )
        log.info("dgraph.deleted_all_symbols", count=len(uids))
        return len(uids)

    async def delete_file_symbols(self, file_path: str) -> None:
        """Remove all Symbol nodes for a given file before re-indexing it."""
        data = await self._query(
            f'{{ q(func: eq(symbol_file, "{_esc(file_path)}")) {{ uid }} }}'
        )
        uids = [n["uid"] for n in data.get("q", [])]
        if not uids:
            return
        del_nquads = "\n".join(f"<{uid}> * * ." for uid in uids)
        await self._http.post(
            f"{self._base}/mutate",
            content=json.dumps({"delete": del_nquads}),
            headers={"Content-Type": "application/json"},
            params={"commitNow": "true"},
        )
        log.info("dgraph.deleted_symbols", file=file_path, count=len(uids))

    # -----------------------------------------------------------------------
    # Query API (called by MCP tools)
    # -----------------------------------------------------------------------

    async def find_symbol(self, name: str) -> list[dict]:
        dql = """
        query find($name: string) {
            q(func: eq(symbol_name, $name)) {
                symbol_id
                symbol_name
                symbol_kind
                symbol_file
                symbol_line
                symbol_signature
                symbol_doc
                symbol_module
                symbol_visibility
            }
        }
        """
        data = await self._query(dql, {"$name": name})
        return data.get("q", [])

    async def fuzzy_find(self, partial: str) -> list[dict]:
        dql = f"""
        {{
            q(func: regexp(symbol_name, /{partial}/i), first: 20) {{
                symbol_id
                symbol_name
                symbol_kind
                symbol_file
                symbol_line
                symbol_signature
                symbol_module
            }}
        }}
        """
        data = await self._query(dql)
        return data.get("q", [])

    async def get_callers(self, symbol_id: str, depth: int = 1) -> list[dict]:
        """Transitive callers up to `depth` hops via ~calls reverse edge."""
        dql = f"""
        {{
            sym(func: eq(symbol_id, "{symbol_id}")) {{
                symbol_id symbol_name symbol_file symbol_line
                {_reverse_expand("~calls", depth, _SYMBOL_LEAF)}
            }}
        }}
        """
        data = await self._query(dql)
        return data.get("sym", [])

    async def get_callees(self, symbol_id: str, depth: int = 1) -> list[dict]:
        dql = f"""
        {{
            sym(func: eq(symbol_id, "{symbol_id}")) {{
                symbol_id symbol_name symbol_file symbol_line
                {_forward_expand("calls", depth, _SYMBOL_LEAF)}
            }}
        }}
        """
        data = await self._query(dql)
        return data.get("sym", [])

    async def get_type_usages(self, symbol_id: str) -> list[dict]:
        dql = f"""
        {{
            sym(func: eq(symbol_id, "{symbol_id}")) {{
                symbol_id symbol_name
                ~uses_type {{
                    symbol_id symbol_name symbol_kind symbol_file symbol_line
                    symbol_signature
                }}
            }}
        }}
        """
        data = await self._query(dql)
        return data.get("sym", [])

    async def get_implementors(self, trait_symbol_id: str) -> list[dict]:
        dql = f"""
        {{
            sym(func: eq(symbol_id, "{trait_symbol_id}")) {{
                symbol_id symbol_name
                ~implements {{
                    symbol_id symbol_name symbol_kind symbol_file symbol_line
                    symbol_signature
                }}
            }}
        }}
        """
        data = await self._query(dql)
        return data.get("sym", [])

    async def get_trait_dependents(self, trait_symbol_id: str) -> list[dict]:
        """All symbols that implement OR use a trait — full blast radius."""
        dql = f"""
        {{
            sym(func: eq(symbol_id, "{trait_symbol_id}")) {{
                symbol_id symbol_name
                ~implements {{
                    symbol_id symbol_name symbol_kind symbol_file symbol_line
                }}
                ~uses_type {{
                    symbol_id symbol_name symbol_kind symbol_file symbol_line
                }}
            }}
        }}
        """
        data = await self._query(dql)
        return data.get("sym", [])

    async def get_tests_for(self, symbol_id: str) -> list[dict]:
        dql = f"""
        {{
            sym(func: eq(symbol_id, "{symbol_id}")) {{
                symbol_id symbol_name
                ~tests {{
                    symbol_id symbol_name symbol_file symbol_line
                    symbol_signature
                }}
            }}
        }}
        """
        data = await self._query(dql)
        return data.get("sym", [])

    async def get_edit_surface(self, symbol_id: str, depth: int = 1) -> dict:
        """
        The minimal read surface before making an edit:
          - the symbol itself (with signature + doc)
          - its direct callees (what it calls)
          - its direct callers (what calls it)
          - the types it uses
          - tests that cover it
        All returned as signatures, no bodies.
        """
        dql = f"""
        {{
            sym(func: eq(symbol_id, "{symbol_id}")) {{
                symbol_id symbol_name symbol_kind symbol_file symbol_line
                symbol_signature symbol_doc symbol_module symbol_visibility
                calls(first: 30) {{
                    symbol_id symbol_name symbol_kind symbol_file symbol_line
                    symbol_signature
                }}
                ~calls(first: 30) {{
                    symbol_id symbol_name symbol_kind symbol_file symbol_line
                    symbol_signature
                }}
                uses_type(first: 20) {{
                    symbol_id symbol_name symbol_kind symbol_file symbol_line
                    symbol_signature
                }}
                ~tests {{
                    symbol_id symbol_name symbol_file symbol_line
                }}
            }}
        }}
        """
        data = await self._query(dql)
        results = data.get("sym", [])
        return results[0] if results else {}

    async def get_module_api(self, module_path: str) -> list[dict]:
        dql = f"""
        {{
            q(func: eq(symbol_module, "{_esc(module_path)}"))
            @filter(eq(symbol_visibility, "pub")) {{
                symbol_id symbol_name symbol_kind symbol_file symbol_line
                symbol_signature symbol_doc
            }}
        }}
        """
        data = await self._query(dql)
        return data.get("q", [])

    async def get_module_tree(self) -> list[dict]:
        dql = """
        {
            q(func: type(Symbol)) @filter(eq(symbol_kind, "module")) {
                symbol_id symbol_name symbol_module symbol_file
                contains(first: 5) { symbol_name symbol_kind }
            }
        }
        """
        data = await self._query(dql)
        return data.get("q", [])

    async def get_coupling_hotspots(self, top_n: int = 20) -> list[dict]:
        """Symbols with the highest in-degree (most depended upon)."""
        # DGraph doesn't have a native degree-sort; we do it application-side.
        dql = """
        {
            q(func: type(Symbol)) {
                symbol_id symbol_name symbol_kind symbol_file symbol_line
                in_degree: count(~calls)
                in_type_degree: count(~uses_type)
            }
        }
        """
        data = await self._query(dql)
        symbols = data.get("q", [])
        for s in symbols:
            s["_total_in_degree"] = s.get("in_degree", 0) + s.get("in_type_degree", 0)
        symbols.sort(key=lambda s: s["_total_in_degree"], reverse=True)
        return symbols[:top_n]

    async def get_cross_module_boundary(self, module_a: str, module_b: str) -> list[dict]:
        """All call/type edges crossing from module_a into module_b."""
        dql = f"""
        {{
            q(func: eq(symbol_module, "{_esc(module_a)}")) {{
                symbol_id symbol_name symbol_kind
                calls @filter(eq(symbol_module, "{_esc(module_b)}")) {{
                    symbol_id symbol_name symbol_kind symbol_file symbol_line
                    symbol_signature
                }}
                uses_type @filter(eq(symbol_module, "{_esc(module_b)}")) {{
                    symbol_id symbol_name symbol_kind symbol_file symbol_line
                    symbol_signature
                }}
            }}
        }}
        """
        data = await self._query(dql)
        results = data.get("q", [])
        # Filter out nodes with no cross-module edges
        return [r for r in results if r.get("calls") or r.get("uses_type")]

    async def get_file_symbols(self, file_path: str) -> list[dict]:
        dql = f"""
        {{
            q(func: eq(symbol_file, "{_esc(file_path)}")) {{
                symbol_id symbol_name symbol_kind symbol_line symbol_col
                symbol_signature symbol_doc symbol_visibility symbol_module
            }}
        }}
        """
        data = await self._query(dql)
        return data.get("q", [])

    async def stats(self) -> dict:
        dql = """
        {
            symbols(func: type(Symbol)) { count(uid) }
            files(func: type(File)) { count(uid) }
        }
        """
        data = await self._query(dql)
        symbol_count = data.get("symbols", [{}])[0].get("count", 0)
        file_count = data.get("files", [{}])[0].get("count", 0)
        if not symbol_count:
            # Fallback: count nodes that have a symbol_id predicate
            fallback = await self._query(
                "{ q(func: has(symbol_id)) { count(uid) } }"
            )
            symbol_count = fallback.get("q", [{}])[0].get("count", 0)
        return {"symbols": symbol_count, "files": file_count}

    async def close(self) -> None:
        await self._http.aclose()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SYMBOL_LEAF = "symbol_id symbol_name symbol_kind symbol_file symbol_line symbol_signature"


def _reverse_expand(edge: str, depth: int, leaf_fields: str) -> str:
    if depth <= 0:
        return ""
    inner = _reverse_expand(edge, depth - 1, leaf_fields)
    block = f"{leaf_fields}\n{inner}" if inner else leaf_fields
    return f"{edge} {{\n{block}\n}}"


def _forward_expand(edge: str, depth: int, leaf_fields: str) -> str:
    if depth <= 0:
        return ""
    inner = _forward_expand(edge, depth - 1, leaf_fields)
    block = f"{leaf_fields}\n{inner}" if inner else leaf_fields
    return f"{edge} {{\n{block}\n}}"


def _esc(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
         .replace('"', '\\"')
         .replace("\n", "\\n")
         .replace("\r", "\\r")
         .replace("\t", "\\t")
    )


def _symbol_nquads(sym: dict) -> list[tuple[str, str]]:
    """Return (predicate, rdf_object) pairs for a Symbol dict."""
    pairs = [
        ("<symbol_id>", f'"{_esc(sym["id"])}"'),
        ("<symbol_name>", f'"{_esc(sym["name"])}"'),
        ("<symbol_kind>", f'"{sym["kind"]}"'),
        ("<symbol_file>", f'"{_esc(sym.get("file_path", ""))}"'),
        ("<symbol_line>", f'"{sym.get("line", 0)}"^^<xs:int>'),
        ("<symbol_col>", f'"{sym.get("col", 0)}"^^<xs:int>'),
        ("<symbol_signature>", f'"{_esc(sym.get("signature", ""))}"'),
        ("<symbol_doc>", f'"{_esc(sym.get("doc", ""))}"'),
        ("<symbol_visibility>", f'"{sym.get("visibility", "private")}"'),
        ("<symbol_module>", f'"{_esc(sym.get("module", ""))}"'),
    ]
    return pairs
