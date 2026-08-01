# OceanFS — Agent Pipeline (PIPELINE.md)

**Agents:** Always consult this file before performing codebase or documentation
lookups. It defines the **mandatory search priority** and catalogs every MCP
tool at your disposal.

---

## 1. Search Priority (DO NOT SKIP)

```
1. MCP server tools                         ← always try first
2. grep / glob (pattern & filename search)  ← fallback for full-text scan
3. Full file read (Read tool)               ← only when steps 1–2 are insufficient
4. Bash scripting (awk, jq, custom)         ← last resort
```

**Every time** you need to find code, understand a symbol, locate tests, or
discover documentation — **query the MCP servers before reaching for `grep`,
`glob`, or `read`.** The MCP servers have pre-indexed the code graph and the
documentation; they return exact results with zero scanning cost.

---

## 2. MCP `code-graph` — Code & Symbol Lookup

**Server:** `code-graph` (streamable-HTTP at `http://localhost:8765/mcp`)  
**What it indexes:** Every symbol (function, struct, trait, enum, module, …)
in the workspace, plus call edges, type-usages, implements edges, and test
associations. Language is auto-detected (currently Rust).

### 2.1 Symbol Lookup

| Tool | Signature | Use When |
|---|---|---|
| `find_symbol` | `name: str` | You know the exact symbol name (e.g. `SegmentHandle`) |
| `fuzzy_find` | `partial: str` | You have a partial name (e.g. `seg_hand`), returns up to 20 candidates |
| `get_file_symbols` | `file_path: str` | You want a structural overview of one file before editing |
| `get_signature` | `symbol_id: str` | You need the exact signature + doc comment of a known symbol |

### 2.2 Navigation & Impact Analysis

| Tool | Signature | Use When |
|---|---|---|
| `get_callers` | `symbol_id: str, depth: int = 1` | You are about to change a function's signature — see what will break |
| `get_callees` | `symbol_id: str, depth: int = 1` | You want to understand what a function depends on before refactoring |
| `get_type_usages` | `symbol_id: str` | You are about to change a struct/enum/trait — see all code that references it |
| `get_implementors` | `trait_symbol_id: str` | You are about to change a trait — see every struct implementing it |
| `get_trait_dependents` | `trait_symbol_id: str` | Full blast radius: implementors + type users of a trait |
| `get_tests_for` | `symbol_id: str` | Before or after an edit — know which tests to run |

### 2.3 Context Assembly (Pre-Edit Workflow)

| Tool | Signature | Use When |
|---|---|---|
| `get_edit_surface` | `symbol_id: str, depth: int = 1` | **First call before any code change.** Returns the symbol's signature, docs, direct callees, direct callers, used types, and covering tests — all as compact signatures. No need to read the file first. |

### 2.4 Module & Structure

| Tool | Signature | Use When |
|---|---|---|
| `get_module_api` | `module_path: str` | You are about to depend on a module — see its public contract |
| `get_module_tree` | — | High-level project structure overview |
| `get_coupling_hotspots` | `top_n: int = 20` | Architecture review — find the most-depended-upon symbols (highest change risk) |
| `get_cross_module_boundary` | `module_a: str, module_b: str` | Audit coupling — all edges crossing from module A to module B |

### 2.5 Operational

| Tool | Signature | Use When |
|---|---|---|
| `get_stats` | — | Verify the index is populated before relying on graph queries |
| `list_languages` | — | Confirm which language plugin is active |
| `index_file` | `file_path: str` | The index for a specific file seems stale |
| `index_workspace` | — | After large refactors, trigger a full re-index |

---

## 3. MCP `doc-graph` — Documentation Search

**Server:** `doc-graph` (streamable-HTTP at `http://localhost:8000/mcp`)  
**What it indexes:** Every markdown document under `docs/` in the project
(spec, ADR, guidelines, …). Tracks document status (active, superseded,
deleted) and allows historical version resolution.

### 3.1 Search & Read

| Tool | Signature | Use When |
|---|---|---|
| `search` | `query: str, type?: str, domain?: str, include_deprecated?: bool, limit?: int = 8` | You need to find documentation relevant to a natural-language query (semantic search). The `type` filter narrows to spec/adr/architecture/charter/brainstorm/review/eval. |
| `get_content` | `path: str, blob_sha?: str` | A search hit looks relevant — read the full document. If the file was deleted, pass its `blob_sha` to read the historical version. |

### 3.2 Discovery

| Tool | Signature | Use When |
|---|---|---|
| `list_active` | `type?: str, domain?: str` | You need a manifest of all active documents, optionally filtered by type/domain |

### 3.3 Maintenance (Archivist Only)

| Tool | Signature | Use When |
|---|---|---|
| `index_document` | `path: str` | A document was created or modified — (re)index it |
| `mark_deleted` | `path: str` | A document is being archived — tombstone it BEFORE `git rm` |

---

## 4. Agent Workflows (Prescribed Patterns)

### 4.1 "I need to edit a function"

```
1. get_edit_surface(symbol_id, depth=1)      # pre-edit read package
2. read the file to write the edit            # only the region shown by get_edit_surface
3. [make the edit]
4. get_tests_for(symbol_id)                  # which tests cover this?
5. run the tests                              # verify
```

### 4.2 "I need to understand a dependency chain"

```
1. get_callees(symbol_id, depth=2)           # what does this call?
2. get_callers(symbol_id, depth=2)           # what calls this?
3. get_type_usages(symbol_id)                # who uses this type?
```

### 4.3 "I'm adding a new type / module"

```
1. get_module_tree()                         # project structure overview
2. get_module_api("crate::module")           # public API of the module I'm extending
3. search("how to add a new <concept>")      # relevant docs from doc-graph
4. [write the new type]
5. index_file(file_path)                     # register the new symbols
```

### 4.4 "I need to find something but don't know where it lives"

```
1. fuzzy_find("partial_name")               # code-graph for symbols
2. search("natural language description")    # doc-graph for docs
3. grep / glob                               # only if 1+2 returned nothing
```

### 4.5 "The build is slow (RocksDB, tonic)"

**Install system RocksDB to skip C++ compilation.** `librocksdb-sys` auto-detects
it via `pkg-config` and dynamically links against the system `.so`, bypassing
the ~500 KLoC C++ source build entirely.

```bash
# One-time setup:
sudo apt install librocksdb-dev          # Ubuntu/Debian
sudo pacman -S rocksdb                   # Arch

# Verify:
pkg-config rocksdb --libs --cflags       # should output -lrocksdb

# Then build as normal — no C++ compilation:
cargo build -p oceanfs-storage           # ~5s instead of ~5min
cargo test -p oceanfs-server            # full test suite, sub-30s
```

**What changed from previous `--features testing` approach:** the mock system
was removed in commit `<sha>`. System RocksDB gives us real storage builds at
actual compilation speed. No more feature-gating, no more mock types.

---

## 5. Discipline Rules

1. **Never skip step 1.** If an MCP tool exists for your query, use it before
   falling back to grep/glob/read.
2. **Prefer `get_edit_surface` over `read`.** It returns what you actually
   need (signatures, callees, callers, types, tests) without loading hundreds
   of lines of file content into context.
3. **Check `get_tests_for` after every edit.** Run the identified tests.
4. **`index_file` after creating new source files.** The file watcher catches
   saves, but new files need a manual touch until the watcher picks them up.
5. **If MCP returns no results or errors, log the attempt, then fall back.**
   Never silently skip to grep because "MCP might not have it."
6. **For doc lookups, prefer `search` over `grep`.** Doc search is semantic
   (understands meaning), not lexical (pattern match). A query like "how does
   EC encoding work" will find §6 of the spec even though that section never
   uses those exact words.
7. **Install system RocksDB for fast local builds.** See §4.5. Without it,
    `librocksdb-sys` compiles ~500 KLoC of C++ from source on every clean
    build. A one-time `sudo apt install librocksdb-dev` drops storage crate
    builds from minutes to seconds.
8. **Re-spawn the reviewer in the same batch.** After addressing reviewer
    gaps and producing an updated Implementation Report, always spawn the
    reviewer subagent in the same tool-call batch. Never end a message with
    just a report — that leaves you waiting for a user prompt that the
    workflow already says should be automated.
