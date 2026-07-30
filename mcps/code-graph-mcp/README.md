# code-graph-mcp v2 — LSP-backed, language-agnostic

Structural dependency graph MCP server backed by DGraph.
Symbol and edge extraction is delegated to LSP servers — no language-specific
parsing code in this repository.

## What changed from v1

| v1 (tree-sitter) | v2 (LSP) |
|------------------|----------|
| Rust-only | Rust, Java, Python (extensible) |
| tree-sitter extracts syntax | LSP extracts semantics (macro-expanded, cross-file resolved) |
| Name-based edge resolution (fragile) | Location-based edge resolution (exact) |
| `extractor.py` (700 lines Rust-specific) | `plugin_base.py` + small plugin per language |
| No call hierarchy | `callHierarchy/incomingCalls` + `outgoingCalls` |
| No type hierarchy | `typeHierarchy/supertypes` + `subtypes` |

All 17 MCP tools are identical. Agent prompts and PIPELINE.md do not change.

---

## Supported languages

| Language | LSP Server | Install |
|----------|-----------|---------|
| `rust` | rust-analyzer | Installed automatically in Docker |
| `java` | Eclipse JDT LS | Installed automatically in Docker |
| `python` | pylsp or pyright | Installed automatically in Docker |

---

## Setup

### 1. Configure

```bash
cp .env.example .env
# Set PROJECT_ROOT and LANGUAGE
```

### 2. Build and start

```bash
# The LANGUAGE build arg controls which LSP binary is baked into the image
docker compose build
docker compose up -d
```

To switch languages, change `LANGUAGE` in `.env` and rebuild:
```bash
LANGUAGE=java docker compose build && docker compose up -d
```

### 3. Register with Claude Code

```bash
claude mcp add \
  --transport http \
  --scope project \
  code-graph \
  http://localhost:8765/mcp
```

---

## Adding a new language

1. Create `server/app/plugins/<language>.py` subclassing `LanguagePlugin`
2. Implement the four required properties:
   - `name` — identifier string (e.g. `"typescript"`)
   - `lsp_command` — how to spawn the LSP server (e.g. `["typescript-language-server", "--stdio"]`)
   - `file_patterns` — globs to watch (e.g. `["*.ts", "*.tsx"]`)
   - `language_id` — LSP languageId string (e.g. `"typescript"`)
3. Add the import to `server/app/plugins/__init__.py`
4. Add LSP binary installation to `server/Dockerfile` under a new `LANGUAGE` branch
5. Set `LANGUAGE=<your_language>` in `.env`

Optionally override:
- `symbol_kind_to_graph_kind(lsp_kind)` — custom kind mapping
- `module_path_for_file(file_path)` — language-specific module naming
- `is_test_symbol(name, kind, detail)` — test detection heuristic
- `infer_tested_symbol(test_name)` — tested symbol name inference
- `exclude_dirs` — directories to skip
- `lsp_init_options` — LSP server configuration

---

## Architecture

```
MCP server (language-agnostic)
├── lsp_client.py       Generic async LSP JSON-RPC client (stdin/stdout)
├── plugin_base.py      Abstract base class + @register decorator + registry
├── plugins/
│   ├── __init__.py     Imports all plugins (triggers @register)
│   ├── rust.py         rust-analyzer
│   ├── java.py         Eclipse JDT LS
│   └── python.py       pylsp / pyright
├── graph_builder.py    LSP responses → symbol dicts + edge tuples
├── indexer.py          Orchestrates LSP queries → DGraph (language-agnostic)
├── watcher.py          File events → LSP didChange → incremental re-index
├── dgraph.py           DGraph client (unchanged from v1)
├── config.py           Settings (adds LANGUAGE, LSP_INIT_TIMEOUT)
└── server.py           FastMCP: lifespan wires plugin+LSP, 18 tools
```

---

## Why LSP over tree-sitter

tree-sitter extracts syntax — it sees what the source text says.
LSP extracts semantics — it sees what the compiler resolves.

The difference is critical for edges:
- Macro-generated symbols are visible to LSP (invisible to tree-sitter)
- Cross-file type resolution is exact in LSP (name-guessed in tree-sitter)
- Call hierarchy is a first-class LSP concept (approximated in tree-sitter)
- Trait implementations are resolved by the type checker (not a name heuristic)

---

## Ports

| Service | Port |
|---------|------|
| DGraph Zero (HTTP) | 6080 |
| DGraph Alpha (DQL/GraphQL) | 8080 |
| MCP server | 8765 |

---

## Known limitations

- One language per running instance. To index a polyglot repo, run two
  instances on different ports with different LANGUAGE settings.
- LSP startup is slower than tree-sitter (10–90s depending on language and
  project size). The full index runs at startup; subsequent queries hit DGraph.
- jdtls is the heaviest: give it at least 1GB of RAM and 120s startup timeout.
