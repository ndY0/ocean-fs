"""
Rust language plugin — delegates to rust-analyzer.

rust-analyzer must be on PATH. Install via rustup:
  rustup component add rust-analyzer
or download from https://github.com/rust-lang/rust-analyzer/releases
"""

from __future__ import annotations

from pathlib import Path

from ..plugin_base import LanguagePlugin, register


@register
class RustPlugin(LanguagePlugin):

    @property
    def name(self) -> str:
        return "rust"

    @property
    def lsp_command(self) -> list[str]:
        return ["rust-analyzer"]

    @property
    def file_patterns(self) -> list[str]:
        return ["*.rs"]

    @property
    def language_id(self) -> str:
        return "rust"

    @property
    def exclude_dirs(self) -> list[str]:
        return [".git", "target"]

    @property
    def lsp_init_options(self) -> dict:
        return {
            # Enable call hierarchy and type hierarchy extensions
            "callHierarchy": {"enabled": True},
            "cargo": {
                "buildScripts": {"enable": True},
                "features": "all",
                # Workspace is read-only — write build artifacts here
                "targetDir": "/tmp/cargo-target",
            },
            # Run cargo check so semantic analysis is available for
            # call hierarchy and references.  The indexer waits for
            # analysis completion before querying.
            "check": {"command": "check"},
            "checkOnSave": {"enable": False},
            "diagnostics": {"enable": False},
        }

    def symbol_kind_to_graph_kind(self, lsp_kind: int) -> str:
        # rust-analyzer uses standard LSP kinds; our default map covers them all.
        # Refine: LSP "Class" (5) maps to "struct" in Rust context.
        _RUST_OVERRIDES = {
            5:  "struct",
            11: "trait",   # Interface → trait
            23: "struct",  # Struct → struct (duplicate of 5, but explicit)
        }
        from ..plugin_base import _DEFAULT_KIND_MAP
        return _RUST_OVERRIDES.get(lsp_kind, _DEFAULT_KIND_MAP.get(lsp_kind, "unknown"))

    def module_path_for_file(self, file_path: str) -> str:
        """
        Rust module paths use :: separator and strip src/ prefix.
        src/validation/rules.rs → validation::rules
        src/lib.rs → (root)
        """
        try:
            rel = Path(file_path).relative_to(self.workspace)
        except ValueError:
            rel = Path(file_path)

        parts = list(rel.parts)

        # Strip src/ prefix
        if parts and parts[0] == "src":
            parts = parts[1:]

        if not parts:
            return ""

        # Strip .rs extension and handle mod.rs / lib.rs / main.rs
        stem = Path(parts[-1]).stem
        if stem in ("mod", "lib", "main"):
            parts = parts[:-1]
        else:
            parts[-1] = stem

        return "::".join(parts)

    def is_test_symbol(self, symbol_name: str, lsp_kind: int, detail: str) -> bool:
        # rust-analyzer includes test functions as regular functions.
        # The #[test] attribute shows up in the detail field for some versions.
        return (
            symbol_name.startswith("test_")
            or symbol_name.startswith("tests_")
            or "#[test]" in detail
            or "#[tokio::test]" in detail
        )

    def infer_tested_symbol(self, test_name: str) -> str | None:
        for prefix in ("test_", "tests_"):
            if test_name.startswith(prefix):
                candidate = test_name[len(prefix):]
                if candidate:
                    return candidate
        return None
