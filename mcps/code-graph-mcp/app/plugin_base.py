"""
Plugin base class and registry.

Each language plugin is responsible for three things only:
  1. Providing the LSP server command to spawn
  2. Providing the file glob patterns to watch and index
  3. Mapping LSP SymbolKind integers to our graph vocabulary

Everything else — JSON-RPC, graph building, DGraph upserts — is handled
by language-agnostic infrastructure.

To add a new language:
  1. Create plugins/<language>.py subclassing LanguagePlugin
  2. Implement the three abstract properties/methods
  3. Register it by importing it in plugins/__init__.py

The plugin registry is a simple dict keyed by the language name string
that matches the LANGUAGE env var.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class LanguagePlugin(ABC):
    """Abstract base class for a language plugin."""

    def __init__(self, workspace: str) -> None:
        self.workspace = workspace

    # -----------------------------------------------------------------------
    # Required overrides
    # -----------------------------------------------------------------------

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Short identifier, e.g. "rust", "java", "python".
        Must match the LANGUAGE env var value.
        """
        ...

    @property
    @abstractmethod
    def lsp_command(self) -> list[str]:
        """
        Command to spawn the LSP server subprocess.
        The process must speak LSP JSON-RPC on stdin/stdout.
        e.g. ["rust-analyzer"]
              ["jdtls", "-data", "/tmp/jdtls-ws"]
              ["pylsp"]
        """
        ...

    @property
    @abstractmethod
    def file_patterns(self) -> list[str]:
        """
        Glob patterns for source files this language owns.
        Used by the workspace walker and the file watcher.
        e.g. ["*.rs"]
              ["*.java"]
              ["*.py"]
        """
        ...

    @property
    @abstractmethod
    def language_id(self) -> str:
        """
        LSP languageId string for textDocument/didOpen.
        e.g. "rust", "java", "python"
        """
        ...

    # -----------------------------------------------------------------------
    # Optional overrides
    # -----------------------------------------------------------------------

    @property
    def lsp_init_options(self) -> dict | None:
        """
        Optional initializationOptions passed to the LSP server during
        initialize. Override to configure language-server-specific settings.
        """
        return None

    @property
    def exclude_dirs(self) -> list[str]:
        """
        Directory names to exclude when walking the workspace.
        Override to add language-specific build output directories.
        """
        return [".git", "node_modules"]

    def symbol_kind_to_graph_kind(self, lsp_kind: int) -> str:
        """
        Map an LSP SymbolKind integer to our graph vocabulary.
        See: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#symbolKind

        The default mapping covers the common kinds. Override to customise.
        """
        return _DEFAULT_KIND_MAP.get(lsp_kind, "unknown")

    def module_path_for_file(self, file_path: str) -> str:
        """
        Derive a logical module path from a file path.
        Override to implement language-specific module conventions.
        Default: path relative to workspace, with separators as "."
        """
        try:
            rel = Path(file_path).relative_to(self.workspace)
        except ValueError:
            rel = Path(file_path)
        parts = list(rel.parts)
        # Strip extension from last part
        if parts:
            stem = Path(parts[-1]).stem
            parts[-1] = stem
        return ".".join(parts)

    def is_test_symbol(self, symbol_name: str, lsp_kind: int, detail: str) -> bool:
        """
        Heuristic to detect if a symbol is a test function.
        Override for language-specific conventions.
        Default: checks for common test naming prefixes.
        """
        return symbol_name.startswith("test_") or symbol_name.startswith("test")

    def infer_tested_symbol(self, test_name: str) -> str | None:
        """
        Given a test function name, infer the name of the symbol under test.
        Override for language-specific naming conventions.
        Default: strips common test prefixes.
        """
        for prefix in ("test_", "tests_", "test"):
            if test_name.startswith(prefix):
                candidate = test_name[len(prefix):].strip("_")
                if candidate:
                    return candidate
        return None


# -----------------------------------------------------------------------
# LSP SymbolKind → graph kind default mapping
# -----------------------------------------------------------------------

_DEFAULT_KIND_MAP: dict[int, str] = {
    1:  "file",
    2:  "module",
    3:  "namespace",
    4:  "package",
    5:  "struct",      # Class
    6:  "method",
    7:  "property",
    8:  "field",
    9:  "constructor",
    10: "enum",
    11: "trait",       # Interface
    12: "function",
    13: "variable",
    14: "const",
    15: "string",
    16: "number",
    17: "boolean",
    18: "array",
    19: "object",
    20: "key",
    21: "null",
    22: "enum_member", # EnumMember
    23: "struct",      # Struct
    24: "event",
    25: "function",    # Operator
    26: "type_alias",  # TypeParameter
}


# -----------------------------------------------------------------------
# Plugin registry
# -----------------------------------------------------------------------

_REGISTRY: dict[str, type[LanguagePlugin]] = {}


def register(cls: type[LanguagePlugin]) -> type[LanguagePlugin]:
    """Class decorator to register a plugin."""
    _REGISTRY[cls.name.fget(cls)] = cls  # type: ignore[attr-defined]
    return cls


def get_plugin(language: str, workspace: str) -> LanguagePlugin:
    """Instantiate a plugin by language name."""
    cls = _REGISTRY.get(language)
    if cls is None:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(
            f"No plugin registered for language '{language}'. "
            f"Available: {available}"
        )
    return cls(workspace)


def available_languages() -> list[str]:
    """Return all registered language names."""
    return sorted(_REGISTRY.keys())
