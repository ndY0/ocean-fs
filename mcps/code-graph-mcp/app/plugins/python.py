"""
Python language plugin — delegates to python-lsp-server (pylsp).

Installation:
  pip install python-lsp-server

For better type inference, pyright can be used instead:
  pip install pyright
  Set PYTHON_LSP=pyright in environment.

pylsp has broader plugin support (rope, autopep8, flake8) but pyright
has significantly better type inference for call hierarchy resolution.
We default to pylsp for zero-config startup.
"""

from __future__ import annotations

import os
from pathlib import Path

from ..plugin_base import LanguagePlugin, register

# Set PYTHON_LSP=pyright to use pyright instead
_LSP_BACKEND = os.environ.get("PYTHON_LSP", "pylsp")


@register
class PythonPlugin(LanguagePlugin):

    @property
    def name(self) -> str:
        return "python"

    @property
    def lsp_command(self) -> list[str]:
        if _LSP_BACKEND == "pyright":
            return ["pyright-langserver", "--stdio"]
        return ["pylsp"]

    @property
    def file_patterns(self) -> list[str]:
        return ["*.py"]

    @property
    def language_id(self) -> str:
        return "python"

    @property
    def exclude_dirs(self) -> list[str]:
        return [
            ".git", "__pycache__", ".venv", "venv", "env",
            ".tox", "dist", "build", "*.egg-info",
        ]

    @property
    def lsp_init_options(self) -> dict | None:
        if _LSP_BACKEND == "pyright":
            return {
                "python": {
                    "analysis": {
                        "autoSearchPaths": True,
                        "useLibraryCodeForTypes": True,
                        "diagnosticMode": "off",
                    }
                }
            }
        return {
            "pylsp": {
                "plugins": {
                    # Disable all linting/formatting — we only need symbols
                    "pyflakes": {"enabled": False},
                    "pycodestyle": {"enabled": False},
                    "autopep8": {"enabled": False},
                    "yapf": {"enabled": False},
                    "mccabe": {"enabled": False},
                    "pylint": {"enabled": False},
                    "flake8": {"enabled": False},
                    "rope_autoimport": {"enabled": False},
                }
            }
        }

    def symbol_kind_to_graph_kind(self, lsp_kind: int) -> str:
        _PYTHON_OVERRIDES = {
            5:  "class",
            6:  "method",
            12: "function",
            8:  "field",       # instance variable
            13: "variable",
            14: "const",
        }
        from ..plugin_base import _DEFAULT_KIND_MAP
        return _PYTHON_OVERRIDES.get(lsp_kind, _DEFAULT_KIND_MAP.get(lsp_kind, "unknown"))

    def module_path_for_file(self, file_path: str) -> str:
        """
        Python module paths strip the workspace root and use . separator.
        /workspace/src/mypackage/utils.py → mypackage.utils
        /workspace/mypackage/__init__.py → mypackage
        """
        try:
            rel = Path(file_path).relative_to(self.workspace)
        except ValueError:
            rel = Path(file_path)

        parts = list(rel.parts)

        # Strip src/ prefix if present
        if parts and parts[0] == "src":
            parts = parts[1:]

        if not parts:
            return ""

        stem = Path(parts[-1]).stem
        if stem == "__init__":
            parts = parts[:-1]
        else:
            parts[-1] = stem

        return ".".join(parts)

    def is_test_symbol(self, symbol_name: str, lsp_kind: int, detail: str) -> bool:
        # pytest convention: test_* functions or Test* classes
        return (
            symbol_name.startswith("test_")
            or symbol_name.startswith("Test")
            or symbol_name == "setUp"
            or symbol_name == "tearDown"
        )

    def infer_tested_symbol(self, test_name: str) -> str | None:
        if test_name.startswith("test_"):
            candidate = test_name[5:]
            if candidate:
                return candidate
        return None
