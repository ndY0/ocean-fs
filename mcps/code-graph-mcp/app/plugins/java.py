"""
Java language plugin — delegates to Eclipse JDT Language Server (jdtls).

Installation:
  Download jdtls from https://github.com/eclipse-jdtls/eclipse.jdt.ls/releases
  Unpack to /opt/jdtls (or set JDTLS_HOME env var).

  The launcher jar path is:
    /opt/jdtls/plugins/org.eclipse.equinox.launcher_*.jar

Docker note: the Dockerfile installs jdtls automatically when LANGUAGE=java.
"""

from __future__ import annotations

import os
import glob
from pathlib import Path

from ..plugin_base import LanguagePlugin, register

_JDTLS_HOME = os.environ.get("JDTLS_HOME", "/opt/jdtls")
_JDTLS_DATA = os.environ.get("JDTLS_DATA", "/tmp/jdtls-workspace-data")


def _find_launcher_jar() -> str:
    pattern = f"{_JDTLS_HOME}/plugins/org.eclipse.equinox.launcher_*.jar"
    jars = glob.glob(pattern)
    if not jars:
        raise FileNotFoundError(
            f"jdtls launcher jar not found at {pattern}. "
            f"Set JDTLS_HOME to your jdtls installation directory."
        )
    return sorted(jars)[-1]  # take the latest version if multiple


@register
class JavaPlugin(LanguagePlugin):

    @property
    def name(self) -> str:
        return "java"

    @property
    def lsp_command(self) -> list[str]:
        launcher = _find_launcher_jar()
        config_dir = f"{_JDTLS_HOME}/config_linux"
        return [
            "java",
            "-Declipse.application=org.eclipse.jdt.ls.core.id1",
            "-Dosgi.bundles.defaultStartLevel=4",
            "-Declipse.product=org.eclipse.jdt.ls.core.product",
            "-Dlog.level=ALL",
            "-noverify",
            "-Xmx1G",
            "--add-modules=ALL-SYSTEM",
            "--add-opens", "java.base/java.util=ALL-UNNAMED",
            "--add-opens", "java.base/java.lang=ALL-UNNAMED",
            "-jar", launcher,
            "-configuration", config_dir,
            "-data", _JDTLS_DATA,
        ]

    @property
    def file_patterns(self) -> list[str]:
        return ["*.java"]

    @property
    def language_id(self) -> str:
        return "java"

    @property
    def exclude_dirs(self) -> list[str]:
        return [".git", "target", "build", ".gradle"]

    @property
    def lsp_init_options(self) -> dict:
        return {
            "bundles": [],
            "workspaceFolders": [Path(self.workspace).as_uri()],
            "settings": {
                "java": {
                    "format": {"enabled": False},
                    "saveActions": {"organizeImports": False},
                    "completion": {"enabled": False},
                    "signatureHelp": {"enabled": False},
                    "contentProvider": {"preferred": None},
                    "autobuild": {"enabled": True},
                }
            },
        }

    def symbol_kind_to_graph_kind(self, lsp_kind: int) -> str:
        _JAVA_OVERRIDES = {
            5:  "class",      # Class
            11: "interface",  # Interface
            23: "struct",     # Struct (rare in Java, but map it)
            10: "enum",
            6:  "method",
            9:  "constructor",
            8:  "field",
            14: "const",      # Constant
        }
        from ..plugin_base import _DEFAULT_KIND_MAP
        return _JAVA_OVERRIDES.get(lsp_kind, _DEFAULT_KIND_MAP.get(lsp_kind, "unknown"))

    def module_path_for_file(self, file_path: str) -> str:
        """
        Java module paths are package names.
        src/main/java/com/example/Foo.java → com.example.Foo
        """
        try:
            rel = Path(file_path).relative_to(self.workspace)
        except ValueError:
            rel = Path(file_path)

        parts = list(rel.parts)

        # Strip src/main/java or src/test/java prefix
        for prefix in (
            ["src", "main", "java"],
            ["src", "test", "java"],
            ["src"],
        ):
            if parts[: len(prefix)] == prefix:
                parts = parts[len(prefix):]
                break

        if not parts:
            return ""

        # Strip .java extension from last part
        parts[-1] = Path(parts[-1]).stem
        return ".".join(parts)

    def is_test_symbol(self, symbol_name: str, lsp_kind: int, detail: str) -> bool:
        # JUnit tests are annotated with @Test — detail may contain this
        return (
            symbol_name.startswith("test")
            or "@Test" in detail
            or "@ParameterizedTest" in detail
            or "Test" in symbol_name  # catches testFoo, fooTest, TestFoo
        )

    def infer_tested_symbol(self, test_name: str) -> str | None:
        # Java: testFooBar → FooBar or fooBar
        # Remove leading "test" prefix
        if test_name.lower().startswith("test"):
            candidate = test_name[4:]
            if candidate:
                # Lowercase first char for camelCase
                return candidate[0].lower() + candidate[1:]
        # Remove trailing "Test" suffix (class-level)
        if test_name.endswith("Test"):
            return test_name[:-4]
        return None
