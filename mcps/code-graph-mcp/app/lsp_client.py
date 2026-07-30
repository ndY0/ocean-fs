"""
Async LSP client — speaks JSON-RPC 2.0 over a subprocess's stdin/stdout.

Responsibilities:
  - Spawn the LSP server process
  - Send requests and notifications (fire-and-forget)
  - Route incoming responses and server-initiated notifications
  - Implement the LSP initialize / initialized handshake
  - Expose high-level helpers for the methods we actually use:
      textDocument/documentSymbol
      textDocument/references
      callHierarchy/incomingCalls
      callHierarchy/outgoingCalls
      typeHierarchy/supertypes
      typeHierarchy/subtypes
      textDocument/prepareCallHierarchy
      textDocument/prepareTypeHierarchy
      workspace/symbol

This module is intentionally language-agnostic. It knows nothing about Rust,
Java, or any other language. Language-specific concerns live in the plugins.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger()

# LSP content-length framing
_HEADER = "Content-Length: {}\r\n\r\n"


class LspError(Exception):
    """Raised when an LSP request returns an error response."""
    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"LSP error {code}: {message}")
        self.code = code
        self.message = message


class LspClient:
    """
    Async LSP client for a subprocess language server.

    Usage:
        client = LspClient(["rust-analyzer"], workspace="/path/to/project")
        await client.start()
        symbols = await client.document_symbols("file:///path/to/file.rs")
        await client.stop()
    """

    def __init__(self, command: list[str], workspace: str) -> None:
        self._command = command
        self._workspace = workspace
        self._process: asyncio.subprocess.Process | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future] = {}
        self._reader_task: asyncio.Task | None = None
        self._write_lock = asyncio.Lock()
        self._server_capabilities: dict = {}

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def start(self, init_options: dict | None = None, timeout: int = 60) -> None:
        """Spawn the LSP server and complete the initialize handshake."""
        log.info("lsp.starting", command=self._command[0], workspace=self._workspace)

        self._process = await asyncio.create_subprocess_exec(
            *self._command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self._workspace,
        )

        # Start background reader before sending initialize
        self._reader_task = asyncio.create_task(self._read_loop())

        # LSP initialize
        init_params = {
            "processId": os.getpid(),
            "rootUri": _path_to_uri(self._workspace),
            "capabilities": _CLIENT_CAPABILITIES,
            "workspaceFolders": [
                {"uri": _path_to_uri(self._workspace), "name": Path(self._workspace).name}
            ],
        }
        if init_options:
            init_params["initializationOptions"] = init_options

        result = await asyncio.wait_for(
            self._request("initialize", init_params),
            timeout=timeout,
        )
        self._server_capabilities = result.get("capabilities", {}) if result else {}

        # Notify server that client is ready
        await self._notify("initialized", {})
        log.info("lsp.initialized",
                 command=self._command[0],
                 capabilities={
                     "callHierarchy": self.has_call_hierarchy,
                     "typeHierarchy": self.has_type_hierarchy,
                     "keys": sorted(self._server_capabilities.keys()),
                 })

    async def stop(self) -> None:
        """Graceful shutdown: send shutdown/exit, then terminate."""
        try:
            await asyncio.wait_for(self._request("shutdown", None), timeout=5)
            await self._notify("exit", None)
        except Exception:
            pass
        if self._process:
            try:
                self._process.terminate()
                await asyncio.wait_for(self._process.wait(), timeout=3)
            except Exception:
                self._process.kill()
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

    # -----------------------------------------------------------------------
    # High-level LSP method wrappers
    # -----------------------------------------------------------------------

    @property
    def has_call_hierarchy(self) -> bool:
        return bool(self._server_capabilities.get("callHierarchyProvider"))

    @property
    def has_type_hierarchy(self) -> bool:
        return bool(self._server_capabilities.get("typeHierarchyProvider"))

    async def workspace_symbols(self, query: str = "") -> list[dict]:
        """workspace/symbol — all symbols matching query (empty = all)."""
        result = await self._request("workspace/symbol", {"query": query})
        return result or []

    async def document_symbols(self, file_uri: str) -> list[dict]:
        """textDocument/documentSymbol — all symbols in a file."""
        result = await self._request(
            "textDocument/documentSymbol",
            {"textDocument": {"uri": file_uri}},
        )
        return result or []

    async def references(self, file_uri: str, line: int, character: int) -> list[dict]:
        """textDocument/references — all reference locations for a position."""
        result = await self._request(
            "textDocument/references",
            {
                "textDocument": {"uri": file_uri},
                "position": {"line": line, "character": character},
                "context": {"includeDeclaration": False},
            },
        )
        return result or []

    async def prepare_call_hierarchy(
        self, file_uri: str, line: int, character: int
    ) -> list[dict]:
        """textDocument/prepareCallHierarchy — prepare call hierarchy item."""
        result = await self._request(
            "textDocument/prepareCallHierarchy",
            {
                "textDocument": {"uri": file_uri},
                "position": {"line": line, "character": character},
            },
        )
        if result is None:
            log.info("lsp.prepare_call_hierarchy_null",
                     uri=file_uri, line=line, character=character)
        return result or []

    async def incoming_calls(self, item: dict) -> list[dict]:
        """callHierarchy/incomingCalls — callers of a call hierarchy item."""
        result = await self._request("callHierarchy/incomingCalls", {"item": item})
        return result or []

    async def outgoing_calls(self, item: dict) -> list[dict]:
        """callHierarchy/outgoingCalls — callees of a call hierarchy item."""
        result = await self._request("callHierarchy/outgoingCalls", {"item": item})
        return result or []

    async def prepare_type_hierarchy(
        self, file_uri: str, line: int, character: int
    ) -> list[dict]:
        """textDocument/prepareTypeHierarchy — prepare type hierarchy item."""
        result = await self._request(
            "textDocument/prepareTypeHierarchy",
            {
                "textDocument": {"uri": file_uri},
                "position": {"line": line, "character": character},
            },
        )
        return result or []

    async def supertypes(self, item: dict) -> list[dict]:
        """typeHierarchy/supertypes — parent types / traits implemented."""
        result = await self._request("typeHierarchy/supertypes", {"item": item})
        return result or []

    async def subtypes(self, item: dict) -> list[dict]:
        """typeHierarchy/subtypes — implementors / child types."""
        result = await self._request("typeHierarchy/subtypes", {"item": item})
        return result or []

    async def did_open(self, file_uri: str, language_id: str, text: str) -> None:
        """textDocument/didOpen — notify server a file was opened."""
        await self._notify(
            "textDocument/didOpen",
            {
                "textDocument": {
                    "uri": file_uri,
                    "languageId": language_id,
                    "version": 1,
                    "text": text,
                }
            },
        )

    async def did_change(self, file_uri: str, text: str) -> None:
        """textDocument/didChange — notify server of file content change."""
        await self._notify(
            "textDocument/didChange",
            {
                "textDocument": {"uri": file_uri, "version": 2},
                "contentChanges": [{"text": text}],
            },
        )

    async def did_close(self, file_uri: str) -> None:
        """textDocument/didClose."""
        await self._notify(
            "textDocument/didClose",
            {"textDocument": {"uri": file_uri}},
        )

    async def did_save(self, file_uri: str, text: str | None = None) -> None:
        """textDocument/didSave — triggers on-save analysis (cargo check)."""
        params: dict = {"textDocument": {"uri": file_uri}}
        if text is not None:
            params["text"] = text
        await self._notify("textDocument/didSave", params)

    # -----------------------------------------------------------------------
    # JSON-RPC internals
    # -----------------------------------------------------------------------

    async def _request(self, method: str, params: Any) -> Any:
        """Send a JSON-RPC request and await its response."""
        req_id = self._next_id
        self._next_id += 1

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[req_id] = future

        await self._send({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params})
        return await future

    async def _notify(self, method: str, params: Any) -> None:
        """Send a JSON-RPC notification (no response expected)."""
        await self._send({"jsonrpc": "2.0", "method": method, "params": params})

    async def _send(self, message: dict) -> None:
        assert self._process and self._process.stdin
        # JSON-RPC 2.0 §4: params MUST be omitted, not null
        if message.get("params") is None:
            message = {k: v for k, v in message.items() if k != "params"}
        body = json.dumps(message).encode("utf-8")
        header = _HEADER.format(len(body)).encode("ascii")
        async with self._write_lock:
            self._process.stdin.write(header + body)
            await self._process.stdin.drain()

    async def _read_loop(self) -> None:
        """Background task: read LSP messages from stdout and dispatch them."""
        assert self._process and self._process.stdout
        reader = self._process.stdout
        try:
            while True:
                # Read headers
                headers: dict[str, str] = {}
                while True:
                    line = await reader.readline()
                    if not line:
                        return
                    line = line.decode("ascii", errors="replace").strip()
                    if not line:
                        break  # blank line separates headers from body
                    if ":" in line:
                        k, _, v = line.partition(":")
                        headers[k.strip().lower()] = v.strip()

                length = int(headers.get("content-length", 0))
                if not length:
                    continue

                body = await reader.readexactly(length)
                message = json.loads(body.decode("utf-8"))
                self._dispatch(message)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("lsp.reader_error", error=str(e))
        finally:
            error = ConnectionError("LSP reader loop terminated")
            for future in self._pending.values():
                if not future.done():
                    try:
                        future.set_exception(error)
                    except Exception:
                        pass
            self._pending.clear()

    def _dispatch(self, message: dict) -> None:
        """Route an incoming LSP message to the appropriate waiting future."""
        msg_id = message.get("id")

        if msg_id is not None and msg_id in self._pending:
            future = self._pending.pop(msg_id)
            if "error" in message:
                err = message["error"]
                future.set_exception(LspError(err.get("code", -1), err.get("message", "")))
            else:
                future.set_result(message.get("result"))
        else:
            # Server-initiated notification (diagnostics, progress, etc.) — ignore
            method = message.get("method", "")
            if method and not method.startswith("$/"):
                log.debug("lsp.notification", method=method)


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _path_to_uri(path: str) -> str:
    return Path(path).as_uri()


# Minimal client capabilities — just enough for the methods we use
_CLIENT_CAPABILITIES = {
    "textDocument": {
        "documentSymbol": {"hierarchicalDocumentSymbolSupport": True},
        "references": {},
        "callHierarchy": {"dynamicRegistration": False},
        "typeHierarchy": {"dynamicRegistration": False},
    },
    "workspace": {
        "symbol": {"dynamicRegistration": False},
    },
}
