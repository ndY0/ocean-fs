"""
File watcher: watches the workspace for source file changes and triggers
incremental re-indexing. Language-agnostic — uses plugin.file_patterns
to determine which files to watch.

Change batching: file changes within a short window are collected into a batch.
If the batch exceeds a threshold, a full workspace re-index is triggered
(which parallelises symbol/edge extraction). Otherwise, files are indexed
individually. This prevents agents making large multi-file edits from
triggering a sequential queue of single-file indexes.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from watchfiles import awatch, Change

from .config import settings
from .dgraph import DGraphClient
from .indexer import index_file, index_workspace
from .lsp_client import LspClient

if TYPE_CHECKING:
    from .plugin_base import LanguagePlugin

log = structlog.get_logger()

# Seconds to wait after the last file change before processing the batch.
_BATCH_WINDOW_SECONDS = 2.0

# If this many files change within the window, trigger a full workspace
# re-index instead of individual file indexes.
_FULL_INDEX_THRESHOLD = 3


async def watch_workspace(
    client: DGraphClient,
    lsp: LspClient,
    plugin: "LanguagePlugin",
) -> None:
    """
    Watch workspace for source file changes.
    Runs forever as a background task.
    """
    workspace = settings.workspace
    extensions = _extensions_from_patterns(plugin.file_patterns)
    exclude_dirs = set(plugin.exclude_dirs)

    log.info(
        "watcher.started",
        workspace=workspace,
        language=plugin.name,
        extensions=list(extensions),
    )

    pending_files: set[str] = set()
    batch_timer: asyncio.TimerHandle | None = None
    loop = asyncio.get_event_loop()

    def _flush_batch() -> None:
        nonlocal batch_timer
        batch_timer = None
        files = pending_files.copy()
        pending_files.clear()
        if files:
            _schedule_batch(client, lsp, plugin, list(files))

    async for changes in awatch(workspace, recursive=True):
        for change_type, path in changes:
            # Filter by extension
            if not any(path.endswith(ext) for ext in extensions):
                continue

            # Filter excluded directories
            path_parts = Path(path).parts
            if any(part in exclude_dirs for part in path_parts):
                continue

            if change_type == Change.deleted:
                log.info("watcher.deleted", file=path)
                await client.delete_file_symbols(path)
                pending_files.discard(path)
                continue

            # Add to batch and restart the timer
            pending_files.add(path)

            if batch_timer is not None:
                batch_timer.cancel()
            batch_timer = loop.call_later(_BATCH_WINDOW_SECONDS, _flush_batch)

    # In case the event loop stops cleanly, flush any remaining files
    if pending_files:
        _schedule_batch(client, lsp, plugin, list(pending_files))
        pending_files.clear()


def _schedule_batch(
    client: DGraphClient,
    lsp: LspClient,
    plugin: "LanguagePlugin",
    files: list[str],
) -> None:
    """Schedule a batch of files for (re-)indexing."""
    if len(files) >= _FULL_INDEX_THRESHOLD:
        log.info("watcher.batch_full_index", count=len(files), files=files)
        asyncio.ensure_future(_safe_index_workspace(client, lsp, plugin))
    else:
        log.info("watcher.batch_incremental", count=len(files), files=files)
        for path in files:
            asyncio.ensure_future(_safe_index_file(client, lsp, plugin, path))


async def _safe_index_file(
    client: DGraphClient,
    lsp: LspClient,
    plugin: "LanguagePlugin",
    file_path: str,
) -> None:
    try:
        await index_file(client, lsp, plugin, file_path)
    except Exception as e:
        log.error("watcher.reindex_failed", file=file_path, error=str(e))


async def _safe_index_workspace(
    client: DGraphClient,
    lsp: LspClient,
    plugin: "LanguagePlugin",
) -> None:
    try:
        await index_workspace(client, lsp, plugin)
    except Exception as e:
        log.error("watcher.full_index_failed", error=str(e))


def _extensions_from_patterns(patterns: list[str]) -> set[str]:
    """
    Extract file extensions from glob patterns.
    ["*.rs", "*.toml"] → {".rs", ".toml"}
    """
    exts = set()
    for pattern in patterns:
        if "." in pattern:
            ext = "." + pattern.rsplit(".", 1)[-1]
            exts.add(ext)
    return exts
