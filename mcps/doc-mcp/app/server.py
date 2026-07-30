"""Document-retrieval MCP server for the agent pipeline.

Exposes five tools over streamable-HTTP:
  - search          : hybrid metadata-filter + semantic search over docs
  - get_content     : resolve a doc's content (working tree, or git history)
  - index_document  : (re)index a doc — called by the Archivist after a cycle
  - mark_deleted    : tombstone a doc in the index, retaining its blob SHA
  - list_active     : manifest-style listing of active docs

The search/get_content tools are what specialist agents use to pull the
relevant subset of project knowledge. The index_document/mark_deleted tools are
maintenance calls the Archivist makes; ordinary agents do not call them.
"""
from mcp.server.fastmcp import FastMCP

import time

from .config import config
from . import frontmatter_parser, gitres
from .chunker import chunk_markdown
from .embedder import Embedder
from .store import Store

mcp = FastMCP(
    "doc-retrieval",
    host=config.host,
    port=config.port,
    stateless_http=True,
    json_response=True,
)

store = Store()
embedder = Embedder()


@mcp.tool()
def search(
    query: str,
    type: str | None = None,
    domain: str | None = None,
    include_deprecated: bool = False,
    limit: int = 8,
) -> list[dict]:
    """Find the document sections most relevant to a query.

    Results are filtered by metadata first (active status by default, plus the
    optional type/domain constraints) and then ranked by semantic similarity.
    Returns section-level hits — read the full document with get_content when a
    hit looks relevant rather than relying on the snippet alone.

    Args:
        query: Natural-language description of what you are looking for.
        type: Restrict to one doc type (spec, adr, architecture, charter,
            brainstorm, review, eval). Omit to search all types.
        domain: Restrict to one bounded context. Omit to search all domains.
        include_deprecated: If true, also search superseded/deleted docs. Default
            false — by default you only see current, active truth.
        limit: Maximum number of section hits to return.

    Returns:
        A list of hits, each with: path, section_title, section_anchor,
        doc_status, type, domain, blob_sha, score, and a snippet.
    """
    vector = embedder.embed_one(query)
    return store.search(vector, type, domain, include_deprecated, limit)


@mcp.tool()
def get_content(path: str, blob_sha: str | None = None) -> str:
    """Return the full text of a document.

    Resolution order:
      1. If blob_sha is given, return exactly that historical version.
      2. Else if the file exists on the working tree, return the current file.
      3. Else (the file was deleted) resolve its last indexed blob from history.

    This means a document remains readable even after it has been archived and
    removed from the working tree.

    Args:
        path: Repo-relative path, e.g. "docs/specs/old-feature.md".
        blob_sha: Optional explicit git blob SHA to fetch a specific version.

    Returns:
        The document's full markdown content.
    """
    if blob_sha:
        return gitres.cat_blob(blob_sha)
    if gitres.path_exists_on_disk(path):
        return gitres.read_from_disk(path)
    # Deleted from the working tree — resolve from history via the stored blob.
    sha = gitres.blob_sha_at_head(path) or store.latest_blob_for_path(path)
    if not sha:
        raise ValueError(
            f"No content found for '{path}' on disk, at HEAD, or in the index."
        )
    return gitres.cat_blob(sha)


@mcp.tool()
def index_document(path: str) -> dict:
    """(Re)index a single document. Maintenance call — used by the Archivist.

    Reads the file from the working tree, parses its front-matter, splits it into
    section-level chunks, embeds each chunk, and upserts them. Any previously
    indexed chunks for this path are removed first, so renamed or removed
    sections do not linger. The document's current blob SHA (from HEAD) is stored
    on every chunk so the version is resolvable from history later.

    Args:
        path: Repo-relative path to the document to index.

    Returns:
        A summary: path, indexed chunk count, resolved blob_sha, and doc status.
    """
    if not gitres.path_exists_on_disk(path):
        raise ValueError(f"'{path}' does not exist on the working tree.")
    raw = gitres.read_from_disk(path)
    parsed = frontmatter_parser.parse(raw)
    chunks = chunk_markdown(parsed.body)
    blob_sha = gitres.blob_sha_at_head(path) or "uncommitted"

    store.delete_by_path(path)
    if not chunks:
        return {"path": path, "indexed_chunks": 0, "blob_sha": blob_sha,
                "status": parsed.metadata.get("status")}

    vectors = embedder.embed([f"{c.title}\n{c.text}" for c in chunks])
    count = store.upsert_chunks(path, blob_sha, parsed.metadata, chunks, vectors)
    return {"path": path, "indexed_chunks": count, "blob_sha": blob_sha,
            "status": parsed.metadata.get("status")}


@mcp.tool()
def mark_deleted(path: str) -> dict:
    """Tombstone a document in the index. Maintenance call — used by the Archivist.

    Call this BEFORE `git rm`-ing the file. It captures the document's current
    blob SHA (from HEAD) and flips every chunk to status=deleted while retaining
    that SHA, so the content stays resolvable from git history afterward.

    Args:
        path: Repo-relative path being archived.

    Returns:
        A summary: path, the retained blob_sha, and the number of chunks updated.
    """
    sha = gitres.blob_sha_at_head(path) or store.latest_blob_for_path(path)
    if not sha:
        raise ValueError(
            f"Cannot resolve a blob SHA for '{path}'. Index it before deleting."
        )
    updated = store.mark_deleted(path, sha)
    return {"path": path, "blob_sha": sha, "chunks_updated": updated}


@mcp.tool()
def list_active(type: str | None = None, domain: str | None = None) -> list[dict]:
    """List active documents (manifest view), optionally filtered.

    This is the cheap "what is current truth" query. It returns one entry per
    active document (deduplicated across its sections), never superseded or
    deleted ones.

    Args:
        type: Restrict to one doc type. Omit for all.
        domain: Restrict to one domain. Omit for all.

    Returns:
        A list of documents, each with: path, type, domain, feature, date.
    """
    return store.list_active(type, domain)


def _wait_for_qdrant(retries: int = 30, delay: float = 2.0) -> None:
    """Poll Qdrant until it accepts connections.

    `depends_on` only guarantees the Qdrant container has started, not that it is
    ready to serve. We poll a cheap endpoint until it responds, so startup
    ordering doesn't depend on any tooling inside the (distroless) Qdrant image.
    """
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            store.client.get_collections()
            print(f"Qdrant is ready (after {attempt} attempt(s)).", flush=True)
            return
        except Exception as exc:  # connection refused, etc.
            last_error = exc
            print(f"Waiting for Qdrant ({attempt}/{retries})…", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"Qdrant not reachable after {retries} attempts: {last_error}")


def main() -> None:
    _wait_for_qdrant()
    store.ensure_collection()
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()