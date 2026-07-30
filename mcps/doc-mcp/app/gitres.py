"""Git plumbing: resolve document content from the working tree or from history.

The retrieval index stores each chunk's blob SHA. That makes a document's
content permanently resolvable even after the file is deleted from the working
tree, because git keeps the blob reachable through the commits that contained it.
"""
import os
import subprocess

from .config import config


class GitError(RuntimeError):
    pass


def _run(args: list[str]) -> str:
    result = subprocess.run(
        ["git", "-C", config.repo_path, *args],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise GitError(result.stderr.strip() or "git command failed")
    return result.stdout


def path_exists_on_disk(rel_path: str) -> bool:
    return os.path.isfile(os.path.join(config.repo_path, rel_path))


def read_from_disk(rel_path: str) -> str:
    with open(os.path.join(config.repo_path, rel_path), "r", encoding="utf-8") as fh:
        return fh.read()


def blob_sha_at_head(rel_path: str) -> str | None:
    """The blob SHA of a path at HEAD, or None if it isn't tracked at HEAD."""
    try:
        return _run(["rev-parse", f"HEAD:{rel_path}"]).strip()
    except GitError:
        return None


def cat_blob(blob_sha: str) -> str:
    """Print the content of a blob by SHA — works for deleted files too."""
    return _run(["cat-file", "-p", blob_sha])
