"""Parse the YAML front-matter that every pipeline document carries.

Expected front-matter (see PIPELINE.md, supersession protocol):

    ---
    type: spec | adr | architecture | charter | brainstorm | review | eval
    domain: <domain name, or "global">
    feature: <feature slug, or "none">
    status: active | superseded | deprecated
    date: <ISO 8601>
    superseded_by: <path, or "none">
    superseded_date: <ISO 8601, or "none">
    ---
"""
from dataclasses import dataclass

import frontmatter


@dataclass
class ParsedDoc:
    metadata: dict
    body: str


_DEFAULTS = {
    "type": "unknown",
    "domain": "global",
    "feature": "none",
    "status": "active",
    "date": "unknown",
    "superseded_by": "none",
    "superseded_date": "none",
}


def parse(text: str) -> ParsedDoc:
    """Parse a markdown string into metadata + body, applying defaults."""
    post = frontmatter.loads(text)
    meta = dict(_DEFAULTS)
    for key in _DEFAULTS:
        if key in post.metadata and post.metadata[key] is not None:
            meta[key] = str(post.metadata[key])
    return ParsedDoc(metadata=meta, body=post.content)
