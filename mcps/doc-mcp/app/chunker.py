"""Split a markdown body into section-level chunks, one per heading.

We chunk by heading rather than by token count because pipeline docs have
meaningful structure (an ADR's Decision, a spec's Acceptance criteria). A
heading and the text beneath it, up to the next heading, is one chunk. Content
before the first heading becomes a "(preamble)" chunk.
"""
import re
from dataclasses import dataclass

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")


@dataclass
class Chunk:
    index: int
    title: str
    anchor: str
    text: str


def _slugify(title: str) -> str:
    slug = title.strip().lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    return slug.strip("-") or "section"


def chunk_markdown(body: str) -> list[Chunk]:
    lines = body.splitlines()
    sections: list[tuple[str, list[str]]] = []
    current_title = "(preamble)"
    current_lines: list[str] = []

    for line in lines:
        m = _HEADING_RE.match(line)
        if m:
            # Close the previous section if it had any content.
            if current_lines or current_title != "(preamble)":
                sections.append((current_title, current_lines))
            current_title = m.group(2).strip()
            current_lines = []
        else:
            current_lines.append(line)
    sections.append((current_title, current_lines))

    chunks: list[Chunk] = []
    idx = 0
    seen_anchors: dict[str, int] = {}
    for title, content_lines in sections:
        text = "\n".join(content_lines).strip()
        # Skip an empty preamble, but keep empty headed sections (the heading
        # itself carries meaning and may match a query).
        if not text and title == "(preamble)":
            continue
        base_anchor = _slugify(title)
        # Disambiguate repeated headings.
        n = seen_anchors.get(base_anchor, 0)
        seen_anchors[base_anchor] = n + 1
        anchor = base_anchor if n == 0 else f"{base_anchor}-{n}"
        chunks.append(Chunk(index=idx, title=title, anchor=anchor, text=text))
        idx += 1
    return chunks
