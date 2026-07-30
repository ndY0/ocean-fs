"""Qdrant-backed store. One point per document section (chunk).

The hybrid retrieval pattern lives here: a payload filter (status, type, domain)
constrains the candidate set first, then vector similarity ranks within it.
This is "filter on metadata first, rank by meaning second."
"""
import uuid

from qdrant_client import QdrantClient, models

from .config import config

# Stable namespace so the same (path, anchor) always maps to the same point id,
# which makes re-indexing an upsert (overwrite) rather than a duplicate insert.
_NAMESPACE = uuid.UUID("8f1a0b6e-2c3d-4e5f-9a7b-1c2d3e4f5a6b")

# Status tiers
_ACTIVE = "active"
_DEPRECATED_TIER = ["active", "superseded", "deprecated", "deleted"]


def _point_id(path: str, anchor: str, index: int) -> str:
    return str(uuid.uuid5(_NAMESPACE, f"{path}::{anchor}::{index}"))


class Store:
    def __init__(self) -> None:
        self.client = QdrantClient(url=config.qdrant_url)

    def ensure_collection(self) -> None:
        existing = {c.name for c in self.client.get_collections().collections}
        if config.collection not in existing:
            self.client.create_collection(
                collection_name=config.collection,
                vectors_config=models.VectorParams(
                    size=config.vector_size,
                    distance=models.Distance.COSINE,
                ),
            )
        # Payload indexes make filtering fast. Keyword indexes for exact-match
        # fields; a full-text index on the chunk text for keyword matching.
        for field in ("path", "type", "domain", "feature", "doc_status"):
            self._safe_payload_index(field, models.PayloadSchemaType.KEYWORD)
        self._safe_payload_index("chunk_text", models.PayloadSchemaType.TEXT)

    def _safe_payload_index(self, field: str, schema) -> None:
        try:
            self.client.create_payload_index(
                collection_name=config.collection,
                field_name=field,
                field_schema=schema,
            )
        except Exception:
            # Index already exists — Qdrant raises rather than no-ops.
            pass

    # ------------------------------------------------------------------ writes

    def delete_by_path(self, path: str) -> None:
        """Remove all chunks for a path, so a re-index leaves no stale sections."""
        self.client.delete(
            collection_name=config.collection,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[models.FieldCondition(
                        key="path", match=models.MatchValue(value=path))]
                )
            ),
        )

    def upsert_chunks(
        self,
        path: str,
        blob_sha: str,
        metadata: dict,
        chunks: list,
        vectors: list[list[float]],
    ) -> int:
        points = []
        for chunk, vector in zip(chunks, vectors):
            payload = {
                "path": path,
                "blob_sha": blob_sha,
                "doc_status": metadata.get("status", "active"),
                "type": metadata.get("type", "unknown"),
                "domain": metadata.get("domain", "global"),
                "feature": metadata.get("feature", "none"),
                "date": metadata.get("date", "unknown"),
                "superseded_by": metadata.get("superseded_by", "none"),
                "superseded_date": metadata.get("superseded_date", "none"),
                "section_title": chunk.title,
                "section_anchor": chunk.anchor,
                "chunk_text": chunk.text,
            }
            points.append(models.PointStruct(
                id=_point_id(path, chunk.anchor, chunk.index),
                vector=vector,
                payload=payload,
            ))
        self.client.upsert(collection_name=config.collection, points=points)
        return len(points)

    def mark_deleted(self, path: str, blob_sha: str) -> int:
        """Flip every chunk of a path to status=deleted, retaining its blob SHA.

        Keeping the points (rather than removing them) is what lets a deleted
        document still be discovered and resolved from history.
        """
        flt = models.Filter(must=[models.FieldCondition(
            key="path", match=models.MatchValue(value=path))])
        # Count affected points for the return value.
        count = self.client.count(
            collection_name=config.collection, count_filter=flt, exact=True).count
        self.client.set_payload(
            collection_name=config.collection,
            payload={"doc_status": "deleted", "blob_sha": blob_sha},
            points=flt,
        )
        return count

    # ------------------------------------------------------------------- reads

    def search(
        self,
        query_vector: list[float],
        doc_type: str | None,
        domain: str | None,
        include_deprecated: bool,
        limit: int,
    ) -> list[dict]:
        must = []
        if include_deprecated:
            must.append(models.FieldCondition(
                key="doc_status", match=models.MatchAny(any=_DEPRECATED_TIER)))
        else:
            must.append(models.FieldCondition(
                key="doc_status", match=models.MatchValue(value=_ACTIVE)))
        if doc_type:
            must.append(models.FieldCondition(
                key="type", match=models.MatchValue(value=doc_type)))
        if domain:
            must.append(models.FieldCondition(
                key="domain", match=models.MatchValue(value=domain)))

        result = self.client.query_points(
            collection_name=config.collection,
            query=query_vector,
            query_filter=models.Filter(must=must),
            limit=limit,
            with_payload=True,
        )
        hits = []
        for point in result.points:
            p = point.payload
            text = p.get("chunk_text", "")
            hits.append({
                "path": p.get("path"),
                "section_title": p.get("section_title"),
                "section_anchor": p.get("section_anchor"),
                "doc_status": p.get("doc_status"),
                "type": p.get("type"),
                "domain": p.get("domain"),
                "blob_sha": p.get("blob_sha"),
                "score": round(point.score, 4),
                "snippet": text[:300] + ("…" if len(text) > 300 else ""),
            })
        return hits

    def latest_blob_for_path(self, path: str) -> str | None:
        """Most recently stored blob SHA for a path (used to resolve deletions)."""
        flt = models.Filter(must=[models.FieldCondition(
            key="path", match=models.MatchValue(value=path))])
        points, _ = self.client.scroll(
            collection_name=config.collection,
            scroll_filter=flt,
            limit=1,
            with_payload=True,
        )
        if points:
            return points[0].payload.get("blob_sha")
        return None

    def list_active(self, doc_type: str | None, domain: str | None) -> list[dict]:
        must = [models.FieldCondition(
            key="doc_status", match=models.MatchValue(value=_ACTIVE))]
        if doc_type:
            must.append(models.FieldCondition(
                key="type", match=models.MatchValue(value=doc_type)))
        if domain:
            must.append(models.FieldCondition(
                key="domain", match=models.MatchValue(value=domain)))

        flt = models.Filter(must=must)
        by_path: dict[str, dict] = {}
        offset = None
        while True:
            points, offset = self.client.scroll(
                collection_name=config.collection,
                scroll_filter=flt,
                limit=256,
                offset=offset,
                with_payload=True,
            )
            for pt in points:
                p = pt.payload
                path = p.get("path")
                if path not in by_path:
                    by_path[path] = {
                        "path": path,
                        "type": p.get("type"),
                        "domain": p.get("domain"),
                        "feature": p.get("feature"),
                        "date": p.get("date"),
                    }
            if offset is None:
                break
        return sorted(by_path.values(), key=lambda d: (d["type"], d["domain"], d["path"]))
