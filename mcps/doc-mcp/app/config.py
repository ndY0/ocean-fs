"""Configuration, read from environment variables with defaults."""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    qdrant_url: str = os.getenv("QDRANT_URL", "http://qdrant:6333")
    collection: str = os.getenv("COLLECTION", "pipeline_docs")
    embed_model: str = os.getenv("EMBED_MODEL", "BAAI/bge-small-en-v1.5")
    vector_size: int = int(os.getenv("VECTOR_SIZE", "384"))
    # Root of the project git repo, mounted into the container (read-only).
    repo_path: str = os.getenv("REPO_PATH", "/repo")
    host: str = os.getenv("MCP_HOST", "0.0.0.0")
    port: int = int(os.getenv("MCP_PORT", "8000"))


config = Config()
