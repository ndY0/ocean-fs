"""Local embedding via fastembed (ONNX, CPU). No external API, no token cost."""
from fastembed import TextEmbedding

from .config import config


class Embedder:
    def __init__(self) -> None:
        # Model is pre-downloaded in the Docker image, so this is fast.
        self._model = TextEmbedding(model_name=config.embed_model)

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts into vectors."""
        return [vec.tolist() for vec in self._model.embed(texts)]

    def embed_one(self, text: str) -> list[float]:
        return next(iter(self._model.embed([text]))).tolist()
