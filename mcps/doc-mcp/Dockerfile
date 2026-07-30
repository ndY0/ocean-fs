FROM python:3.12-slim

# git is required at runtime: the server resolves deleted-doc content from
# history via `git cat-file` and computes blob SHAs via `git rev-parse`.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache the embedding model inside the image so the container is self-contained
# and the first request isn't slowed by a model download.
ENV FASTEMBED_CACHE_PATH=/opt/fastembed_cache
ENV HF_HOME=/opt/hf_cache

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the embedding model into the image layer.
RUN python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='BAAI/bge-small-en-v1.5')"

COPY app ./app

EXPOSE 8000

CMD ["python", "-m", "app.server"]
