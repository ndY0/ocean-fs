FROM python:3.12-slim

# ── system packages ──────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    # Java support (jdtls requires JRE 17+)
    openjdk-21-jdk-headless \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Python deps ──────────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Language server installation ─────────────────────────────────────────────
# Each block is ARG-gated so the image only installs what LANGUAGE requires.
# Build with: docker build --build-arg LANGUAGE=rust .
ARG LANGUAGE=rust

# --- Rust: rust-analyzer -------------------------------------------------------
# Install Rust toolchain (rust-analyzer needs cargo/rustc for full analysis).
# Then download rust-analyzer binary.
RUN if [ "$LANGUAGE" = "rust" ]; then \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    . $HOME/.cargo/env && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then RA_ARCH="x86_64-unknown-linux-gnu"; \
    elif [ "$ARCH" = "aarch64" ]; then RA_ARCH="aarch64-unknown-linux-gnu"; \
    else echo "Unsupported arch: $ARCH" && exit 1; fi && \
    RA_URL="https://github.com/rust-lang/rust-analyzer/releases/latest/download/rust-analyzer-${RA_ARCH}.gz" && \
    curl -L "$RA_URL" | gunzip -c > /usr/local/bin/rust-analyzer && \
    chmod +x /usr/local/bin/rust-analyzer && \
    echo "rust-analyzer installed: $(rust-analyzer --version)"; \
    fi

# Ensure cargo/rustc are on PATH for the application
ENV PATH="/root/.cargo/bin:${PATH}"

# --- Java: Eclipse JDT Language Server ----------------------------------------
# Pin to a known-good milestone release.
ENV JDTLS_VERSION=1.38.0_2024-10-03
ENV JDTLS_HOME=/opt/jdtls

RUN if [ "$LANGUAGE" = "java" ]; then \
    mkdir -p ${JDTLS_HOME} && \
    JDTLS_URL="https://download.eclipse.org/jdtls/milestones/${JDTLS_VERSION%_*}/jdt-language-server-${JDTLS_VERSION}.tar.gz" && \
    curl -L "$JDTLS_URL" | tar xz -C ${JDTLS_HOME} && \
    echo "jdtls installed at ${JDTLS_HOME}"; \
    fi

# --- Python: python-lsp-server ------------------------------------------------
RUN if [ "$LANGUAGE" = "python" ]; then \
    pip install --no-cache-dir python-lsp-server[all]; \
    fi

# ── Application ──────────────────────────────────────────────────────────────
COPY app/ ./app/

ENV CODE_LANGUAGE=${LANGUAGE}

CMD ["python", "-m", "app.main"]
