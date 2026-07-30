from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    dgraph_url: str = "http://localhost:8080"
    mcp_host: str = "0.0.0.0"
    mcp_port: int = 8765
    log_level: str = "INFO"
    workspace: str = "/workspace"

    # Language to activate. Must match a registered plugin name.
    # Supported: rust, java, python
    code_language: str = "rust"

    # LSP server startup timeout in seconds.
    lsp_init_timeout: int = 60

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
