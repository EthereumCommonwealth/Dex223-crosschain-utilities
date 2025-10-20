import os
from pydantic import BaseModel, Field


class AppConfig(BaseModel):
    chain: str = Field(default_factory=lambda: os.getenv("CHAIN", "eth").lower())
    rpc_url: str = Field(default_factory=lambda: os.getenv("RPC_URL", "https://rpc.mevblocker.io"))
    rpc_pool: list[str] = Field(default_factory=lambda: [x for x in os.getenv("RPC_POOL", "").split(",") if x.strip()])

    tokens_file: str = Field(default_factory=lambda: os.getenv("TOKENS_FILE", "eth_tokens_list.txt"))
    contracts_file: str = Field(default_factory=lambda: os.getenv("CONTRACTS_FILE", "eth_contracts_list.txt"))
    results_path: str = Field(default_factory=lambda: os.getenv("RESULTS", "").strip())

    etherscan_enabled: bool = Field(default_factory=lambda: os.getenv("ETHERSCAN", "false").lower() != "false")
    etherscan_api_key: str = Field(default_factory=lambda: os.getenv("ETHERSCAN_KEY", ""))

    exclude_by_list: bool = Field(default_factory=lambda: os.getenv("EXCLUDE_BY_LIST", "true").lower() != "false")
    exclude_by_mint: bool = Field(default_factory=lambda: os.getenv("EXCLUDE_BY_MINT", "true").lower() != "false")
    exclude_by_extract: bool = Field(default_factory=lambda: os.getenv("EXCLUDE_BY_EXTRACT", "true").lower() != "false")
    collect_extract: bool = Field(default_factory=lambda: os.getenv("COLLECT_EXTRACT", "false").lower() != "false")

    token_concurrency: int = Field(default_factory=lambda: int(os.getenv("CONCURRENCY", "4")))
    holder_concurrency: int = Field(default_factory=lambda: int(os.getenv("HOLDER_CONCURRENCY", "32")))
    rps: int = Field(default_factory=lambda: int(os.getenv("RPS", "80")))
    max_retries: int = Field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "1")))
    retry_base_delay: float = Field(default_factory=lambda: float(os.getenv("RETRY_BASE_DELAY", "0.25")))
    call_timeout: float = Field(default_factory=lambda: float(os.getenv("CALL_TIMEOUT", "15.0")))


def load_env() -> AppConfig:
    from dotenv import load_dotenv
    load_dotenv()
    return AppConfig()
