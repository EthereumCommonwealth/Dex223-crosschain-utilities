import asyncio
import json
import logging
from pathlib import Path
from decimal import Decimal
from datetime import datetime
from config import AppConfig, load_env
from services.scanner import LostTokensScannerService
from adapters.web3_gateway import Web3TokenGateway
from adapters.etherscan_client import EtherscanClient
from adapters.node_pool import Web3NodePool

from repositories.price_etherscan import EtherscanPriceRepository, NullPriceRepository
from repositories.excludes_file import FileExcludesRepository
from repositories.cache_fs import FileCacheRepository
from utils.rate_limiter import RateLimiter
from utils.iohelpers import uniq, read_text
from utils.parsing import parse_addresses_from_text

logging.basicConfig(level="INFO", format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("main")


def to_json(o):
    if isinstance(o, Decimal): return str(o)
    if hasattr(o, "__dict__"): return o.__dict__
    return o


async def run(cfg: AppConfig) -> None:
    start = datetime.now()
    base = Path(__file__).parent / "tmp"
    in_dir = base / "in"
    out_dir = base / "out"
    out_dir.mkdir(exist_ok=True)
    tokens_src = read_text(in_dir / cfg.tokens_file)
    contracts_src = read_text(in_dir / cfg.contracts_file)
    tokens = uniq(parse_addresses_from_text(tokens_src))
    contracts = uniq(parse_addresses_from_text(contracts_src))

    log.info("Tokens: %d | Contracts: %d", len(tokens), len(contracts))

    limiter = RateLimiter(cfg.rps)
    cache = FileCacheRepository(base)
    urls = [cfg.rpc_url] + list(cfg.rpc_pool or [])
    node_pool = Web3NodePool(urls, request_timeout=30.0)

    web3 = Web3TokenGateway(
        node_pool=node_pool,
        limiter=limiter,
        retries=cfg.max_retries,
        base_delay=cfg.retry_base_delay,
        call_timeout=cfg.call_timeout,
    )

    etherscan = EtherscanClient(cfg.etherscan_api_key)
    price_repo = (EtherscanPriceRepository(etherscan) if cfg.etherscan_enabled else NullPriceRepository())
    excludes_repo = FileExcludesRepository(in_dir / "excludes.json")

    service = LostTokensScannerService(
        token_gateway=web3,
        price_repo=price_repo,
        excludes_repo=excludes_repo,
        cache_repo=cache,
        etherscan=etherscan,
        collect_extract=cfg.collect_extract,
        concurrency=cfg.token_concurrency,
        token_timeout_sec=None
    )

    if cfg.results_path:
        results = json.loads(Path(cfg.results_path).read_text(encoding="utf-8"))
    else:
        results = await service.scan(tokens)

    await node_pool.aclose()

    report, total_usd, n = service.format_report(
        results,
        exclude_by_list=cfg.exclude_by_list,
        exclude_by_mint=cfg.exclude_by_mint,
        exclude_by_extract=cfg.exclude_by_extract,
    )
    name = f"lost_tokens_result_{start.strftime('%d_%m_%Y')}"
    (out_dir / f"{name}.txt").write_text(report, encoding="utf-8")
    (out_dir / f"{name}.json").write_text(json.dumps(
        results,
        default=to_json, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    log.info("Saved: %s", out_dir / f"{name}.txt")
    log.info("Saved: %s", out_dir / f"{name}.json")


if __name__ == "__main__":
    config = load_env()
    asyncio.run(run(config))
