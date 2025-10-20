# -*- coding: utf-8 -*-
"""
ERC-20/223 Snapshot — with rate-limit.

Dependencies:
    pip install web3 pandas tqdm
"""
from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Protocol, Iterable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import threading
import time
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from web3 import Web3
from web3.types import FilterParams, ChecksumAddress
from web3.exceptions import BlockNotFound
from core.log import setup_logging

setup_logging(debug=False)
logger = logging.getLogger("erc20-snapshot")


# =========================
# ======== Config =========
# =========================

class ReadContractError(Exception):
    pass


@dataclass(frozen=False)
class SnapshotConfig:
    rpc_url: str
    token_address: str
    snapshot_block: int | None = None
    start_block: int | None = None  # if None - start with 0
    logs_chunk: int = 5_000
    logs_max_workers: int = 8  # streams for getLogs
    logs_rps_limit: int = 15  # RPS limitation specifically for getLogs
    logs_max_retries: int = 4  # getLogs retries for errors

    symbol_fallback: str = "TOKEN"
    decimals_fallback: int = 18
    # If we know the deployment block in advance, we can set it, otherwise it will be found by binary search
    known_deploy_block: Optional[int] = None
    # Parallel and rate limit:
    max_workers: int = 20  # number of threads for balanceOf
    rps_limit: int = 90  # no more than X requests per second
    max_retries: int = 5  # retrace on balanceOf
    backoff_base: float = 0.5  # start delay (sec) for exponential backoff
    http_timeout: int = 60  # HTTPProvider timeout, sec
    debug_logs: bool = False  # print debug parameters getLogs


# ==================================
# ===== Abstractions / Protocols ===
# ==================================

class EthProvider(Protocol):
    def w3(self) -> Web3: ...

    def ensure_block_exists(self, block: int) -> None: ...


class DeployBlockResolver(Protocol):
    def resolve_deploy_block(self, token_addr: ChecksumAddress, hi_block: int) -> int: ...


class TokenGateway(Protocol):
    def symbol(self, block: int) -> str: ...

    def decimals(self, block: int) -> int: ...

    def total_supply(self, block: int) -> Optional[int]: ...

    def balance_of(self, owner: ChecksumAddress, block: int) -> int: ...


class LogScanner(Protocol):
    def collect_transfer_participants(self, start_block: int, end_block: int) -> set[ChecksumAddress]: ...


class BalanceReader(Protocol):
    def read_balances(self, holders: Iterable[ChecksumAddress], block: int) -> dict[str, int]: ...


class SnapshotWriter(Protocol):
    def write(self, rows: list[dict], out_name: str) -> str: ...


# ===============================
# ========= Implementations =====
# ===============================

# --- Provider ---

class Web3Provider(EthProvider):
    def __init__(self, rpc_url: str, http_timeout: int = 60):
        self._w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": http_timeout}))

    def w3(self) -> Web3:
        return self._w3

    def ensure_block_exists(self, block: int) -> None:
        try:
            self._w3.eth.get_block(block)
        except BlockNotFound as e:
            raise SystemExit(f"Block {block} not found on this chain / provider") from e


# --- Deploy block resolution strategies ---

class AutoBinarySearchDeployResolver(DeployBlockResolver):
    """Finds the first block where the code appeared at the address (deployment)."""

    def __init__(self, provider: EthProvider):
        self._w3 = provider.w3()

    def resolve_deploy_block(self, token_addr: ChecksumAddress, hi_block: int) -> int:
        lo, hi = 0, hi_block
        first = hi_block
        while lo <= hi:
            mid = (lo + hi) // 2
            code = self._w3.eth.get_code(token_addr, block_identifier=mid)
            if code and len(code) > 0:
                first = mid
                hi = mid - 1
            else:
                lo = mid + 1
        return first


class FixedDeployResolver(DeployBlockResolver):
    """If the deployment block is known in advance, we use it, but validate it."""

    def __init__(self, provider: EthProvider, deploy_block: int):
        self._w3 = provider.w3()
        self._deploy_block = deploy_block

    def resolve_deploy_block(self, token_addr: ChecksumAddress, hi_block: int) -> int:
        code = self._w3.eth.get_code(token_addr, block_identifier=self._deploy_block)
        if not code or len(code) == 0:
            # auto-safety with binary search if the specified block is invalid
            lo = self._deploy_block
            hi = hi_block
            first = hi_block
            while lo <= hi:
                mid = (lo + hi) // 2
                c = self._w3.eth.get_code(token_addr, block_identifier=mid)
                if c and len(c) > 0:
                    first = mid
                    hi = mid - 1
                else:
                    lo = mid + 1
            return first
        return self._deploy_block


# --- Token gateway ---

ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "decimals",
     "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "owner", "type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
    {"anonymous": False, "inputs": [
        {"indexed": True, "name": "from", "type": "address"},
        {"indexed": True, "name": "to", "type": "address"},
        {"indexed": False, "name": "value", "type": "uint256"},
    ], "name": "Transfer", "type": "event"},
    {"constant": True, "inputs": [], "name": "totalSupply",
     "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol",
     "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "name",
     "outputs": [{"name": "", "type": "string"}], "type": "function"},
]
TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").to_0x_hex()
ZERO = "0x0000000000000000000000000000000000000000"
KNOWN_TOO_MANY_LOGS = (
    "query returned more than",  # Infura/Alchemy-style
    "Response size exceeded",  # common generic
    "Log response size exceeded",
    "block range is too wide",
)
ERROR_CODE_TOO_MANY_LOGS = -32005


class Web3TokenGateway(TokenGateway):
    def __init__(
            self,
            provider: EthProvider,
            token_address: str,
            symbol_fallback: str = "TOKEN", decimals_fallback: int = 18):
        self._w3 = provider.w3()
        self._address = Web3.to_checksum_address(token_address)
        self._c = self._w3.eth.contract(address=self._address, abi=ERC20_ABI)
        self._symbol_fallback = symbol_fallback
        self._decimals_fallback = decimals_fallback

    def symbol(self, block: int) -> str:
        try:
            return self._c.functions.symbol().call(block_identifier=block)
        except ReadContractError:
            return self._symbol_fallback

    def decimals(self, block: int) -> int:
        try:
            return int(self._c.functions.decimals().call(block_identifier=block))
        except ReadContractError:
            return self._decimals_fallback

    def total_supply(self, block: int) -> Optional[int]:
        return int(self._c.functions.totalSupply().call(block_identifier=block))

    def balance_of(self, owner: ChecksumAddress, block: int) -> int:
        return int(self._c.functions.balanceOf(Web3.to_checksum_address(owner)).call(
            block_identifier=block
        ))

    @property
    def address(self) -> ChecksumAddress:
        return self._address


# --- Log scanner ---

class TopicTransferScanner(LogScanner):
    """Scanner for standard Transfer(address,address,uint256)."""

    def __init__(
            self,
            provider: EthProvider,
            token: Web3TokenGateway,
            logs_chunk: int,
            deploy_resolver: DeployBlockResolver,
            debug: bool = False,
            max_workers: int = 8,
            rps_limit: int = 15,
            max_retries: int = 4,
    ):
        self._w3 = provider.w3()
        self._token = token
        self._chunk = logs_chunk
        self._resolver = deploy_resolver
        self._debug = debug
        self._workers = max_workers
        self._limiter = RateLimiter(rps_limit)
        self._max_retries = max_retries

    def _fetch_logs_range(self, frm: int, to: int) -> list:
        """Pulls logs to the range [frm, to] with retries and range reduction on overflow."""
        attempt = 0
        cur_from, cur_to = frm, to
        while True:
            self._limiter.acquire()
            try:
                params: FilterParams = {
                    "fromBlock": cur_from,
                    "toBlock": cur_to,
                    "address": self._token.address,
                    "topics": [TRANSFER_TOPIC],
                }
                if self._debug:
                    logger.debug(params)
                return self._w3.eth.get_logs(params)
            except Exception as e:
                msg = str(e)
                # If there are too many logs, we reduce the range by half and try again.
                if getattr(e, "code", None) == ERROR_CODE_TOO_MANY_LOGS or any(t in msg for t in KNOWN_TOO_MANY_LOGS):
                    if cur_from == cur_to:
                        # there is nowhere to divide further - let's pass the error up
                        raise
                    mid = (cur_from + cur_to) // 2
                    # recursively download the two halves
                    left = self._fetch_logs_range(cur_from, mid)
                    right = self._fetch_logs_range(mid + 1, cur_to)
                    return left + right

                # other temporary errors - retry with backoff
                if attempt >= self._max_retries:
                    raise
                delay = 0.5 * (2 ** attempt)
                time.sleep(delay)
                attempt += 1

    def collect_transfer_participants(self, start_block: int, end_block: int) -> set[ChecksumAddress]:
        deploy_block = self._resolver.resolve_deploy_block(self._token.address, end_block)
        safe_start = max(0, start_block)
        effective_start = max(safe_start, deploy_block)
        holders: set[ChecksumAddress] = set()
        if effective_start > end_block:
            return holders

        # We split the entire range into large non-overlapping pieces
        ranges: list[tuple[int, int]] = []
        frm = effective_start
        while frm <= end_block:
            to = min(end_block, frm + self._chunk - 1)
            ranges.append((frm, to))
            frm = to + 1

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            futures = [pool.submit(self._fetch_logs_range, a, b) for (a, b) in ranges]
            for fut in tqdm(as_completed(futures), total=len(futures), desc="Scanning logs"):
                logs = fut.result()
                for log in logs:
                    if len(log["topics"]) >= 3:
                        from_addr = Web3.to_checksum_address("0x" + log["topics"][1].hex()[-40:])
                        to_addr = Web3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])
                        if from_addr.lower() != ZERO:
                            holders.add(from_addr)
                        if to_addr.lower() != ZERO:
                            holders.add(to_addr)
        return holders


# --- Rate limiter (token bucket) ---

class RateLimiter:
    """Simple thread-safe limiter: no more than N calls in the last 1.0 seconds."""

    def __init__(self, rps: int):
        self.rps = max(1, int(rps))
        self._lock = threading.Lock()
        self._hits = deque()  # timestamps of last hits (seconds)

    def acquire(self):
        while True:
            with self._lock:
                now = time.time()
                # throwing out old hits
                while self._hits and now - self._hits[0] >= 1.0:
                    self._hits.popleft()
                if len(self._hits) < self.rps:
                    self._hits.append(now)
                    return
                # you need to wait until space becomes available
                wait_for = 1.0 - (now - self._hits[0])
            if wait_for > 0:
                time.sleep(wait_for)


# --- Balance reader (threaded + retries) ---

class BalanceReaderThreaded(BalanceReader):
    """Parallel reader with RPS limitation and retries."""

    def __init__(self, token: TokenGateway, max_workers: int, rps_limit: int,
                 max_retries: int = 5, backoff_base: float = 0.5):
        self._token = token
        self._workers = max(1, int(max_workers))
        self._limiter = RateLimiter(rps_limit)
        self._max_retries = max(0, int(max_retries))
        self._base = backoff_base

    def _read_one(self, addr: ChecksumAddress, block: int) -> tuple[ChecksumAddress, int]:
        attempt = 0
        while True:
            self._limiter.acquire()
            try:
                bal = self._token.balance_of(addr, block)
                return addr, bal
            except Exception as e:
                logger.warning(f"Balance read error for {addr} @ {block}: {e}")
                if attempt >= self._max_retries:
                    return addr, 0
                # exponential delay with low jitter
                delay = self._base * (2 ** attempt)
                time.sleep(delay)
                attempt += 1

    def read_balances(self, holders: Iterable[ChecksumAddress], block: int) -> dict[str, int]:
        unique = sorted(set(holders))
        balances: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            futures = [pool.submit(self._read_one, addr, block) for addr in unique]
            for f in tqdm(as_completed(futures), total=len(unique), desc="Reading balances"):
                addr, bal = f.result()
                if bal > 0:
                    balances[addr] = bal
        return balances


# --- Writer ---

class CsvWriter(SnapshotWriter):
    def write(self, rows: list[dict], out_name: str) -> str:
        df = pd.DataFrame(rows)
        if not df.empty:
            # sort by size without changing the column type (remains a row)
            df = df.sort_values("balance_wei", key=lambda s: s.map(int), ascending=False)
        df.to_csv(out_name, index=False)
        return out_name


# --- Snapshot service (Application layer) ---

class SnapshotService:
    """Glues everything together: scanning, reading balances, metadata, recording results"""

    def __init__(
            self,
            provider: EthProvider,
            token: Web3TokenGateway,
            log_scanner: LogScanner,
            balance_reader: BalanceReader,
            writer: SnapshotWriter,
            symbol_fallback: str,
            decimals_fallback: int,
    ):
        self._provider = provider
        self._token = token
        self._scanner = log_scanner
        self._balances = balance_reader
        self._writer = writer
        self._symbol_fallback = symbol_fallback
        self._decimals_fallback = decimals_fallback

    def run_snapshot(self, snapshot_block: int, start_block: int | None = None) -> tuple[str, dict[str, int]]:
        self._provider.ensure_block_exists(snapshot_block)

        sym = self._safe_symbol(snapshot_block)
        dec = self._safe_decimals(snapshot_block)
        total_supply = self._safe_total_supply(snapshot_block)

        real_start = 0 if start_block is None else int(start_block)
        holders = self._scanner.collect_transfer_participants(real_start, snapshot_block)

        balances = self._balances.read_balances(holders, snapshot_block)

        rows = [
            {
                "address": addr,
                "balance_wei": str(raw),
                "balance_human": float(raw) / (10 ** dec),
            }
            for addr, raw in balances.items()
        ]

        out_name = f"snapshot_{sym}_{snapshot_block}.csv"
        path = self._writer.write(rows, out_name)

        if total_supply is not None:
            s = sum(int(v) for v in balances.values())
            logger.info(f"[check] Sum(balances) vs totalSupply @ {snapshot_block}: {s} / {total_supply}")

        logger.info(f"[ok] Holders: {len(holders)}, non-zero: {len(balances)} -> {path}")
        logger.info(
            f"[ok] Token: {sym}, Decimals: {dec}, TotalSupply: {total_supply if total_supply is not None else 'N/A'}"
        )

        return path, balances

    # --- helpers
    def _safe_symbol(self, block: int) -> str:
        try:
            return self._token.symbol(block)
        except ReadContractError:
            return self._symbol_fallback

    def _safe_decimals(self, block: int) -> int:
        try:
            return self._token.decimals(block)
        except ReadContractError:
            return self._decimals_fallback

    def _safe_total_supply(self, block: int) -> Optional[int]:
        try:
            return self._token.total_supply(block)
        except ReadContractError:
            return None


# =================================
# ========== Composition ==========
# =================================

def build_app(cfg: SnapshotConfig) -> SnapshotService:
    provider = Web3Provider(cfg.rpc_url, http_timeout=cfg.http_timeout)
    token = Web3TokenGateway(provider, cfg.token_address, cfg.symbol_fallback, cfg.decimals_fallback)
    if not cfg.snapshot_block:
        cfg.snapshot_block = provider.w3().eth.get_block("latest")['number']
        logger.info(f"No snapshot block specified, using latest: {cfg.snapshot_block}")
    resolver: DeployBlockResolver = (
        FixedDeployResolver(provider, cfg.known_deploy_block)
        if cfg.known_deploy_block is not None
        else AutoBinarySearchDeployResolver(provider)
    )

    scanner = TopicTransferScanner(
        provider=provider,
        token=token,
        logs_chunk=cfg.logs_chunk,
        deploy_resolver=resolver,
        debug=cfg.debug_logs,
        max_workers=cfg.logs_max_workers,  # <-- NEW
        rps_limit=cfg.logs_rps_limit,  # <-- NEW
        max_retries=cfg.logs_max_retries,  # <-- NEW
    )

    reader = BalanceReaderThreaded(
        token=token,
        max_workers=cfg.max_workers,
        rps_limit=cfg.rps_limit,
        max_retries=cfg.max_retries,
        backoff_base=cfg.backoff_base,
    )
    writer = CsvWriter()

    return SnapshotService(
        provider=provider,
        token=token,
        log_scanner=scanner,
        balance_reader=reader,
        writer=writer,
        symbol_fallback=cfg.symbol_fallback,
        decimals_fallback=cfg.decimals_fallback,
    )


# ================================
# ============= CLI ==============
# ================================

if __name__ == "__main__":
    # === Config scripts ===
    config = SnapshotConfig(
        rpc_url="https://lb.drpc.org/ethereum/AraUCj6z2EF7vENFig8OOXDjdvyaDhcR8Jblgk2scBzi",
        token_address="0x0908078Da2935A14BC7a17770292818C85b580dd",
        start_block=0,  # you can use 0 - the scanner will "clamp" itself before deployment
        # known_deploy_block=23447751,
        symbol_fallback="TOKEN",
        decimals_fallback=18,
        max_workers=24,  # select for CPU/provider
        rps_limit=90,
        max_retries=5,
        backoff_base=0.5,
        http_timeout=60,
        debug_logs=False,
        logs_chunk=5_000,
        logs_max_workers=8,
        logs_rps_limit=200,
        logs_max_retries=5,
    )

    app = build_app(config)
    with logging_redirect_tqdm():
        app.run_snapshot(config.snapshot_block, config.start_block)
