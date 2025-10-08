#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Airdrop (SOLID, async, EIP-1559, checkpoint, concurrency).
    - SRP: Each class does one thing.
    - OCP: Easily add a new stabilizer/filter/checkpoint store.
    - LSP: Implementation replaced by protocol.
    - ISP: Narrow interfaces (FeePolicy, NonceAllocator, TxSender, etc.).
    - DIP: Composition via abstractions.

CSV leading with columns:
    address, Balance_wei[, Balance_human]

Dependencies:
    pip install web3 pandas tqdm python-dotenv
"""

from __future__ import annotations

import os
import sys
import json
import time
import math
import signal
import asyncio
import logging
import argparse
from dataclasses import dataclass
from typing import Protocol, Iterable, Dict, Tuple, List, Optional, Set, Hashable

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from eth_account import Account
from web3 import Web3, AsyncWeb3
from web3.types import TxParams, HexBytes, ChecksumAddress
from web3.providers.rpc import AsyncHTTPProvider

env = load_dotenv('.env')

# =========================
# ======= Logging =========
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("airdrop-solid")


# =========================
# ======= Config ==========
# =========================
@dataclass(frozen=True)
class AirdropConfig:
    rpc_url: str
    token_address: str
    csv_path: str
    mode: str  # mirror | scale | flat
    scale_total_human: Optional[float] = None  # for scale
    flat_amount_human: Optional[float] = None  # for flat
    decimals_override: Optional[int] = None
    chain_id: Optional[int] = None
    pk: Optional[str] = None
    pk_file: Optional[str] = None
    concurrency: int = 15
    rps_limit: int = 4
    retries: int = 3
    gas_limit: Optional[int] = None
    max_fee_mult: float = 1.0
    priority_mult: float = 1.0
    start_index: int = 0
    limit: Optional[int] = None
    min_balance_wei: int = 0
    skip_zero: bool = False
    allowlist_path: Optional[str] = None
    denylist_path: Optional[str] = None
    checkpoint_path: str = "airdrop_checkpoint.jsonl"
    dry_run: bool = False
    column: str = "balance_wei"


# =========================
# ===== Protocols =========
# =========================
class CsvReader(Protocol):
    def read(self, path: str) -> pd.DataFrame: ...


class DistributionStrategy(Protocol):
    async def build_distribution(self, df: pd.DataFrame, decimals: int) -> Dict[str, int]: ...


class AddressFilter(Protocol):
    def apply(self, df: pd.DataFrame) -> pd.DataFrame: ...


class FeePolicy(Protocol):
    async def suggest(self) -> Tuple[int, int]:  # (maxFeePerGas, maxPriorityFeePerGas)
        ...


class NonceAllocator(Protocol):
    async def starting_nonce(self) -> int: ...

    def nonce_for(self, local_order: int) -> int: ...


class TxSender(Protocol):
    async def estimate_gas(self, tx: TxParams, sender: str) -> int: ...

    async def send_raw(self, tx: TxParams, pk: str) -> str: ...


class CheckpointStore(Protocol):
    def done_indices(self) -> Set[int]: ...

    def save(self, idx: int, tx_hash: str, to: str, amount_wei: int) -> None: ...


class RateLimiter(Protocol):
    async def acquire(self) -> None: ...


# =========================
# ==== Implementations ====
# =========================
# --- Reader
class PandasCsvReader(CsvReader):
    def read(self, path: str) -> pd.DataFrame:
        if not os.path.isfile(path):
            raise SystemExit(f"CSV not found: {path}")
        df = pd.read_csv(path)
        if "address" not in df.columns:
            raise SystemExit("CSV must contain 'address'")
        if "balance_wei" not in df.columns and "balance_human" not in df.columns:
            raise SystemExit("CSV must contain 'balance_wei' or 'balance_human'")
        return df


# --- Filters
def _read_addr_set(path: Optional[str]) -> Optional[Set[str]]:
    if not path:
        return None
    s: Set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if t:
                s.add(t.lower())
    return s


class CompositeFilter(AddressFilter):
    def __init__(self, min_balance_wei: int, skip_zero: bool,
                 allowlist_path: Optional[str], denylist_path: Optional[str]):
        self.min_balance_wei = min_balance_wei
        self.skip_zero = skip_zero
        self.allowset = _read_addr_set(allowlist_path)
        self.denyset = _read_addr_set(denylist_path)

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        res = df.copy()
        if self.skip_zero and "balance_wei" in res.columns:
            res = res[res["balance_wei"].astype(str).map(int) > 0]
        if self.min_balance_wei > 0 and "balance_wei" in res.columns:
            res = res[res["balance_wei"].astype[str].map(int) >= self.min_balance_wei]
        if self.allowset:
            res = res[res["address"].astype(str).str.lower().isin(self.allowset)]
        if self.denyset:
            res = res[~res["address"].astype(str).str.lower().isin(self.denyset)]
        return res.reset_index(drop=True)


# --- Strategies
class MirrorStrategy(DistributionStrategy):
    def __init__(self, column: str = "balance_wei"):
        self.column = column

    async def build_distribution(self, df: pd.DataFrame, decimals: int) -> Dict[str, int]:
        if self.column != "balance_wei":
            raise SystemExit("MirrorStrategy requires column=balance_wei")
        out = {}
        for addr, w in zip(df["address"].astype(str), df["balance_wei"].astype(str).map(int)):
            if w > 0:
                out[Web3.to_checksum_address(addr)] = int(w)
        return out


class ScaleStrategy(DistributionStrategy):
    def __init__(self, total_human: float, column: str = "balance_wei"):
        self.total_human = total_human
        self.column = column

    async def build_distribution(self, df: pd.DataFrame, decimals: int) -> Dict[str, int]:
        target_total_wei = int(self.total_human * (10 ** decimals))
        if self.column == "balance_wei":
            weights = df["balance_wei"].astype(str).map(int)
            total_src_wei = int(weights.sum())
        else:
            weights = df["balance_human"].astype(float)
            total_src_wei = int((weights * (10 ** decimals)).sum())
        if total_src_wei == 0:
            raise SystemExit("Source total is zero, cannot scale")
        out: Dict[str, int] = {}
        for addr, w in zip(df["address"].astype(str), weights):
            w_int = int(w) if self.column == "balance_wei" else int(w * (10 ** decimals))
            part = (w_int * target_total_wei) // total_src_wei
            if part > 0:
                out[Web3.to_checksum_address(addr)] = part
        return out


class FlatStrategy(DistributionStrategy):
    def __init__(self, each_human: float):
        self.each_human = each_human

    async def build_distribution(self, df: pd.DataFrame, decimals: int) -> Dict[str, int]:
        each = int(self.each_human * (10 ** decimals))
        out: Dict[str, int] = {}
        if each <= 0:
            return out
        for addr in df["address"].astype(str):
            out[Web3.to_checksum_address(addr)] = each
        return out


# --- FeePolicy
class Web3FeePolicy(FeePolicy):
    def __init__(self, w3: AsyncWeb3, max_fee_mult: float = 1.0, priority_mult: float = 1.0):
        self.w3 = w3
        self.max_fee_mult = max_fee_mult
        self.priority_mult = priority_mult

    async def suggest(self) -> Tuple[int, int]:
        try:
            prio = int(await self.w3.eth.max_priority_fee)
        except Exception:
            prio = Web3.to_wei(1, "gwei")
        prio = int(prio * self.priority_mult)
        block = await self.w3.eth.get_block("pending")
        base = int(block.get("baseFeePerGas", Web3.to_wei(1, "gwei")))
        max_fee = int(base * 1.4) + prio
        max_fee = int(max_fee * self.max_fee_mult)
        return max_fee, prio


# --- NonceAllocator
class LinearNonceAllocator(NonceAllocator):
    def __init__(self, w3: AsyncWeb3, sender: str, forced: Optional[int] = None):
        self.w3 = w3
        self.sender = sender
        self._base = forced
        self._cached = None

    async def starting_nonce(self) -> int:
        if self._base is not None:
            self._cached = self._base
            return self._base
        self._cached = int(await self.w3.eth.get_transaction_count(self.sender, "pending"))
        return self._cached

    def nonce_for(self, local_order: int) -> int:
        if self._cached is None and self._base is None:
            raise RuntimeError("starting_nonce() must be called before nonce_for()")
        base = self._cached if self._cached is not None else self._base
        return int(base) + int(local_order)


# --- TxSender
ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "decimals",
     "outputs": [{"name": "", "type": "uint8"}], "type": "function", "stateMutability": "view"},
    {"constant": True, "inputs": [], "name": "symbol",
     "outputs": [{"name": "", "type": "string"}], "type": "function", "stateMutability": "view"},
    {"constant": False, "inputs": [{"name": "to", "type": "address"}, {"name": "value", "type": "uint256"}],
     "name": "transfer", "outputs": [{"name": "", "type": "bool"}], "type": "function",
     "stateMutability": "nonpayable"},
    {"constant": True, "inputs": [{"name": "owner", "type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function",
     "stateMutability": "view"},
]


def clamp_uint256(x: int) -> int:
    return max(0, min(x, 2 ** 256 - 1))


class Web3TxSender(TxSender):
    def __init__(self, w3: AsyncWeb3, token_addr: str, abi: Optional[dict] = None):
        if not abi:
            abi = ERC20_ABI
        self.w3 = w3
        self.token = self.w3.eth.contract(address=Web3.to_checksum_address(token_addr), abi=abi)

    async def estimate_gas(self, tx: TxParams, sender: str) -> int:
        est = int(await self.w3.eth.estimate_gas({**tx, "from": sender}))
        return int(est * 1.15)

    async def send_raw(self, tx: TxParams, pk: str) -> str:
        signed = Account.sign_transaction(tx, pk)
        txh: HexBytes = await self.w3.eth.send_raw_transaction(signed.raw_transaction)
        return txh.hex()


# --- Checkpoint
class JsonlCheckpoint(CheckpointStore):
    def __init__(self, path: str):
        self.path = path

    def done_indices(self) -> Set[int]:
        if not os.path.isfile(self.path):
            return set()
        s: Set[int] = set()
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    j = json.loads(line)
                    s.add(int(j.get("index", -1)))
                except Exception:
                    continue
        return s

    def save(self, idx: int, tx_hash: str, to: str, amount_wei: int) -> None:
        rec = {"index": idx, "tx": tx_hash, "to": to, "amount_wei": str(amount_wei), "ts": int(time.time())}
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")


# --- RateLimiter
class AsyncLeakyLimiter(RateLimiter):
    """Average RPS Limit: Guarantees a pause of ~1/rps between successes."""

    def __init__(self, rps: int):
        self.dt = 1.0 / max(1, int(rps))
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.time()
            wait = max(0.0, self._last + self.dt - now)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.time()


# =========================
# ====== Use-Case =========
# =========================
class AirdropUseCase:
    """Orchestrator: loads CSV, builds distribution, filters, and sends in parallel."""

    def __init__(
            self,
            cfg: AirdropConfig,
            w3: AsyncWeb3,
            csv_reader: CsvReader,
            filters: AddressFilter,
            strategy: DistributionStrategy,
            fee_policy: FeePolicy,
            nonce_alloc: NonceAllocator,
            sender: TxSender,
            checkpoint: CheckpointStore,
            limiter: RateLimiter,
    ):
        self.cfg = cfg
        self.w3 = w3
        self.csv_reader = csv_reader
        self.filters = filters
        self.strategy = strategy
        self.fee_policy = fee_policy
        self.nonce_alloc = nonce_alloc
        self.sender = sender
        self.checkpoint = checkpoint
        self.limiter = limiter

        self.token = self.sender.token  # reuse contract

    async def _resolve_decimals(self) -> int:
        if self.cfg.decimals_override is not None:
            return int(self.cfg.decimals_override)
        try:
            return int(await self.token.functions.decimals().call())
        except Exception:
            log.warning("Cannot read decimals(); defaulting to 18")
            return 18

    async def _sender_balance_warn(self, sender_addr: str, need_wei: int):
        try:
            bal = int(await self.token.functions.balanceOf(sender_addr).call())
            if bal < need_wei:
                log.warning(f"Token balance {bal} < required {need_wei}")
        except Exception as e:
            log.warning(f"Cannot read sender token balance: {e}")

    async def run(self, sender_addr: Optional[str], pk: Optional[str]) -> None:
        # load CSV
        df = self.csv_reader.read(self.cfg.csv_path)
        df = self.filters.apply(df)
        if sender_addr:
            df = df[df["address"].str.lower() != sender_addr.lower()]
        df = df.tail(3)  # last 3 rows

        # build distribution
        decimals = await self._resolve_decimals()
        dist = await self.strategy.build_distribution(df, decimals)
        if not dist:
            raise SystemExit("Nothing to distribute after filters")

        # keep input order; build rows
        rows: list[tuple[Hashable, ChecksumAddress, int]] = list()
        for i, row in df.iterrows():
            a = Web3.to_checksum_address(row["address"])
            if a in dist:
                rows.append((i, a, int(dist[a])))
        # slice
        start = max(0, int(self.cfg.start_index))
        end = len(rows) if self.cfg.limit is None else min(len(rows), start + int(self.cfg.limit))
        rows = rows[start:end]

        total_wei = sum(x[2] for x in rows)
        log.info(
            f"Recipients: {len(rows)}, total (wei): {total_wei}, total (human): {total_wei / (10 ** decimals):.8f}")

        for i in range(min(5, len(rows))):
            idx, to, amt = rows[i]
            log.info(f"Preview[{i}] -> {to} : {amt} wei")

        if self.cfg.dry_run:
            log.info("Dry-run enabled, exit before sending.")
            return

        if not sender_addr or not pk:
            raise SystemExit("PRIVATE_KEY / sender required for real sending")

        await self._sender_balance_warn(sender_addr, total_wei)

        # nonces
        base_nonce = await self.nonce_alloc.starting_nonce()
        done = self.checkpoint.done_indices()

        sem = asyncio.Semaphore(max(1, int(self.cfg.concurrency)))
        chain_id = int(self.cfg.chain_id or (await self.w3.eth.chain_id))

        async def send_one(local_order: int, idx: int, to: str, amount_wei: int) -> bool:
            if idx in done:
                return True

            async with sem:
                nonce = self.nonce_alloc.nonce_for(local_order)
                max_fee, prio = await self.fee_policy.suggest()

                data = self.token.encode_abi("transfer", args=[to, clamp_uint256(int(amount_wei))])
                tx: TxParams = {
                    "chainId": chain_id,
                    "to": self.token.address,
                    "nonce": nonce,
                    "type": 2,
                    "maxFeePerGas": max_fee,
                    "maxPriorityFeePerGas": prio,
                    "data": data,
                }
                # gas
                try:
                    gas = int(self.cfg.gas_limit) if self.cfg.gas_limit else await self.sender.estimate_gas(
                        tx,
                        sender_addr
                    )
                    tx["gas"] = gas
                except Exception as e:
                    log.error(f"[{idx}] estimate failed for {to}: {e}")
                    await self.limiter.acquire()
                    return False

                # retries
                for attempt in range(self.cfg.retries + 1):
                    try:
                        tx_hash = await self.sender.send_raw(tx, pk)
                        log.info(f"[{idx}] sent(nonce={nonce}) -> {to}, wei={amount_wei}, tx={tx_hash}")
                        self.checkpoint.save(idx, tx_hash, to, amount_wei)
                        await self.limiter.acquire()
                        df.loc[idx, "tx"] = tx_hash

                        return True
                    except Exception as e:
                        msg = str(e)
                        # For nonce conflicts, wait and repeat the same nonce
                        if "nonce too low" in msg.lower() or "replacement transaction underpriced" in msg.lower():
                            await asyncio.sleep(0.8 + attempt * 0.5)
                            continue
                        log.warning(f"[{idx}] send attempt {attempt} failed: {msg}")
                        await asyncio.sleep(0.8 + attempt * 0.5)
                await self.limiter.acquire()
                return False

        tasks: List[asyncio.Task] = []
        local_order = 0
        for idx, to, amt in rows:
            if idx in done:
                continue
            tasks.append(asyncio.create_task(send_one(base_nonce + local_order - base_nonce, idx, to, amt)))
            local_order += 1

        log.info(f"Concurrent sends: {sem._value} | tasks: {len(tasks)} | base_nonce: {base_nonce}")

        results = []
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Airdropping"):
            results.append(await f)
        sent = sum(1 for r in results if r)
        failures = len(results) - sent
        df.to_csv(f'result_{self.cfg.csv_path}', index=False)
        log.info(f"CSV updated with tx hashes: {self.cfg.csv_path}")
        log.info(f"Done. Sent: {sent}, Failures: {failures}, Checkpoint: {self.cfg.checkpoint_path}")


def build_strategy(cfg: AirdropConfig) -> DistributionStrategy:
    if cfg.mode == "mirror":
        return MirrorStrategy(column=cfg.column)
    if cfg.mode == "scale":
        if cfg.scale_total_human is None:
            raise SystemExit("--scale-total is required for mode=scale")
        return ScaleStrategy(total_human=float(cfg.scale_total_human), column=cfg.column)
    if cfg.mode == "flat":
        if cfg.flat_amount_human is None:
            raise SystemExit("--flat-amount is required for mode=flat")
        return FlatStrategy(each_human=float(cfg.flat_amount_human))
    raise SystemExit(f"Unknown mode: {cfg.mode}")


# =========================
# ========= CLI ===========
# =========================
def parse_args() -> AirdropConfig:
    p = argparse.ArgumentParser(description="SOLID async ERC-20 airdrop from CSV")
    p.add_argument("--rpc", required=True)
    p.add_argument("--token", required=True)
    p.add_argument("--csv", required=True)
    p.add_argument("--mode", choices=["mirror", "scale", "flat"], required=True)
    p.add_argument("--scale-total", type=float)
    p.add_argument("--flat-amount", type=float)
    p.add_argument("--decimals", type=int)
    p.add_argument("--chain-id", type=int)
    p.add_argument("--concurrency", type=int, default=15)
    p.add_argument("--rps", type=int, default=4)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--gas-limit", type=int)
    p.add_argument("--max-fee-mult", type=float, default=1.0)
    p.add_argument("--priority-mult", type=float, default=1.0)
    p.add_argument("--from", dest="start_index", type=int, default=0)
    p.add_argument("--limit", type=int)
    p.add_argument("--min-balance-wei", type=int, default=0)
    p.add_argument("--skip-zero", action="store_true")
    p.add_argument("--allowlist")
    p.add_argument("--denylist")
    p.add_argument("--checkpoint", default="airdrop_checkpoint.jsonl")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--column", default="balance_wei")
    a = p.parse_args()
    return AirdropConfig(
        rpc_url=a.rpc,
        token_address=a.token,
        csv_path=a.csv,
        mode=a.mode,
        scale_total_human=a.scale_total,
        flat_amount_human=a.flat_amount,
        decimals_override=a.decimals,
        chain_id=a.chain_id,
        pk=os.getenv("PRIVATE_KEY"),
        concurrency=a.concurrency,
        rps_limit=a.rps,
        retries=a.retries,
        gas_limit=a.gas_limit,
        max_fee_mult=a.max_fee_mult,
        priority_mult=a.priority_mult,
        start_index=a.start_index,
        limit=a.limit,
        min_balance_wei=a.min_balance_wei,
        skip_zero=a.skip_zero,
        allowlist_path=a.allowlist,
        denylist_path=a.denylist,
        checkpoint_path=a.checkpoint,
        dry_run=a.dry_run,
        column=a.column,
    )


# =========================
# ========= Main ==========
# =========================
async def main(cfg: AirdropConfig):
    # graceful stop
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, lambda: sys.exit(1))
        except NotImplementedError:
            pass

    w3 = AsyncWeb3(AsyncHTTPProvider(cfg.rpc_url, request_kwargs={"timeout": 120}))
    if not await w3.is_connected():
        raise SystemExit("Cannot connect to RPC")

    # sender
    pk = cfg.pk
    print(pk)
    # pk = load_private_key_direct_or_env(cfg.pk, cfg.pk_file, cfg.dry_run)
    sender_addr = Account.from_key(pk).address if pk else None
    if sender_addr:
        log.info(f"Sender: {sender_addr}")

    # dependencies
    csv_reader = PandasCsvReader()
    filters = CompositeFilter(cfg.min_balance_wei, cfg.skip_zero, cfg.allowlist_path, cfg.denylist_path)
    strategy = build_strategy(cfg)
    fee_policy = Web3FeePolicy(w3, cfg.max_fee_mult, cfg.priority_mult)
    nonce_alloc = LinearNonceAllocator(w3, sender_addr or "0x" + "0" * 40, None)
    tx_sender = Web3TxSender(w3, cfg.token_address)
    checkpoint = JsonlCheckpoint(cfg.checkpoint_path)
    limiter = AsyncLeakyLimiter(cfg.rps_limit)

    use_case = AirdropUseCase(
        cfg=cfg,
        w3=w3,
        csv_reader=csv_reader,
        filters=filters,
        strategy=strategy,
        fee_policy=fee_policy,
        nonce_alloc=nonce_alloc,
        sender=tx_sender,
        checkpoint=checkpoint,
        limiter=limiter,
    )

    await use_case.run(sender_addr, pk if not cfg.dry_run else None)


if __name__ == "__main__":
    config = parse_args()
    asyncio.run(main(config))
