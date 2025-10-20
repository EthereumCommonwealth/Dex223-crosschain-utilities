# services/scanner.py
from __future__ import annotations
import asyncio
import logging
from time import monotonic
from decimal import Decimal
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

from web3 import AsyncWeb3
from eth_abi import encode, decode

from ports import (
    TokenGateway, PriceRepository, ExcludesRepository, CacheRepository, EtherscanLike, TokenMeta,
)
from adapters.multicall import MulticallClient
from utils.extract_rules import analyze_extract

log = logging.getLogger("scanner")

# ERC20 balanceOf(address) selector
BALANCE_OF_SELECTOR = AsyncWeb3.keccak(text="balanceOf(address)")[:4]


@dataclass
class BalanceRecord:
    contract: str
    raw_balance: int
    amount: Decimal
    exclude: bool = False
    notes: list[str] = field(default_factory=list)


@dataclass
class TokenScanResult:
    tokenAddress: str
    ticker: str
    decimals: int
    price: Decimal
    logo: Optional[str] = None
    hasExtract: bool = False
    hasBurnMint: bool = False
    records: list[BalanceRecord] = field(default_factory=list)
    asDollar: Decimal = Decimal("0")
    amount: Decimal = Decimal("0")


class LostTokensScannerService:
    """
    NxN, but fast: holders = tokens. For each holder, we call Multicall on all tokens in batches.
    """
    PROGRESS_EVERY = 50
    BATCH_SIZE = 250  # How many tokens are in one multicall batch

    def __init__(
            self,
            token_gateway: TokenGateway,
            price_repo: PriceRepository,
            excludes_repo: ExcludesRepository,
            cache_repo: CacheRepository,
            etherscan: EtherscanLike,
            collect_extract: bool,
            concurrency: int,
            token_timeout_sec: Optional[float] = None,
    ) -> None:
        self._token_gateway = token_gateway
        self._price_repo = price_repo
        self._excludes_repo = excludes_repo
        self._cache_repo = cache_repo
        self._etherscan = etherscan
        self._collect_extract = collect_extract

        self._holder_workers = max(1, int(concurrency))
        self._token_timeout_sec = token_timeout_sec

        self._w3_for_mc: Optional[AsyncWeb3] = None  # Let's take it from the gateway node_pool on the fly

    async def _ensure_w3(self) -> AsyncWeb3:
        if self._w3_for_mc is not None:
            return self._w3_for_mc
        # берём клиента из пула gateway (хак: у нас нет прямого доступа, но gateway.next_client есть косвенно)
        # добавим в gateway лёгкий helper — или просто возьмём мету и перехватим w3:
        # проще: в token_gateway.balance_of мы всё равно формируем контракт — но нам нужен сам client.
        # Поэтому создадим отдельный AsyncWeb3 прямо по тому же RPC, что в пуле не страшно:
        # ЛУЧШЕ: добавь метод get_any_client() в твой Web3NodePool и передай сюда при инициализации.
        # Чтобы не менять другие файлы, сделаем трюк: возьмём его из token_gateway._nodes
        w3 = await self._token_gateway._nodes.next_client()  # type: ignore
        self._w3_for_mc = w3
        return w3

    @staticmethod
    def _encode_balance_of(holder: str) -> bytes:
        # balanceOf(holder)
        return BALANCE_OF_SELECTOR + encode(["address"], [holder])

    async def _prefetch_token_infos(
            self, tokens: List[str], prices_map: Dict[str, TokenMeta]
    ) -> Dict[str, Dict]:
        """
        We're preparing the token metadata (symbol/decimals/price). Whenever possible, we'll take it from prices_map.
        We'll get the rest via gateway.get_token_meta.
        """
        out: Dict[str, Dict] = {}
        sem = asyncio.Semaphore(16)  # Let's limit simultaneous web3 meta calls

        async def one(addr: str):
            addr_l = addr.lower()
            hint = prices_map.get(addr_l)
            if hint and hint.get("symbol") and ("decimals" in hint):
                out[addr_l] = {
                    "address": AsyncWeb3.to_checksum_address(addr),
                    "ticker": hint["symbol"] or addr[:6],
                    "decimals": int(hint["decimals"]),
                    "price": Decimal(str(hint.get("price_usd", "0") or 0)),
                    "logo": None,
                }
                return
            async with sem:
                meta = await self._token_gateway.get_token_meta(addr)
            price_usd = Decimal(str(hint.get("price_usd", "0") if hint else "0"))
            if hint and hint.get("symbol") and not meta["symbol"]:
                meta["symbol"] = hint["symbol"]
            out[addr_l] = {
                "address": meta["address"],
                "ticker": meta["symbol"] or meta["address"][:6],
                "decimals": meta["decimals"],
                "price": price_usd,
                "logo": None,
            }

        await asyncio.gather(*(one(t) for t in tokens))
        return out

    async def scan(self, tokens: List[str]) -> List[TokenScanResult]:
        holders = list({AsyncWeb3.to_checksum_address(t) for t in tokens})
        tokens_cs = [AsyncWeb3.to_checksum_address(t) for t in tokens]
        if not tokens_cs:
            log.info("No tokens to scan")
            return []

        prices_map = await self._price_repo.get_prices(tokens_cs)
        excludes_map = await self._excludes_repo.get_excludes()

        # Prepare token information (ticker/decimals/price)
        infos = await self._prefetch_token_infos(tokens_cs, prices_map)

        # Let's prepare the resulting baskets for tokens
        results_by_token: Dict[str, TokenScanResult] = {}
        for t in tokens_cs:
            i = infos[t.lower()]
            results_by_token[t.lower()] = TokenScanResult(
                tokenAddress=i["address"],
                ticker=i["ticker"],
                decimals=i["decimals"],
                price=i["price"],
                logo=i["logo"],
            )

        # Queue holders
        q: asyncio.Queue[str] = asyncio.Queue()
        for h in holders:
            q.put_nowait(h)

        total = q.qsize()
        done = 0
        start_all = monotonic()

        w3 = await self._ensure_w3()
        mc = MulticallClient(w3)
        batch = self.BATCH_SIZE

        lock = asyncio.Lock()  # protect the records of results

        async def process_holder(holder_addr: str):
            nonlocal done
            holder_addr = AsyncWeb3.to_checksum_address(holder_addr)
            holder_lc = holder_addr.lower()

            # We go through ALL the tokens in batches, except for the holder itself
            # (to avoid creating a balanceOf token on yourself - you can leave it)
            idx = 0
            n = len(tokens_cs)
            while idx < n:
                chunk = tokens_cs[idx: idx + batch]
                idx += batch

                # prepare tryAggregate calls (target=token, data=balanceOf(holder))
                calls: List[Tuple[str, bytes]] = []
                for token_addr in chunk:
                    # You can skip token==holder, but sometimes it's also interesting - it doesn't interfere
                    calls.append((token_addr, self._encode_balance_of(holder_addr)))

                # one eth_call with multicall
                try:
                    results = await mc.try_aggregate(calls)
                except Exception as e:
                    log.debug("multicall failed for holder %s: %r", holder_addr, e)
                    continue

                # Let's analyze the answers
                for (token_addr, (ok, ret)) in zip(chunk, results):
                    if not ok or len(ret) < 32:
                        continue
                    try:
                        raw = int.from_bytes(ret[-32:], "big")  # uint256
                    except Exception:
                        continue
                    if raw <= 0:
                        continue

                    t_lc = token_addr.lower()
                    info = infos[t_lc]
                    dec = int(info["decimals"]) if info.get("decimals") is not None else 18
                    amount = Decimal(raw) / Decimal(10 ** dec)

                    rec = BalanceRecord(
                        contract=holder_addr,
                        raw_balance=raw,
                        amount=amount,
                        exclude=(holder_lc in set(excludes_map.get(t_lc, []))),
                    )
                    async with lock:
                        results_by_token[t_lc].records.append(rec)

            done += 1
            if (done % self.PROGRESS_EVERY == 0) or (done == total):
                elapsed = monotonic() - start_all
                rate = done / max(elapsed, 1e-6)
                eta = (total - done) / max(rate, 1e-6)
                log.info("[holders %d/%d] elapsed=%.1fs, ETA=%.1fs, rate=%.2f hol/s",
                         done, total, elapsed, eta, rate)

        async def worker():
            while True:
                try:
                    h = await q.get()
                except Exception:
                    return
                try:
                    await process_holder(h)
                finally:
                    q.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(self._holder_workers)]
        try:
            await q.join()
        finally:
            for w in workers: w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        # Post: collect sums, extract, etc.
        for t_lc, res in results_by_token.items():
            # extract-labels are optional
            if self._collect_extract:
                try:
                    ex = await self._load_extract_info(res.tokenAddress)
                    res.hasExtract = bool(ex.get("functions"))
                    res.hasBurnMint = bool(ex.get("burnMint"))
                except Exception:
                    pass
            # let's sum it up
            amount_sum = sum((r.amount for r in res.records), Decimal(0))
            res.amount = amount_sum
            res.asDollar = amount_sum * res.price

        out = list(results_by_token.values())
        out.sort(key=lambda r: r.asDollar, reverse=True)
        log.info("Scan finished: %d holders × %d tokens in %.1fs",
                 len(holders), len(tokens_cs), monotonic() - start_all)
        return out

    async def _load_extract_info(self, token_addr: str) -> dict:
        src = await self._cache_repo.get_source(token_addr)
        if not src:
            src = await self._etherscan.get_contract_source(token_addr)
            if src:
                await self._cache_repo.save_source(token_addr, src)
        return analyze_extract(token_addr, src or "")

    @staticmethod
    def format_report(results: List[TokenScanResult],
                      exclude_by_list: bool,
                      exclude_by_mint: bool,
                      exclude_by_extract: bool) -> tuple[str, Decimal, int]:
        total = Decimal(0)
        lines = []
        for res in results:
            sum_amount = Decimal(0)
            for rec in res.records:
                excluded = (
                        (exclude_by_list and rec.exclude) or
                        (exclude_by_extract and res.hasExtract) or
                        (exclude_by_mint and res.hasBurnMint)
                )
                if not excluded:
                    sum_amount += rec.amount
            usd = sum_amount * res.price
            total += usd

            header = f"\n{res.ticker} [{res.tokenAddress}]: {sum_amount:.5f} tokens lost / ${usd:.2f}"
            subs = []
            for rec in sorted(res.records, key=lambda r: r.amount, reverse=True):
                excluded = (
                        (exclude_by_list and rec.exclude) or
                        (exclude_by_extract and res.hasExtract) or
                        (exclude_by_mint and res.hasBurnMint)
                )
                prefix = "[X] " if excluded else ""
                subs.append(
                    f"Contract {prefix}{rec.contract} => {rec.amount:.5f} {res.ticker} ( ${(rec.amount * res.price):.2f} )")
            lines.append(header + "\n-----------------------------------------------\n" + "\n".join(subs))

        report = f"WHOLE SUM: ${total:.2f} for {len(results)} tokens\n" + "\n".join(lines)
        return report, total, len(results)
