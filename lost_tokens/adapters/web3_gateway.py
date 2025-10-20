import asyncio
import logging
from decimal import Decimal
from typing import Optional, Tuple

from web3 import AsyncWeb3
from web3.exceptions import ContractLogicError, BadFunctionCallOutput

from ports import TokenGateway, TokenMeta
from utils.rate_limiter import RateLimiter
from adapters.node_pool import Web3NodePool

log = logging.getLogger("web3_gateway")

ERC20_ABI_MIN = [
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}],
     "type": "function"},
    {"constant": True, "inputs": [{"name": "owner", "type": "address"}], "name": "balanceOf",
     "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
]


class Web3TokenGateway(TokenGateway):
    def __init__(
            self,
            node_pool: Web3NodePool,
            limiter: RateLimiter,
            retries: int,
            base_delay: float,
            call_timeout: float = 5.0,
    ) -> None:
        self._nodes = node_pool
        self._limiter = limiter
        self._retries = max(0, int(retries))
        self._base_delay = float(base_delay)
        self._call_timeout = float(call_timeout)

    async def _retry(self, fn, label: str):
        last_exc = None
        for attempt in range(self._retries + 1):
            await self._limiter.acquire()
            w3 = await self._nodes.next_client()
            try:
                return await fn(w3)
            except (ContractLogicError, BadFunctionCallOutput, ValueError, asyncio.TimeoutError) as e:
                last_exc = e
                if attempt < self._retries:
                    delay = self._base_delay * (2 ** attempt)
                    log.info(f"retry {label} attempt={attempt + 1}/{self._retries} delay={delay:.2f}s err={e!r}")
                    await asyncio.sleep(delay)
                else:
                    break
            except Exception as e:
                last_exc = e
                break
        raise last_exc

    async def get_token_meta(self, token_address: str) -> TokenMeta:
        addr = AsyncWeb3.to_checksum_address(token_address)

        async def _symbol(w3: AsyncWeb3):
            return await w3.eth.contract(address=addr, abi=ERC20_ABI_MIN).functions.symbol().call()

        async def _decimals(w3: AsyncWeb3):
            return await w3.eth.contract(address=addr, abi=ERC20_ABI_MIN).functions.decimals().call()

        symbol, decimals = "", 18
        try:
            symbol = await self._retry(_symbol, f"symbol({addr})")
        except Exception:
            pass
        try:
            decimals = await self._retry(_decimals, f"decimals({addr})")
        except Exception:
            pass

        return TokenMeta(address=addr, symbol=symbol or addr[:6], decimals=int(decimals) if decimals else 18,
                         price_usd=Decimal("0"))

    async def balance_of(self, token_address: str, holder: str, decimals_hint: Optional[int] = None) -> Tuple[
        int, Decimal]:
        taddr = AsyncWeb3.to_checksum_address(token_address)
        haddr = AsyncWeb3.to_checksum_address(holder)

        async def _balance(w3: AsyncWeb3):
            return await w3.eth.contract(address=taddr, abi=ERC20_ABI_MIN).functions.balanceOf(haddr).call()

        try:
            raw = await asyncio.wait_for(
                self._retry(_balance, f"balanceOf({taddr},{haddr})"),
                timeout=self._call_timeout or 15.0,
            )
        except Exception as e:
            logging.warning(f"Failed to get balanceOf({taddr}, {haddr}): {e!r}")
            return 0, Decimal(0)

        dec = decimals_hint if decimals_hint is not None else 18
        amount = Decimal(raw) / Decimal(10 ** int(dec)) if dec >= 0 else Decimal(0)
        return int(raw), amount
