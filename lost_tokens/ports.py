from typing import Protocol, Optional
from decimal import Decimal


class TokenMeta(dict):
    address: str
    symbol: str
    decimals: int
    price_usd: Decimal


class PriceRepository(Protocol):
    async def get_prices(self, token_addresses: list[str]) -> dict[str, TokenMeta]: ...


class ExcludesRepository(Protocol):
    async def get_excludes(self) -> dict[str, list[str]]: ...


class CacheRepository(Protocol):
    async def get_abi(self, address: str) -> Optional[list]: ...

    async def save_abi(self, address: str, abi: list) -> None: ...

    async def get_source(self, address: str) -> Optional[str]: ...

    async def save_source(self, address: str, source: str) -> None: ...


class EtherscanLike(Protocol):
    async def get_contract_abi(self, address: str) -> list: ...

    async def get_contract_source(self, address: str) -> Optional[str]: ...

    async def get_prices_bulk(self) -> dict[str, dict]: ...


class TokenGateway(Protocol):
    async def get_token_meta(self, token_address: str) -> TokenMeta: ...

    async def balance_of(self, token_address: str, holder: str, decimals_hint: Optional[int] = None) -> tuple[
        int, Decimal]: ...
