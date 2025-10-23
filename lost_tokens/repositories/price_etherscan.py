import logging
from decimal import Decimal
from ports import PriceRepository, TokenMeta, EtherscanLike


class EtherscanPriceRepository(PriceRepository):
    def __init__(self, etherscan: EtherscanLike) -> None:
        self.etherscan = etherscan

    async def get_prices(self, token_addresses: list[str]) -> dict[str, TokenMeta]:
        data = await self.etherscan.get_prices_bulk()
        ret: dict[str, TokenMeta] = {}
        for addr, meta in data.items():
            price = Decimal(str(meta.get("price", "0") or "0"))
            symbol = meta.get("symbol") or ""
            decimals = int(meta.get("decimals", 18))
            logo = meta.get("logo") or None
            ret[addr.lower()] = TokenMeta(
                address=addr,
                symbol=symbol,
                decimals=decimals,
                price_usd=price,
                logo=logo
            )

        logging.getLogger("prices").info(f"Loaded prices: {len(ret)} items (requested tokens: {len(token_addresses)})")
        return ret


class NullPriceRepository(PriceRepository):
    async def get_prices(self, token_addresses: list[str]) -> dict[str, TokenMeta]:
        return {}
