import logging
from typing import Optional, Dict
import aiohttp

log = logging.getLogger("etherscan_client")


class EtherscanClient:
    def __init__(self, api_key: str, chain_id: int = 1) -> None:
        self.api_key = api_key
        self.chain_id = chain_id

    async def get_contract_abi(self, address: str) -> list:
        if not self.api_key:
            return []
        url = f"https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={self.api_key}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=30) as r:
                j = await r.json()
        if j.get("status") == "1":
            import json
            try:
                return json.loads(j["result"])
            except Exception:
                return []
        return []

    async def get_contract_source(self, address: str) -> Optional[str]:
        if not self.api_key:
            return None
        url = (f"https://api.etherscan.io/v2/api?module=contract&chainid={self.chain_id}&"
               f"action=getsourcecode&address={address}&apikey={self.api_key}")
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=30) as r:
                j = await r.json()
        if j.get("status") != "1":
            return None

        data = j["result"][0]
        src = data.get("SourceCode", "")

        # Unpacking Etherscan formats: plain / { "content": ... } / {"sources": {...}}
        def extract_sources(ob) -> str:
            out = []
            for k, v in ob.items():
                content = v.get("content", "")
                out.append(f"// {k}\n{content}\n")
            return "\n".join(out)

        if not src:
            return None
        if '"sources":' in src:
            s = src[1:-1]  # снимаем обёртку
            import json
            ob = json.loads(s)
            return extract_sources(ob["sources"]) if "sources" in ob else None
        if '"content":' in src:
            import json
            ob = json.loads(src)
            return extract_sources(ob)
        return src

    @staticmethod
    async def get_prices_bulk() -> dict[str, dict]:
        """
        We pull prices from the new endpoint:
        https://api.dex223.io/v1/cache/explores/tokens/prices?explorer=etherscan
        Answer: { "data": { <addr>: { "price": ..., "symbol": ..., "decimals": ... }, ... } }
        """
        url = "https://api.dex223.io/v1/cache/explores/tokens/prices?explorer=etherscan"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=30) as r:
                    if r.status != 200:
                        log.warning(f"prices_bulk http={r.status}")
                        return {}
                    j = await r.json()
                    data = j.get("data") or {}
                    result = {k.lower(): v for k, v in data.items() if isinstance(v, dict)}
                    return result
        except Exception as e:
            log.warning(f"prices_bulk error: {e}")
            return {}
