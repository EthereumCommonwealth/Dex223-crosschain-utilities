import asyncio
import logging
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

log = logging.getLogger("node_pool")


class Web3NodePool:
    """
    A pool of Web3 clients with round-robin and their own aiohttp sessions.
    """

    def __init__(self, urls: list[str], request_timeout: float = 8.0) -> None:
        if not urls:
            raise ValueError("Web3NodePool: empty RPC URL list")

        urls = [u.strip() for u in urls if u and u.strip()]
        urls = list(dict.fromkeys(urls))  # dedup

        self._timeout = float(request_timeout)
        self._clients: list[AsyncWeb3] = []

        for u in urls:
            provider = AsyncHTTPProvider(
                u,
                request_kwargs={"timeout": self._timeout},
            )
            self._clients.append(AsyncWeb3(provider))

        self._rr = 0
        self._lock = asyncio.Lock()
        log.info(f"Web3NodePool: {len(self._clients)} providers initialized (timeout={self._timeout:.1f}s)")

    async def next_client(self) -> AsyncWeb3:
        async with self._lock:
            c = self._clients[self._rr % len(self._clients)]
            self._rr += 1
            return c

    async def aclose(self):
        log.info("Web3NodePool: sessions closed")
