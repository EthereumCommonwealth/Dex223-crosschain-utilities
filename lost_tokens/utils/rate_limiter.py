import asyncio
import time


class RateLimiter:
    """
    A simple token-bucket limiter for the entire process.
    RPS — global request per second (RPS) limit (for all tokens and holders combined).
    """

    def __init__(self, rps: int) -> None:
        self._rps = max(1, int(rps))
        self._tokens = float(self._rps)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                # We are replenishing with fractional tokens
                self._tokens = min(self._rps, self._tokens + elapsed * self._rps)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # How long will it take until 1 token appears
                need = 1.0 - self._tokens
                sleep_s = max(need / self._rps, 0.001)
            await asyncio.sleep(sleep_s)
