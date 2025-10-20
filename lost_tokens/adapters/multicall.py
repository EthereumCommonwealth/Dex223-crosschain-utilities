# adapters/multicall.py
from typing import List, Tuple
from web3 import AsyncWeb3
from eth_abi import encode, decode

# Multicall2 на Ethereum mainnet
MULTICALL2 = "0x5ba1e12693dc8f9c48aad8770482f4739beed696"
# method: tryAggregate(bool requireSuccess, (address target, bytes callData)[] calls) returns ((bool, bytes)[])
TRY_AGGREGATE_SELECTOR = AsyncWeb3.keccak(text="tryAggregate(bool,(address,bytes)[])")[:4]


def _encode_try_aggregate(calls: List[Tuple[str, bytes]], require_success: bool = False) -> bytes:
    # types: (bool, (address, bytes)[])
    types = ["bool", "(address,bytes)[]"]
    values = [require_success, [(c[0], c[1]) for c in calls]]
    return TRY_AGGREGATE_SELECTOR + encode(types, values)


def _decode_try_aggregate_result(data: bytes) -> List[Tuple[bool, bytes]]:
    # returns: (bool, bytes)[]
    return decode(["(bool,bytes)[]"], data)[0]


class MulticallClient:
    def __init__(self, w3: AsyncWeb3, address: str = MULTICALL2):
        self.w3 = w3
        self.address = AsyncWeb3.to_checksum_address(address)

    async def try_aggregate(self, calls: List[Tuple[str, bytes]]) -> List[Tuple[bool, bytes]]:
        if not calls:
            return []
        payload = _encode_try_aggregate(calls, require_success=False)
        res = await self.w3.eth.call({
            "to": self.address,
            "data": payload
        })
        return _decode_try_aggregate_result(res)
