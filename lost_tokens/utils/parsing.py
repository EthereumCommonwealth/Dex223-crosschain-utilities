import re
from web3 import AsyncWeb3

# The hex boundaries matter: without them a 32-byte value (transaction hash, storage slot, any
# bytes32 in the input files) matches its first 40 hex characters and yields a fabricated address,
# which is then scanned for balances and published as a real result.
ADDRESS_RE = re.compile(r"(?<![0-9a-fA-F])0x[0-9a-fA-F]{40}(?![0-9a-fA-F])")


def parse_addresses_from_text(text: str) -> list[str]:
    address = ADDRESS_RE.findall(text or "")
    return [AsyncWeb3.to_checksum_address(a) for a in address]
