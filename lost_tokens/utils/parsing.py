import re
from web3 import AsyncWeb3

ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")


def parse_addresses_from_text(text: str) -> list[str]:
    address = ADDRESS_RE.findall(text or "")
    return [AsyncWeb3.to_checksum_address(a) for a in address]
