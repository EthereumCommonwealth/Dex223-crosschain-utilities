from pathlib import Path
from typing import Optional
import json
from ports import CacheRepository


class FileCacheRepository(CacheRepository):
    def __init__(self, base_dir: Path) -> None:
        self.abi_full = base_dir / "abi_full"
        self.abi_functions = base_dir / "abi_functions"
        self.contracts = base_dir / "contracts"
        self.abi_full.mkdir(exist_ok=True)
        self.abi_functions.mkdir(exist_ok=True)
        self.contracts.mkdir(exist_ok=True)

    async def get_abi(self, address: str) -> Optional[list]:
        p = self.abi_functions / f"{address.lower()}.abi"
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:  # noqa
            return None

    async def save_abi(self, address: str, abi: list) -> None:
        full = self.abi_full / f"{address.lower()}.abi"
        funcs = self.abi_functions / f"{address.lower()}.abi"
        full.write_text(json.dumps(abi), encoding="utf-8")
        f_names = [a["name"].lower() for a in abi if
                   isinstance(a, dict) and a.get("type") == "function" and a.get("name")]
        funcs.write_text(json.dumps(f_names), encoding="utf-8")

    async def get_source(self, address: str) -> Optional[str]:
        p = self.contracts / f"{address.lower()}.sol"
        if not p.exists():
            return None
        return p.read_text(encoding="utf-8")

    async def save_source(self, address: str, source: str) -> None:
        p = self.contracts / f"{address.lower()}.sol"
        p.write_text(source or "", encoding="utf-8")
