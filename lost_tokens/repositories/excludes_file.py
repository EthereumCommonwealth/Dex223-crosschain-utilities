import json
from pathlib import Path
from ports import ExcludesRepository


class FileExcludesRepository(ExcludesRepository):
    def __init__(self, path: Path) -> None:
        self._path = path

    async def get_excludes(self) -> dict[str, list[str]]:
        if not self._path.exists():
            return {}
        arr = json.loads(self._path.read_text(encoding="utf-8"))
        out: dict[str, list[str]] = {}
        if isinstance(arr, list):
            for item in arr:
                k, vals = item[0].lower(), [v.lower() for v in item[1]]
                out[k] = vals
            return out
        if isinstance(arr, dict):
            return {k.lower(): [v.lower() for v in vals] for k, vals in arr.items()}
        return {}
