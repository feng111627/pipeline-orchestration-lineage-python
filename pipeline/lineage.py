"""血缘审计：查询运行血缘、按历史版本复现运行结论。"""
from __future__ import annotations

from .models import LineageRecord, RunRecord
from .store import Store


class LineageService:
    def __init__(self, store: Store) -> None:
        self._store = store
        raise NotImplementedError

    def lineage_of_run(self, run_id: str) -> list[LineageRecord]:
        raise NotImplementedError

    def replay(self, run_id: str) -> RunRecord:
        """按运行记录中锚定的管道版本与输入复现结论（只读，不产生副作用）。"""
        raise NotImplementedError
