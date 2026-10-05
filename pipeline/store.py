"""本地 JSON 存储：管道版本、运行记录、血缘记录的持久化与并发锁。"""
from __future__ import annotations

import threading
from pathlib import Path

from .models import LineageRecord, PipelineVersion, RunRecord


class Store:
    """基于本地 JSON 文件的存储，进程内用锁保证读写串行化。"""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._lock = threading.RLock()
        raise NotImplementedError

    # ---- 管道版本 ----
    def save_pipeline_version(self, version: PipelineVersion) -> None:
        raise NotImplementedError

    def get_pipeline_version(self, pipeline_id: str, version: int) -> PipelineVersion | None:
        raise NotImplementedError

    def latest_version(self, pipeline_id: str) -> PipelineVersion | None:
        raise NotImplementedError

    def list_versions(self, pipeline_id: str) -> list[PipelineVersion]:
        raise NotImplementedError

    # ---- 运行记录 ----
    def save_run(self, run: RunRecord) -> None:
        raise NotImplementedError

    def get_run(self, run_id: str) -> RunRecord | None:
        raise NotImplementedError

    def find_run_by_idempotency_key(self, pipeline_id: str, key: str) -> RunRecord | None:
        raise NotImplementedError

    def active_run_of(self, pipeline_id: str) -> RunRecord | None:
        raise NotImplementedError

    def list_runs(self, pipeline_id: str) -> list[RunRecord]:
        raise NotImplementedError

    # ---- 血缘 ----
    def append_lineage(self, record: LineageRecord) -> None:
        raise NotImplementedError

    def lineage_of_run(self, run_id: str) -> list[LineageRecord]:
        raise NotImplementedError
