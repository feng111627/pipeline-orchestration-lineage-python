"""门面服务：组合注册表、执行器与血缘，供各入口（HTTP/测试）统一调用。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .executor import HandlerRegistry, Orchestrator, StepHandler
from .lineage import LineageService
from .models import (LineageRecord, PipelineDefinition, PipelineVersion, Run)
from .registry import PipelineRegistry
from .store import JsonStore


class PipelineService:
    """所有入口共享同一个实例，从而共享同一把存储锁与同一套一致性规则。"""

    def __init__(self, db_path: str | Path = ":memory:"):
        self._store = JsonStore(db_path)
        self._handlers = HandlerRegistry()
        self._lineage = LineageService(self._store)
        self._registry = PipelineRegistry(self._store)
        self._orchestrator = Orchestrator(self._store, self._handlers,
                                          self._lineage)

    # 定义与版本 ---------------------------------------------------------
    def register_pipeline(self, definition: PipelineDefinition) -> PipelineVersion:
        return self._registry.register(definition)

    def update_pipeline(self, definition: PipelineDefinition,
                        note: str = "") -> PipelineVersion:
        return self._registry.update(definition, note)

    def rollback_pipeline(self, name: str, to_version: int) -> PipelineVersion:
        return self._registry.rollback(name, to_version)

    def list_versions(self, name: str) -> list[PipelineVersion]:
        return self._registry.list_versions(name)

    # 运行 ---------------------------------------------------------------
    def trigger(self, pipeline_name: str, idempotency_key: str) -> Run:
        version = self._registry.latest(pipeline_name)
        return self._orchestrator.trigger(version, idempotency_key)

    def resume(self, source_run_id: str, idempotency_key: str) -> Run:
        """定点续跑：沿用源运行成功结果，按源运行当时的版本只重跑未成功部分。"""
        return self._orchestrator.resume(source_run_id, idempotency_key)

    def cancel(self, run_id: str) -> Run:
        return self._orchestrator.cancel(run_id)

    def get_run(self, run_id: str) -> Run:
        return self._orchestrator.get_run(run_id)

    # 血缘与复现 ---------------------------------------------------------
    def lineage_of_run(self, run_id: str) -> list[LineageRecord]:
        return self._lineage.lineage_of_run(run_id)

    def reproduce(self, run_id: str) -> dict[str, Any]:
        return self._lineage.reproduce(self.get_run(run_id))

    # 处理器 -------------------------------------------------------------
    def register_handler(self, name: str, handler: StepHandler) -> None:
        self._handlers.register(name, handler)
