"""血缘记录与历史复现。

每个成功步骤写一条血缘记录（输入=上游输出，输出，步骤配置，配置版本）。
复现历史运行时以运行内嵌的定义快照为准，与当前定义无关，
因此配置变更、步骤改名、版本回滚都不影响历史结论的可复现性。
"""

from __future__ import annotations

import time
from typing import Any

from .errors import NotFoundError
from .models import (LineageRecord, Run, StepStatus, lineage_from_dict,
                     lineage_to_dict)
from .store import JsonStore

_COLLECTION = "lineage"


class LineageService:
    def __init__(self, store: JsonStore):
        self._store = store

    def record_step(self, run: Run, step_name: str,
                    inputs: dict[str, Any], outputs: dict[str, Any],
                    step_config: dict[str, Any]) -> LineageRecord:
        record = LineageRecord(
            run_id=run.run_id,
            pipeline_name=run.pipeline_name,
            pipeline_version=run.pipeline_version,
            step_name=step_name,
            inputs=dict(inputs),
            outputs=dict(outputs),
            step_config=dict(step_config),
            recorded_at=time.time(),
        )
        self._store.append(_COLLECTION, lineage_to_dict(record))
        return record

    def lineage_of_run(self, run_id: str) -> list[LineageRecord]:
        rows = self._store.find(_COLLECTION, run_id=run_id)
        return [lineage_from_dict(r) for r in rows]

    def reproduce(self, run: Run) -> dict[str, Any]:
        """复现一次历史运行的结论：状态、每步结果与血缘，全部来自快照。

        返回结构：
        - run: 运行级结论（管道、版本、终态、时间）
        - steps: 按快照定义顺序给出每步状态/错误/输出
        - lineage: 每个成功步骤的输入/输出/配置版本
        """
        lineage = {l.step_name: l for l in self.lineage_of_run(run.run_id)}
        snapshot = run.definition_snapshot
        steps = []
        for step_def in snapshot.steps:
            sr = run.step_runs.get(step_def.name)
            if sr is None:
                raise NotFoundError(
                    f"运行 {run.run_id} 缺少步骤 {step_def.name} 的记录")
            entry: dict[str, Any] = {
                "step_name": sr.step_name,
                "handler": step_def.handler,
                "status": sr.status.value,
                "error": sr.error,
                "outputs": dict(sr.outputs),
            }
            rec = lineage.get(sr.step_name)
            if sr.status == StepStatus.SUCCEEDED:
                if rec is None:
                    raise NotFoundError(
                        f"运行 {run.run_id} 步骤 {sr.step_name} 缺少血缘记录")
                entry["lineage"] = {
                    "inputs": dict(rec.inputs),
                    "outputs": dict(rec.outputs),
                    "step_config": dict(rec.step_config),
                    "pipeline_version": rec.pipeline_version,
                }
            steps.append(entry)
        return {
            "run": {
                "run_id": run.run_id,
                "pipeline_name": run.pipeline_name,
                "pipeline_version": run.pipeline_version,
                "idempotency_key": run.idempotency_key,
                "status": run.status.value,
                "created_at": run.created_at,
                "started_at": run.started_at,
                "finished_at": run.finished_at,
            },
            "steps": steps,
        }
