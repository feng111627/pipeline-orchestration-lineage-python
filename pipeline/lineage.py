"""血缘审计：查询运行血缘、按历史版本复现运行结论。

复现是只读的：按运行记录中锚定的 pipeline_version 取回当时的定义，
与运行记录、血缘记录对照，重建当时的结论。由于历史版本不可变，
配置变更、步骤改名、回滚都不会影响已完成运行的可复现性。
"""
from __future__ import annotations

from typing import Any

from .errors import NotFoundError
from .models import LineageRecord, RunRecord
from .store import Store


class LineageService:
    def __init__(self, store: Store) -> None:
        self._store = store

    def lineage_of_run(self, run_id: str) -> list[LineageRecord]:
        if self._store.get_run(run_id) is None:
            raise NotFoundError(f"运行不存在: {run_id}")
        return self._store.lineage_of_run(run_id)

    def replay(self, run_id: str) -> dict[str, Any]:
        """按运行锚定的管道版本与输入复现结论（只读，不产生副作用）。

        返回复现报告：锚定版本、当时的步骤定义、每步结论与血缘。
        若运行引用的版本与步骤定义无法对应（理论上不应发生），明确报错。
        """
        run = self._store.get_run(run_id)
        if run is None:
            raise NotFoundError(f"运行不存在: {run_id}")
        version = self._store.get_pipeline_version(run.pipeline_id, run.pipeline_version)
        if version is None:
            raise NotFoundError(
                f"运行 {run_id} 锚定的版本 {run.pipeline_id}@v{run.pipeline_version} 缺失，无法复现")
        step_defs = {s.name: s for s in version.steps}
        missing = [n for n in run.step_runs if n not in step_defs]
        if missing:
            raise NotFoundError(
                f"运行 {run_id} 的步骤 {missing} 在锚定版本 v{version.version} 中不存在，无法复现")
        lineage = {rec.step_name: rec for rec in self._store.lineage_of_run(run_id)}
        steps = []
        for name, sr in run.step_runs.items():
            steps.append({
                "step_name": name,
                "handler": step_defs[name].handler,
                "config": step_defs[name].config,   # 当时的配置版本
                "status": sr.status.value,
                "skip_reason": sr.skip_reason.value if sr.skip_reason else None,
                "error": sr.error,
                "outputs": sr.outputs,
                "lineage": lineage.get(name),
            })
        return {
            "run_id": run.run_id,
            "pipeline_id": run.pipeline_id,
            "pipeline_version": run.pipeline_version,
            "status": run.status.value,
            "inputs": run.inputs,
            "steps": steps,
        }
