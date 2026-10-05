"""本地 JSON 存储：管道版本、运行记录、血缘记录的持久化与并发锁。

一致性约定：
- 所有写操作先写临时文件再原子 rename，避免半截文件。
- 进程内用 RLock 串行化读写，保证"检查-写入"复合操作的原子性
  （如 executor 的"查活动运行 -> 创建运行"）。
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .models import (
    LineageRecord,
    PipelineVersion,
    RunRecord,
    RunStatus,
    SkipReason,
    StepDef,
    StepRunRecord,
    StepStatus,
)


def _atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _read_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


# ---------- 序列化 ----------

def _step_def_to_dict(s: StepDef) -> dict:
    return {"name": s.name, "depends_on": list(s.depends_on),
            "handler": s.handler, "config": s.config}


def _step_def_from_dict(d: dict) -> StepDef:
    return StepDef(name=d["name"], depends_on=tuple(d.get("depends_on", ())),
                   handler=d.get("handler", "noop"), config=d.get("config", {}))


def _version_to_dict(v: PipelineVersion) -> dict:
    return {"pipeline_id": v.pipeline_id, "version": v.version,
            "steps": [_step_def_to_dict(s) for s in v.steps],
            "created_at": v.created_at, "comment": v.comment}


def _version_from_dict(d: dict) -> PipelineVersion:
    return PipelineVersion(pipeline_id=d["pipeline_id"], version=d["version"],
                           steps=tuple(_step_def_from_dict(s) for s in d["steps"]),
                           created_at=d["created_at"], comment=d.get("comment", ""))


def _step_run_to_dict(sr: StepRunRecord) -> dict:
    d = asdict(sr)
    d["status"] = sr.status.value
    d["skip_reason"] = sr.skip_reason.value if sr.skip_reason else None
    return d


def _step_run_from_dict(d: dict) -> StepRunRecord:
    return StepRunRecord(
        step_name=d["step_name"], status=StepStatus(d["status"]),
        skip_reason=SkipReason(d["skip_reason"]) if d.get("skip_reason") else None,
        started_at=d.get("started_at"), finished_at=d.get("finished_at"),
        error=d.get("error"), outputs=d.get("outputs", {}))


def _run_to_dict(r: RunRecord) -> dict:
    return {"run_id": r.run_id, "pipeline_id": r.pipeline_id,
            "pipeline_version": r.pipeline_version,
            "idempotency_key": r.idempotency_key, "status": r.status.value,
            "step_runs": {k: _step_run_to_dict(v) for k, v in r.step_runs.items()},
            "inputs": r.inputs, "created_at": r.created_at,
            "started_at": r.started_at, "finished_at": r.finished_at}


def _run_from_dict(d: dict) -> RunRecord:
    return RunRecord(
        run_id=d["run_id"], pipeline_id=d["pipeline_id"],
        pipeline_version=d["pipeline_version"],
        idempotency_key=d.get("idempotency_key"), status=RunStatus(d["status"]),
        step_runs={k: _step_run_from_dict(v) for k, v in d.get("step_runs", {}).items()},
        inputs=d.get("inputs", {}), created_at=d.get("created_at", ""),
        started_at=d.get("started_at"), finished_at=d.get("finished_at"))


def _lineage_to_dict(rec: LineageRecord) -> dict:
    return asdict(rec)


def _lineage_from_dict(d: dict) -> LineageRecord:
    return LineageRecord(**d)


# ---------- 存储 ----------

class Store:
    """基于本地 JSON 文件的存储，进程内用锁保证读写串行化。"""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._lock = threading.RLock()
        for sub in ("pipelines", "runs", "lineage"):
            (self._root / sub).mkdir(parents=True, exist_ok=True)

    # ---- 管道版本 ----

    def _version_path(self, pipeline_id: str, version: int) -> Path:
        return self._root / "pipelines" / pipeline_id / "versions" / f"{version}.json"

    def save_pipeline_version(self, version: PipelineVersion) -> None:
        with self._lock:
            path = self._version_path(version.pipeline_id, version.version)
            if path.exists():
                from .errors import InvalidStateError
                raise InvalidStateError(
                    f"管道版本不可变: {version.pipeline_id}@v{version.version} 已存在")
            _atomic_write_json(path, _version_to_dict(version))

    def get_pipeline_version(self, pipeline_id: str, version: int) -> PipelineVersion | None:
        with self._lock:
            d = _read_json(self._version_path(pipeline_id, version))
            return _version_from_dict(d) if d else None

    def list_versions(self, pipeline_id: str) -> list[PipelineVersion]:
        with self._lock:
            vdir = self._root / "pipelines" / pipeline_id / "versions"
            if not vdir.exists():
                return []
            out = []
            for p in sorted(vdir.glob("*.json"), key=lambda p: int(p.stem)):
                out.append(_version_from_dict(_read_json(p)))
            return out

    def latest_version(self, pipeline_id: str) -> PipelineVersion | None:
        versions = self.list_versions(pipeline_id)
        return versions[-1] if versions else None

    # ---- 运行记录 ----

    def _run_path(self, run_id: str) -> Path:
        return self._root / "runs" / f"{run_id}.json"

    def save_run(self, run: RunRecord) -> None:
        with self._lock:
            _atomic_write_json(self._run_path(run.run_id), _run_to_dict(run))

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._lock:
            d = _read_json(self._run_path(run_id))
            return _run_from_dict(d) if d else None

    def list_runs(self, pipeline_id: str) -> list[RunRecord]:
        with self._lock:
            out = []
            for p in sorted((self._root / "runs").glob("*.json")):
                run = _run_from_dict(_read_json(p))
                if run.pipeline_id == pipeline_id:
                    out.append(run)
            out.sort(key=lambda r: r.created_at)
            return out

    def find_run_by_idempotency_key(self, pipeline_id: str, key: str) -> RunRecord | None:
        with self._lock:
            for run in self.list_runs(pipeline_id):
                if run.idempotency_key == key:
                    return run
            return None

    def active_run_of(self, pipeline_id: str) -> RunRecord | None:
        with self._lock:
            for run in self.list_runs(pipeline_id):
                if run.status in (RunStatus.PENDING, RunStatus.RUNNING):
                    return run
            return None

    # ---- 血缘 ----

    def _lineage_path(self, run_id: str) -> Path:
        return self._root / "lineage" / f"{run_id}.jsonl"

    def append_lineage(self, record: LineageRecord) -> None:
        with self._lock:
            path = self._lineage_path(record.run_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(_lineage_to_dict(record), ensure_ascii=False) + "\n")

    def lineage_of_run(self, run_id: str) -> list[LineageRecord]:
        with self._lock:
            path = self._lineage_path(run_id)
            if not path.exists():
                return []
            return [_lineage_from_dict(json.loads(line))
                    for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
