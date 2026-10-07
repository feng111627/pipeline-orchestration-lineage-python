"""领域模型：管道定义、版本快照、运行记录与血缘记录。

所有模型都是纯数据（dataclass），可序列化为 JSON 存入本地存储。
运行记录中内嵌定义快照，保证历史运行在定义变更/改名后仍可复现。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class StepStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED_UPSTREAM_FAILED = "SKIPPED_UPSTREAM_FAILED"  # 直接上游失败，被直接拖累
    SKIPPED_TRANSITIVE = "SKIPPED_TRANSITIVE"            # 更上游失败，间接被连累
    SKIPPED_CANCELLED = "SKIPPED_CANCELLED"              # 运行取消导致的拒绝执行


class RunStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_RUN_STATUSES = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}
)


@dataclass
class StepDefinition:
    name: str
    handler: str                       # 处理器注册表中的 key
    depends_on: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineDefinition:
    name: str
    steps: list[StepDefinition] = field(default_factory=list)


@dataclass
class PipelineVersion:
    """管道定义的一次不可变版本快照。"""

    pipeline_name: str
    version: int
    definition: PipelineDefinition
    created_at: float
    note: str = ""                     # 例如 "rollback to v2"


@dataclass
class StepRun:
    step_name: str
    status: StepStatus = StepStatus.PENDING
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None
    outputs: dict[str, Any] = field(default_factory=dict)
    reused: bool = False  # 续跑中沿用历史成功结果，未重新执行


@dataclass
class Run:
    """一次运行。definition_snapshot 冻结了触发时刻的定义与配置版本。"""

    run_id: str
    pipeline_name: str
    pipeline_version: int
    idempotency_key: str
    definition_snapshot: PipelineDefinition
    status: RunStatus = RunStatus.PENDING
    step_runs: dict[str, StepRun] = field(default_factory=dict)
    created_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None
    # 续跑来源：非空表示本次运行从该历史运行接续；None 表示全新触发
    resumed_from_run_id: str | None = None


@dataclass
class LineageRecord:
    """单个步骤在一次运行中的血缘：输入、输出与配置版本。"""

    run_id: str
    pipeline_name: str
    pipeline_version: int
    step_name: str
    inputs: dict[str, Any] = field(default_factory=dict)   # 上游步骤输出
    outputs: dict[str, Any] = field(default_factory=dict)
    step_config: dict[str, Any] = field(default_factory=dict)
    recorded_at: float = 0.0
    # 沿用旧结果时指向来源运行；本次新计算则为 None
    reused_from_run_id: str | None = None


# ---------------------------------------------------------------------------
# JSON 序列化（本地存储的编解码层）
# ---------------------------------------------------------------------------

def step_definition_to_dict(s: StepDefinition) -> dict[str, Any]:
    return {"name": s.name, "handler": s.handler,
            "depends_on": list(s.depends_on), "config": dict(s.config)}


def step_definition_from_dict(d: dict[str, Any]) -> StepDefinition:
    return StepDefinition(name=d["name"], handler=d["handler"],
                          depends_on=list(d.get("depends_on", [])),
                          config=dict(d.get("config", {})))


def definition_to_dict(d: PipelineDefinition) -> dict[str, Any]:
    return {"name": d.name,
            "steps": [step_definition_to_dict(s) for s in d.steps]}


def definition_from_dict(d: dict[str, Any]) -> PipelineDefinition:
    return PipelineDefinition(name=d["name"],
                              steps=[step_definition_from_dict(s)
                                     for s in d.get("steps", [])])


def version_to_dict(v: PipelineVersion) -> dict[str, Any]:
    return {"pipeline_name": v.pipeline_name, "version": v.version,
            "definition": definition_to_dict(v.definition),
            "created_at": v.created_at, "note": v.note}


def version_from_dict(d: dict[str, Any]) -> PipelineVersion:
    return PipelineVersion(pipeline_name=d["pipeline_name"],
                           version=d["version"],
                           definition=definition_from_dict(d["definition"]),
                           created_at=d["created_at"], note=d.get("note", ""))


def step_run_to_dict(s: StepRun) -> dict[str, Any]:
    return {"step_name": s.step_name, "status": s.status.value,
            "started_at": s.started_at, "finished_at": s.finished_at,
            "error": s.error, "outputs": dict(s.outputs),
            "reused": s.reused}


def step_run_from_dict(d: dict[str, Any]) -> StepRun:
    return StepRun(step_name=d["step_name"], status=StepStatus(d["status"]),
                   started_at=d.get("started_at"),
                   finished_at=d.get("finished_at"),
                   error=d.get("error"), outputs=dict(d.get("outputs", {})),
                   reused=bool(d.get("reused", False)))


def run_to_dict(r: Run) -> dict[str, Any]:
    return {"run_id": r.run_id, "pipeline_name": r.pipeline_name,
            "pipeline_version": r.pipeline_version,
            "idempotency_key": r.idempotency_key,
            "definition_snapshot": definition_to_dict(r.definition_snapshot),
            "status": r.status.value,
            "step_runs": {k: step_run_to_dict(v) for k, v in r.step_runs.items()},
            "created_at": r.created_at, "started_at": r.started_at,
            "finished_at": r.finished_at,
            "resumed_from_run_id": r.resumed_from_run_id}


def run_from_dict(d: dict[str, Any]) -> Run:
    return Run(run_id=d["run_id"], pipeline_name=d["pipeline_name"],
               pipeline_version=d["pipeline_version"],
               idempotency_key=d["idempotency_key"],
               definition_snapshot=definition_from_dict(d["definition_snapshot"]),
               status=RunStatus(d["status"]),
               step_runs={k: step_run_from_dict(v)
                          for k, v in d.get("step_runs", {}).items()},
               created_at=d.get("created_at", 0.0),
               started_at=d.get("started_at"),
               finished_at=d.get("finished_at"),
               resumed_from_run_id=d.get("resumed_from_run_id"))


def lineage_to_dict(l: LineageRecord) -> dict[str, Any]:
    return {"run_id": l.run_id, "pipeline_name": l.pipeline_name,
            "pipeline_version": l.pipeline_version, "step_name": l.step_name,
            "inputs": dict(l.inputs), "outputs": dict(l.outputs),
            "step_config": dict(l.step_config), "recorded_at": l.recorded_at,
            "reused_from_run_id": l.reused_from_run_id}


def lineage_from_dict(d: dict[str, Any]) -> LineageRecord:
    return LineageRecord(run_id=d["run_id"], pipeline_name=d["pipeline_name"],
                         pipeline_version=d["pipeline_version"],
                         step_name=d["step_name"], inputs=dict(d.get("inputs", {})),
                         outputs=dict(d.get("outputs", {})),
                         step_config=dict(d.get("step_config", {})),
                         recorded_at=d.get("recorded_at", 0.0),
                         reused_from_run_id=d.get("reused_from_run_id"))
