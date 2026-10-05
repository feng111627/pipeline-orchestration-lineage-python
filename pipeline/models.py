"""核心数据模型：管道定义、运行记录、血缘记录的类型定义。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class StepStatus(str, enum.Enum):
    """步骤运行状态机。合法流转见 executor 中的校验。"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"      # 上游失败/取消导致的拒绝执行
    CANCELLED = "CANCELLED"


class SkipReason(str, enum.Enum):
    """步骤被拒绝执行的可区分原因。"""
    UPSTREAM_FAILED = "UPSTREAM_FAILED"
    UPSTREAM_SKIPPED = "UPSTREAM_SKIPPED"
    RUN_CANCELLED = "RUN_CANCELLED"


class RunStatus(str, enum.Enum):
    """运行状态机：PENDING -> RUNNING -> {SUCCEEDED, FAILED, CANCELLED}。"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_RUN_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED})
TERMINAL_STEP_STATUSES = frozenset({StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.SKIPPED, StepStatus.CANCELLED})


@dataclass(frozen=True)
class StepDef:
    """管道中的一个处理步骤定义。"""
    name: str
    depends_on: tuple[str, ...] = ()
    handler: str = "noop"          # 执行器名，由 executor 注册表解析
    config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineVersion:
    """管道定义的不可变版本快照。版本一经创建不得修改，保证历史可复现。"""
    pipeline_id: str
    version: int
    steps: tuple[StepDef, ...]
    created_at: str
    comment: str = ""


@dataclass
class StepRunRecord:
    """单个步骤在一次运行中的记录。"""
    step_name: str
    status: StepStatus = StepStatus.PENDING
    skip_reason: SkipReason | None = None
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    outputs: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunRecord:
    """一次运行的完整记录，含步骤快照与配置版本引用（血缘锚点）。"""
    run_id: str
    pipeline_id: str
    pipeline_version: int
    idempotency_key: str | None
    status: RunStatus = RunStatus.PENDING
    step_runs: dict[str, StepRunRecord] = field(default_factory=dict)
    inputs: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    started_at: str | None = None
    finished_at: str | None = None


@dataclass(frozen=True)
class LineageRecord:
    """血缘记录：某次运行中某步骤的输入/输出与配置版本。"""
    run_id: str
    step_name: str
    pipeline_id: str
    pipeline_version: int
    inputs: dict[str, Any]
    outputs: dict[str, Any]
    recorded_at: str
