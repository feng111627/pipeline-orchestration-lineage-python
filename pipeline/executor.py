"""编排执行器：按依赖顺序推进、失败传播、取消与并发/幂等控制。

一致性规则：
- 同一管道同一时刻只允许一个活动运行（PENDING/RUNNING）。
- 相同幂等键的重复提交返回既有运行，不产生第二次执行。
- 步骤的副作用（输出 + 血缘）只在步骤成功后、同一把锁内一次性生效，
  失败步骤零副作用，不存在"执行到一半已生效"的中间态。
- 所有状态流转经过校验，非法流转抛 InvalidTransitionError。
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Callable

from .errors import ConflictError, InvalidTransitionError, NotFoundError
from .lineage import LineageService
from .models import (Run, RunStatus, StepRun, StepStatus, PipelineVersion,
                     TERMINAL_RUN_STATUSES, run_from_dict, run_to_dict)
from .registry import topo_order
from .store import JsonStore

StepHandler = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]
"""handler(step_config, upstream_outputs) -> outputs"""

_RUNS = "runs"

_ALLOWED_RUN_TRANSITIONS = {
    RunStatus.PENDING: {RunStatus.RUNNING, RunStatus.CANCELLED},
    RunStatus.RUNNING: {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED},
}
_ALLOWED_STEP_TRANSITIONS = {
    StepStatus.PENDING: {StepStatus.RUNNING, StepStatus.SKIPPED_UPSTREAM_FAILED,
                         StepStatus.SKIPPED_CANCELLED},
    StepStatus.RUNNING: {StepStatus.SUCCEEDED, StepStatus.FAILED},
}


def _check_transition(table: dict, current, target, what: str) -> None:
    if target not in table.get(current, set()):
        raise InvalidTransitionError(
            f"{what} 不允许从 {current.value} 流转到 {target.value}")


class HandlerRegistry:
    """步骤处理器注册表（本地模拟真实计算）。"""

    def __init__(self) -> None:
        self._handlers: dict[str, StepHandler] = {}

    def register(self, name: str, handler: StepHandler) -> None:
        self._handlers[name] = handler

    def get(self, name: str) -> StepHandler:
        try:
            return self._handlers[name]
        except KeyError:
            raise NotFoundError(f"未注册的 handler: {name}") from None


class Orchestrator:
    def __init__(self, store: JsonStore, handlers: HandlerRegistry,
                 lineage: LineageService | None = None):
        self._store = store
        self._handlers = handlers
        self._lineage = lineage
        self._cancel_events: dict[str, threading.Event] = {}
        self._events_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 触发与取消
    # ------------------------------------------------------------------

    def trigger(self, version: PipelineVersion, idempotency_key: str) -> Run:
        """触发一次运行（同步执行完毕并返回终态运行）。

        - 幂等键命中既有运行：直接返回该运行（重复提交无副作用）。
        - 存在活动运行：抛 ConflictError。
        """
        with self._store.lock:
            dup = self._store.find_one(_RUNS, pipeline_name=version.pipeline_name,
                                       idempotency_key=idempotency_key)
            if dup is not None:
                return run_from_dict(dup)  # 幂等命中：不重复执行
            for row in self._store.find(_RUNS, pipeline_name=version.pipeline_name):
                if RunStatus(row["status"]) not in TERMINAL_RUN_STATUSES:
                    raise ConflictError(
                        f"管道 {version.pipeline_name} 已有活动运行 {row['run_id']}")
            run = Run(
                run_id=uuid.uuid4().hex,
                pipeline_name=version.pipeline_name,
                pipeline_version=version.version,
                idempotency_key=idempotency_key,
                definition_snapshot=version.definition,
                created_at=time.time(),
                step_runs={s.name: StepRun(step_name=s.name)
                           for s in version.definition.steps},
            )
            self._store.append(_RUNS, run_to_dict(run))
        self._set_cancel_event(run.run_id, threading.Event())
        try:
            self._transition_run(run, RunStatus.RUNNING)
            self._execute(run)
        finally:
            self._set_cancel_event(run.run_id, None)
        return self.get_run(run.run_id)

    def cancel(self, run_id: str) -> Run:
        """取消活动运行；已结束或不存在的运行抛错。"""
        run = self.get_run(run_id)
        if run.status in TERMINAL_RUN_STATUSES:
            raise InvalidTransitionError(
                f"运行 {run_id} 已处于终态 {run.status.value}，不能取消")
        event = self._get_cancel_event(run_id)
        if event is not None:
            event.set()  # 执行循环在步骤边界观察到后停在一致状态
        else:
            # 活动但当前没有执行循环（如 PENDING 未调度）：直接一致地终态化
            with self._store.lock:
                run = self.get_run(run_id)
                if run.status in TERMINAL_RUN_STATUSES:
                    raise InvalidTransitionError(
                        f"运行 {run_id} 已处于终态 {run.status.value}，不能取消")
                for sr in run.step_runs.values():
                    if sr.status == StepStatus.PENDING:
                        self._transition_step(sr, StepStatus.SKIPPED_CANCELLED)
                self._transition_run(run, RunStatus.CANCELLED)
                self._persist(run)
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> Run:
        row = self._store.find_one(_RUNS, run_id=run_id)
        if row is None:
            raise NotFoundError(f"运行 {run_id} 不存在")
        return run_from_dict(row)

    # ------------------------------------------------------------------
    # 执行循环
    # ------------------------------------------------------------------

    def _execute(self, run: Run) -> None:
        definition = run.definition_snapshot  # 用快照推进，与当前定义无关
        order = topo_order(definition)
        configs = {s.name: dict(s.config) for s in definition.steps}
        deps = {s.name: list(s.depends_on) for s in definition.steps}
        handlers = {s.name: s.handler for s in definition.steps}

        for name in order:
            sr = run.step_runs[name]
            if self._cancel_requested(run.run_id):
                self._skip_remaining(run, order, name,
                                     StepStatus.SKIPPED_CANCELLED)
                self._transition_run(run, RunStatus.CANCELLED)
                self._persist(run)
                return
            upstream = [run.step_runs[d] for d in deps[name]]
            if any(u.status != StepStatus.SUCCEEDED for u in upstream):
                # 失败传播：给出可区分的拒绝原因
                self._transition_step(sr, StepStatus.SKIPPED_UPSTREAM_FAILED)
                sr.error = "上游步骤未成功: " + ",".join(
                    f"{u.step_name}={u.status.value}" for u in upstream
                    if u.status != StepStatus.SUCCEEDED)
                self._persist(run)
                continue
            inputs = {u.step_name: dict(u.outputs) for u in upstream}
            self._run_step(run, sr, handlers[name], configs[name], inputs)

        if any(sr.status == StepStatus.FAILED for sr in run.step_runs.values()):
            self._transition_run(run, RunStatus.FAILED)
        else:
            self._transition_run(run, RunStatus.SUCCEEDED)
        self._persist(run)

    def _run_step(self, run: Run, sr: StepRun, handler_name: str,
                  config: dict[str, Any], inputs: dict[str, Any]) -> None:
        sr.started_at = time.time()
        self._transition_step(sr, StepStatus.RUNNING)
        self._persist(run)
        try:
            handler = self._handlers.get(handler_name)
            outputs = handler(config, inputs)  # 计算阶段：不触碰存储
        except Exception as exc:  # 失败：零副作用，仅记录失败终态
            sr.finished_at = time.time()
            sr.error = f"{type(exc).__name__}: {exc}"
            self._transition_step(sr, StepStatus.FAILED)
            self._persist(run)
            return
        # 生效阶段：输出、步骤终态、血缘在同一把锁内一次性落盘
        with self._store.lock:
            sr.finished_at = time.time()
            sr.outputs = dict(outputs or {})
            self._transition_step(sr, StepStatus.SUCCEEDED)
            self._persist(run)
            if self._lineage is not None:
                self._lineage.record_step(run, sr.step_name, inputs,
                                          sr.outputs, config)

    def _skip_remaining(self, run: Run, order: list[str], from_step: str,
                        status: StepStatus) -> None:
        idx = order.index(from_step)
        for name in order[idx:]:
            sr = run.step_runs[name]
            if sr.status == StepStatus.PENDING:
                self._transition_step(sr, status)
                sr.error = "运行已取消" if status == StepStatus.SKIPPED_CANCELLED \
                    else "上游未成功"

    # ------------------------------------------------------------------
    # 状态流转与持久化
    # ------------------------------------------------------------------

    def _transition_run(self, run: Run, target: RunStatus) -> None:
        _check_transition(_ALLOWED_RUN_TRANSITIONS, run.status, target, "运行")
        run.status = target
        now = time.time()
        if target == RunStatus.RUNNING:
            run.started_at = now
        if target in TERMINAL_RUN_STATUSES:
            run.finished_at = now

    def _transition_step(self, sr: StepRun, target: StepStatus) -> None:
        _check_transition(_ALLOWED_STEP_TRANSITIONS, sr.status, target, "步骤")
        sr.status = target

    def _persist(self, run: Run) -> None:
        with self._store.lock:
            self._store.update_one(_RUNS, {"run_id": run.run_id},
                                   run_to_dict(run))

    # ------------------------------------------------------------------
    # 取消事件
    # ------------------------------------------------------------------

    def _set_cancel_event(self, run_id: str, event: threading.Event | None) -> None:
        with self._events_lock:
            if event is None:
                self._cancel_events.pop(run_id, None)
            else:
                self._cancel_events[run_id] = event

    def _get_cancel_event(self, run_id: str) -> threading.Event | None:
        with self._events_lock:
            return self._cancel_events.get(run_id)

    def _cancel_requested(self, run_id: str) -> bool:
        event = self._get_cancel_event(run_id)
        return event is not None and event.is_set()
