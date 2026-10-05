"""编排执行器：依赖排序推进、失败传播、取消、并发与幂等控制。

一致性规则：
- 触发是"检查活动运行 -> 创建运行"的原子操作（_trigger_lock 保护），
  同一管道同一时刻只允许一个活动运行，并发触发得到 ConflictError。
- 幂等键已存在时直接返回已有运行，不产生重复执行或重复副作用。
- 步骤效果是原子的：handler 的返回值只在成功时一次性写入 outputs，
  不存在"执行到一半已生效"的中间态。
- 取消只阻止后续步骤调度；正在执行的步骤会走到成功或失败的终态，
  未开始的步骤标记 CANCELLED，运行停在一致的 CANCELLED 终态。
- 所有非法流转（如对终态运行取消）抛出 InvalidStateError，绝不静默覆盖。
"""
from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from .errors import ConflictError, InvalidStateError, NotFoundError
from .models import (
    LineageRecord,
    RunRecord,
    RunStatus,
    SkipReason,
    StepRunRecord,
    StepStatus,
    TERMINAL_RUN_STATUSES,
)
from .store import Store

# 步骤处理器签名：接收 (step_config, run_inputs, upstream_outputs)，返回输出 dict。
Handler = Callable[[dict, dict, dict], dict]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _noop_handler(config: dict, inputs: dict, upstream: dict) -> dict:
    return {}


class Executor:
    def __init__(self, store: Store) -> None:
        self._store = store
        self._handlers: dict[str, Handler] = {"noop": _noop_handler}
        self._trigger_lock = threading.Lock()
        self._cancel_flags: dict[str, threading.Event] = {}
        self._flags_lock = threading.Lock()

    def register_handler(self, name: str, handler: Handler) -> None:
        self._handlers[name] = handler

    # ---- 触发 ----

    def trigger(self, pipeline_id: str, inputs: dict[str, Any] | None = None,
                idempotency_key: str | None = None) -> RunRecord:
        """触发一次运行。同一管道同时只允许一个活动运行；幂等键去重。"""
        with self._trigger_lock:
            if idempotency_key is not None:
                existing = self._store.find_run_by_idempotency_key(pipeline_id, idempotency_key)
                if existing is not None:
                    return existing  # 重复提交：返回已有运行，不产生副作用
            active = self._store.active_run_of(pipeline_id)
            if active is not None:
                raise ConflictError(
                    f"管道 {pipeline_id} 已存在活动运行 {active.run_id}，拒绝并发触发")
            version = self._store.latest_version(pipeline_id)
            if version is None:
                raise NotFoundError(f"管道不存在: {pipeline_id}")
            run = RunRecord(
                run_id=uuid.uuid4().hex, pipeline_id=pipeline_id,
                pipeline_version=version.version, idempotency_key=idempotency_key,
                status=RunStatus.PENDING, inputs=dict(inputs or {}), created_at=_now(),
                step_runs={s.name: StepRunRecord(step_name=s.name) for s in version.steps})
            self._store.save_run(run)  # 先落盘，活动运行立即可见

        self._execute(run, version.steps)
        return self._store.get_run(run.run_id) or run

    # ---- 取消 ----

    def cancel(self, run_id: str) -> RunRecord:
        """取消运行：不再调度新步骤，未开始步骤标记 CANCELLED。"""
        run = self._store.get_run(run_id)
        if run is None:
            raise NotFoundError(f"运行不存在: {run_id}")
        if run.status in TERMINAL_RUN_STATUSES:
            raise InvalidStateError(f"运行 {run_id} 已处于终态 {run.status.value}，不能取消")
        with self._flags_lock:
            flag = self._cancel_flags.get(run_id)
            if flag is None:
                flag = self._cancel_flags[run_id] = threading.Event()
            flag.set()
        # 若运行尚未进入执行循环（PENDING），直接在此收口
        if run.status == RunStatus.PENDING:
            with self._trigger_lock:  # 与 trigger 的执行启动互斥
                run = self._store.get_run(run_id) or run
                if run.status == RunStatus.PENDING:
                    for sr in run.step_runs.values():
                        sr.status = StepStatus.CANCELLED
                        sr.finished_at = _now()
                    run.status = RunStatus.CANCELLED
                    run.finished_at = _now()
                    self._store.save_run(run)
        return self._store.get_run(run_id) or run

    def get_run(self, run_id: str) -> RunRecord:
        run = self._store.get_run(run_id)
        if run is None:
            raise NotFoundError(f"运行不存在: {run_id}")
        return run

    # ---- 执行循环 ----

    def _cancel_requested(self, run_id: str) -> bool:
        with self._flags_lock:
            flag = self._cancel_flags.get(run_id)
            return flag is not None and flag.is_set()

    def _execute(self, run: RunRecord, steps: tuple) -> None:
        run.status = RunStatus.RUNNING
        run.started_at = _now()
        self._store.save_run(run)
        deps = {s.name: s.depends_on for s in steps}
        handlers = {s.name: s.handler for s in steps}
        configs = {s.name: s.config for s in steps}

        try:
            while True:
                pending = [n for n, sr in run.step_runs.items() if sr.status == StepStatus.PENDING]
                if not pending:
                    break
                if self._cancel_requested(run.run_id):
                    for n in pending:
                        sr = run.step_runs[n]
                        sr.status = StepStatus.CANCELLED
                        sr.finished_at = _now()
                    run.status = RunStatus.CANCELLED
                    break
                statuses = {n: sr.status for n, sr in run.step_runs.items()}
                ready = [n for n in pending
                         if all(statuses[d] == StepStatus.SUCCEEDED for d in deps[n])]
                if not ready:
                    # 失败传播：剩余 pending 步骤被明确拒绝并给出可区分原因
                    for n in pending:
                        sr = run.step_runs[n]
                        sr.status = StepStatus.SKIPPED
                        sr.skip_reason = self._skip_reason(n, deps, statuses)
                        sr.finished_at = _now()
                    run.status = RunStatus.FAILED
                    break
                for n in ready:
                    if self._cancel_requested(run.run_id):
                        break  # 步骤间检查取消，留给下一轮统一收口
                    self._run_step(run, n, handlers[n], configs[n], deps[n])
                    statuses[n] = run.step_runs[n].status
        finally:
            if run.status not in TERMINAL_RUN_STATUSES:
                run.status = (RunStatus.FAILED if any(
                    sr.status in (StepStatus.FAILED, StepStatus.SKIPPED)
                    for sr in run.step_runs.values()) else RunStatus.SUCCEEDED)
            run.finished_at = _now()
            self._store.save_run(run)
            with self._flags_lock:
                self._cancel_flags.pop(run.run_id, None)

    def _skip_reason(self, name: str, deps: dict, statuses: dict) -> SkipReason:
        for d in deps[name]:
            if statuses[d] == StepStatus.FAILED:
                return SkipReason.UPSTREAM_FAILED
        for d in deps[name]:
            if statuses[d] == StepStatus.SKIPPED:
                return SkipReason.UPSTREAM_SKIPPED
        return SkipReason.UPSTREAM_SKIPPED

    def _run_step(self, run: RunRecord, name: str, handler_name: str,
                  config: dict, dep_names: tuple) -> None:
        sr = run.step_runs[name]
        handler = self._handlers.get(handler_name)
        if handler is None:
            sr.status = StepStatus.FAILED
            sr.error = f"未注册的处理器: {handler_name}"
            sr.finished_at = _now()
            self._store.save_run(run)
            return
        upstream_outputs = {d: run.step_runs[d].outputs for d in dep_names}
        sr.status = StepStatus.RUNNING
        sr.started_at = _now()
        self._store.save_run(run)
        try:
            outputs = handler(config, run.inputs, upstream_outputs) or {}
        except Exception as exc:  # 步骤失败：只标记该步骤，由主循环做失败传播
            sr.status = StepStatus.FAILED
            sr.error = f"{type(exc).__name__}: {exc}"
            sr.finished_at = _now()
            self._store.save_run(run)
            return
        # 成功才一次性写入输出 —— 步骤效果原子生效
        sr.outputs = dict(outputs)
        sr.status = StepStatus.SUCCEEDED
        sr.finished_at = _now()
        self._store.save_run(run)
        self._store.append_lineage(LineageRecord(
            run_id=run.run_id, step_name=name, pipeline_id=run.pipeline_id,
            pipeline_version=run.pipeline_version,
            inputs={"run_inputs": run.inputs, "upstream": upstream_outputs},
            outputs=sr.outputs, recorded_at=_now()))
