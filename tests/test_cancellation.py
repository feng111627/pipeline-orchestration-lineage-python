"""业务域：运行取消一致性 —— 取消后停在一致状态，无半截生效的中间态。"""
from __future__ import annotations

import threading

from pipeline.models import RunStatus, StepDef, StepStatus


def _blocking_pipeline(registry, executor, entered, release):
    """两步管道：first 阻塞可控，second 依赖 first。"""
    def slow(config, inputs, upstream):
        entered.set()
        assert release.wait(timeout=5)
        return {"first": "done"}
    executor.register_handler("slow", slow)
    executor.register_handler("ok", lambda c, i, u: {"ok": True})
    registry.create_version("canc", [
        StepDef("first", handler="slow"), StepDef("second", ("first",), handler="ok"),
    ])


def _trigger_async(executor, results):
    t = threading.Thread(target=lambda: results.append(executor.trigger("canc")))
    t.start()
    return t


def test_cancel_mid_run_stops_at_consistent_state(registry, executor):
    """执行中取消：运行中步骤走完整终态，未开始步骤标记 CANCELLED，运行终态 CANCELLED。"""
    entered, release = threading.Event(), threading.Event()
    _blocking_pipeline(registry, executor, entered, release)
    results: list = []
    t = _trigger_async(executor, results)
    assert entered.wait(timeout=5)

    executor.cancel(_active_id(executor))
    release.set()
    t.join(timeout=5)

    run = results[0]
    assert run.status == RunStatus.CANCELLED
    # 正在执行的步骤完整生效（输出一次性写入），不是半截状态
    assert run.step_runs["first"].status == StepStatus.SUCCEEDED
    assert run.step_runs["first"].outputs == {"first": "done"}
    # 未开始的步骤被明确标记为取消，且其处理器从未执行
    assert run.step_runs["second"].status == StepStatus.CANCELLED
    assert run.step_runs["second"].outputs == {}
    assert run.finished_at is not None


def test_cancelled_step_handler_never_runs(registry, executor):
    """取消后，未开始步骤的处理器绝不被调用（无副作用泄漏）。"""
    entered, release = threading.Event(), threading.Event()
    calls: list = []
    _blocking_pipeline(registry, executor, entered, release)
    executor.register_handler("ok", lambda c, i, u: calls.append("second") or {})
    results: list = []
    t = _trigger_async(executor, results)
    assert entered.wait(timeout=5)
    executor.cancel(_active_id(executor))
    release.set()
    t.join(timeout=5)

    assert calls == []
    assert results[0].status == RunStatus.CANCELLED


def test_cancel_before_start_cancels_all_steps(registry, executor):
    """对尚未进入执行的 PENDING 运行取消：所有步骤直接标记 CANCELLED。"""
    registry.create_version("pre", [StepDef("a"), StepDef("b", ("a",))])
    store = executor._store
    # 直接构造一个 PENDING 运行（绕过执行循环），模拟触发与执行之间的窗口
    from pipeline.models import RunRecord, StepRunRecord
    from datetime import datetime, timezone
    run = RunRecord(run_id="pending-1", pipeline_id="pre", pipeline_version=1,
                    idempotency_key=None, created_at=datetime.now(timezone.utc).isoformat(),
                    step_runs={"a": StepRunRecord("a"), "b": StepRunRecord("b")})
    store.save_run(run)

    cancelled = executor.cancel("pending-1")

    assert cancelled.status == RunStatus.CANCELLED
    assert all(sr.status == StepStatus.CANCELLED for sr in cancelled.step_runs.values())


def test_new_trigger_allowed_after_cancellation(registry, executor):
    """取消到达终态后，管道可再次触发，且不受已取消运行影响。"""
    entered, release = threading.Event(), threading.Event()
    _blocking_pipeline(registry, executor, entered, release)
    results: list = []
    t = _trigger_async(executor, results)
    assert entered.wait(timeout=5)
    executor.cancel(_active_id(executor))
    release.set()
    t.join(timeout=5)

    second = executor.trigger("canc")

    assert second.run_id != results[0].run_id
    assert second.status == RunStatus.SUCCEEDED


def _active_id(executor) -> str:
    run = executor._store.active_run_of("canc")
    assert run is not None
    return run.run_id
