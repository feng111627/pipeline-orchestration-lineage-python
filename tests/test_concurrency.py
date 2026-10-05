"""业务域：并发与重复提交控制 —— 单活动运行、幂等去重、非法流转拒绝。"""
from __future__ import annotations

import threading

import pytest

from pipeline.errors import ConflictError, InvalidStateError
from pipeline.models import RunStatus, StepDef


def _slow_pipeline(registry, executor, entered: threading.Event, release: threading.Event, calls: list):
    """注册一个可阻塞的步骤处理器，便于构造并发窗口。"""
    def slow(config, inputs, upstream):
        calls.append("slow")
        entered.set()
        assert release.wait(timeout=5), "测试未释放阻塞步骤"
        return {"done": True}
    executor.register_handler("slow", slow)
    registry.create_version("conc", [StepDef("s", handler="slow")])


def test_concurrent_triggers_allow_only_one_active_run(registry, executor):
    """两个线程同时触发同一管道：一个成功，另一个必须得到 ConflictError。"""
    entered, release = threading.Event(), threading.Event()
    calls: list = []
    _slow_pipeline(registry, executor, entered, release, calls)

    results, errors = [], []

    def do_trigger():
        try:
            results.append(executor.trigger("conc"))
        except ConflictError as exc:
            errors.append(exc)

    t1 = threading.Thread(target=do_trigger)
    t1.start()
    assert entered.wait(timeout=5)          # 确保第一个运行已进入执行
    t2 = threading.Thread(target=do_trigger)
    t2.start()
    t2.join(timeout=5)
    release.set()
    t1.join(timeout=5)

    assert len(results) == 1 and len(errors) == 1
    assert results[0].status == RunStatus.SUCCEEDED
    assert calls == ["slow"]                # 慢步骤只执行了一次，无重复副作用


def test_idempotent_resubmit_returns_same_run_without_duplicate_effects(registry, executor):
    """相同幂等键重复提交：返回同一运行，处理器只执行一次。"""
    calls: list = []
    executor.register_handler("count", lambda c, i, u: calls.append(1) or {})
    registry.create_version("idem", [StepDef("s", handler="count")])

    first = executor.trigger("idem", idempotency_key="order-42")
    second = executor.trigger("idem", idempotency_key="order-42")
    third = executor.trigger("idem", inputs={"different": True}, idempotency_key="order-42")

    assert first.run_id == second.run_id == third.run_id
    assert len(calls) == 1
    # 重复提交不得覆盖首次运行的输入
    assert third.inputs == first.inputs


def test_trigger_after_terminal_run_is_allowed(registry, executor):
    """运行到达终态后，同一管道可以再次触发（活动运行约束只针对活动期）。"""
    registry.create_version("seq", [StepDef("s")])
    r1 = executor.trigger("seq")
    r2 = executor.trigger("seq")
    assert r1.run_id != r2.run_id
    assert r2.status == RunStatus.SUCCEEDED


def test_cancel_on_terminal_run_is_rejected(registry, executor):
    """对终态运行取消是非法流转，必须明确拒绝而非静默成功。"""
    registry.create_version("term", [StepDef("s")])
    run = executor.trigger("term")
    assert run.status == RunStatus.SUCCEEDED
    with pytest.raises(InvalidStateError):
        executor.cancel(run.run_id)
    # 状态未被静默覆盖
    assert executor.get_run(run.run_id).status == RunStatus.SUCCEEDED


def test_double_cancel_does_not_corrupt_state(registry, executor):
    """运行取消到达终态后，再次取消同样被拒绝。"""
    entered, release = threading.Event(), threading.Event()
    calls: list = []
    _slow_pipeline(registry, executor, entered, release, calls)
    # 追加一个依赖慢步骤的后续步骤，保证取消时仍有未调度步骤
    registry.create_version("conc", [
        StepDef("s", handler="slow"), StepDef("after", ("s",), handler="noop"),
    ])
    results = []
    t = threading.Thread(target=lambda: results.append(executor.trigger("conc")))
    t.start()
    assert entered.wait(timeout=5)
    executor.cancel(_active_run_id(executor))
    release.set()
    t.join(timeout=5)
    run = results[0]
    assert run.status == RunStatus.CANCELLED
    assert run.step_runs["after"].status.value == "CANCELLED"
    with pytest.raises(InvalidStateError):
        executor.cancel(run.run_id)


def _active_run_id(executor) -> str:
    run = executor._store.active_run_of("conc")
    assert run is not None
    return run.run_id
