"""业务域：取消一致性。

运行可被取消并停在一致状态：已完成步骤保留，未开始步骤标记为
"取消跳过"，不存在执行到一半就生效的中间态；取消后管道可再次触发。
"""

from __future__ import annotations

import threading

import pytest

from pipeline.errors import NotFoundError
from pipeline.models import (PipelineDefinition, RunStatus, StepDefinition,
                             StepStatus)

EFFECTS: list[str] = []


@pytest.fixture()
def cancel_service(service):
    entered = threading.Event()
    release = threading.Event()
    service._entered, service._release = entered, release

    def first(config, upstream):
        EFFECTS.append("first")
        return {"done": True}

    def blocking(config, upstream):
        entered.set()
        release.wait(timeout=5)
        EFFECTS.append("second")
        return {}

    def never(config, upstream):
        EFFECTS.append("third")
        return {}

    service.register_handler("first", first)
    service.register_handler("blocking", blocking)
    service.register_handler("never", never)
    service.register_pipeline(PipelineDefinition("cancellable", [
        StepDefinition("s1", "first"),
        StepDefinition("s2", "blocking", ["s1"]),
        StepDefinition("s3", "never", ["s2"]),
    ]))
    EFFECTS.clear()
    return service


class TestCancellationConsistency:
    def test_cancel_stops_at_consistent_state(self, cancel_service):
        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(cancel_service.trigger("cancellable", "ck")))
        t.start()
        assert cancel_service._entered.wait(timeout=5)

        cancelled = cancel_service.cancel(holder_run_id(cancel_service, holder, t))
        cancel_service._release.set()
        t.join(timeout=5)

        final = cancel_service.get_run(cancelled.run_id)
        assert final.status == RunStatus.CANCELLED
        # 已完成步骤保留成果；未开始步骤以"取消跳过"拒绝，原因可区分
        assert final.step_runs["s1"].status == StepStatus.SUCCEEDED
        assert final.step_runs["s3"].status == StepStatus.SKIPPED_CANCELLED
        assert final.step_runs["s3"].status != StepStatus.SKIPPED_UPSTREAM_FAILED
        # 未开始步骤零副作用
        assert "third" not in EFFECTS

    def test_cancelled_run_leaves_no_active_run(self, cancel_service):
        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(cancel_service.trigger("cancellable", "ck2")))
        t.start()
        assert cancel_service._entered.wait(timeout=5)
        cancel_service.cancel(holder_run_id(cancel_service, holder, t))
        cancel_service._release.set()
        t.join(timeout=5)
        # 取消后管道立即可再次触发，不会卡在"已有活动运行"
        again = cancel_service.trigger("cancellable", "ck3")
        assert again.status == RunStatus.SUCCEEDED

    def test_cancel_missing_run_rejected(self, cancel_service):
        with pytest.raises(NotFoundError):
            cancel_service.cancel("no-such-run")


def holder_run_id(service, holder, thread):
    """在执行线程尚未返回时，从存储里找到活动运行的 id。"""
    import time
    for _ in range(100):
        rows = service._store.find("runs", pipeline_name="cancellable")
        if rows:
            return rows[0]["run_id"]
        time.sleep(0.01)
    raise AssertionError("未找到活动运行")
