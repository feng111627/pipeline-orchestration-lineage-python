"""业务域：并发与重复提交。

同一管道同一时刻只允许一个活动运行；相同幂等键的并发/重复提交
返回同一运行且处理器只执行一次，不产生重复副作用。
"""

from __future__ import annotations

import threading

import pytest

from pipeline.errors import ConflictError
from pipeline.models import (PipelineDefinition, RunStatus, StepDefinition)


@pytest.fixture()
def counter_service(service):
    service.register_pipeline(PipelineDefinition("counter", [
        StepDefinition("count", "count"),
    ]))
    return service


def _make_counting_handler():
    calls = {"n": 0}
    lock = threading.Lock()

    def handler(config, upstream):
        with lock:
            calls["n"] += 1
        return {"seq": calls["n"]}

    return handler, calls


class TestConcurrentDuplicateTrigger:
    def test_same_key_concurrent_triggers_execute_once(self, counter_service):
        handler, calls = _make_counting_handler()
        counter_service.register_handler("count", handler)
        results, errors = [], []

        def work():
            try:
                results.append(counter_service.trigger("counter", "same-key"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=work) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert {r.run_id for r in results} and len({r.run_id for r in results}) == 1
        assert calls["n"] == 1  # 处理器只执行一次：无重复副作用

    def test_same_key_resubmit_after_finish_returns_same_run(self, counter_service):
        handler, calls = _make_counting_handler()
        counter_service.register_handler("count", handler)
        first = counter_service.trigger("counter", "k")
        second = counter_service.trigger("counter", "k")
        assert second.run_id == first.run_id
        assert second.status == RunStatus.SUCCEEDED
        assert calls["n"] == 1

    def test_active_run_rejects_new_trigger(self, service):
        entered = threading.Event()
        release = threading.Event()

        def blocking(config, upstream):
            entered.set()
            release.wait(timeout=5)
            return {}

        service.register_handler("block", blocking)
        service.register_pipeline(PipelineDefinition("slow", [
            StepDefinition("s", "block"),
        ]))
        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(service.trigger("slow", "key-a")))
        t.start()
        assert entered.wait(timeout=5)  # 运行已激活

        with pytest.raises(ConflictError):
            service.trigger("slow", "key-b")  # 不同键：并发触发被拒绝
        same = service.trigger("slow", "key-a")  # 同键：幂等返回活动运行本身
        assert same.run_id is not None

        release.set()
        t.join(timeout=5)
        assert holder[0].status == RunStatus.SUCCEEDED
        # 运行结束后可以再次触发
        again = service.trigger("slow", "key-c")
        assert again.status == RunStatus.SUCCEEDED
        assert again.run_id != holder[0].run_id
