"""业务域：定点续跑与跨运行追溯。

从一次已结束且有未成功步骤的历史运行发起续跑：已成功的步骤沿用旧结果
（不重算、标注来源运行），只重跑失败及被连累的步骤；续跑是全新运行，
严格按源运行的版本快照执行，源运行记录不被修改。
"""

from __future__ import annotations

import threading

import pytest

from pipeline.errors import ConflictError, NotFoundError, ValidationError
from pipeline.models import (PipelineDefinition, RunStatus, StepDefinition,
                             StepStatus)

CALLS: list[str] = []  # handler 执行台账：验证"沿用的步骤不重算"


@pytest.fixture()
def dag_service(service):
    """a→b→c→d，b 可开关失败；每次 handler 执行都记入 CALLS。"""
    flag = {"fail_b": True}

    def ok(name):
        def handler(config, upstream):
            CALLS.append(name)
            return {"by": name}
        return handler

    def maybe_fail(config, upstream):
        CALLS.append("b")
        if flag["fail_b"]:
            raise RuntimeError("b 计算失败")
        return {"b_out": upstream["a"]["by"]}

    service.register_handler("ok_a", ok("a"))
    service.register_handler("maybe_fail_b", maybe_fail)
    service.register_handler("ok_c", ok("c"))
    service.register_handler("ok_d", ok("d"))
    service.register_pipeline(PipelineDefinition("resume-dag", [
        StepDefinition("a", "ok_a"),
        StepDefinition("b", "maybe_fail_b", ["a"]),
        StepDefinition("c", "ok_c", ["b"]),
        StepDefinition("d", "ok_d", ["c"]),
    ]))
    CALLS.clear()
    service._fail_flag = flag
    return service


class TestResumeFromFailure:
    def test_resume_reuses_succeeded_and_reruns_rest(self, dag_service):
        first = dag_service.trigger("resume-dag", "r1")
        assert first.status == RunStatus.FAILED
        assert CALLS == ["a", "b"]  # b 失败，c/d 未执行
        CALLS.clear()

        dag_service._fail_flag["fail_b"] = False
        resumed = dag_service.resume(first.run_id, "r1-resume")

        assert resumed.run_id != first.run_id  # 全新运行
        assert resumed.resumed_from_run_id == first.run_id  # 可追溯来源
        assert resumed.status == RunStatus.SUCCEEDED
        # 只重跑失败与受连累的步骤；已成功的 a 不重算
        assert CALLS == ["b", "c", "d"]
        # a 沿用旧结果：状态成功、输出一致、标注来源运行
        a = resumed.step_runs["a"]
        assert a.status == StepStatus.SUCCEEDED
        assert a.origin_run_id == first.run_id
        assert a.outputs == first.step_runs["a"].outputs
        # 重跑的步骤标注为本次计算
        assert resumed.step_runs["b"].origin_run_id is None
        assert resumed.step_runs["b"].outputs == {"b_out": "a"}

    def test_carried_outputs_feed_downstream_inputs(self, dag_service):
        first = dag_service.trigger("resume-dag", "r2")
        dag_service._fail_flag["fail_b"] = False
        resumed = dag_service.resume(first.run_id, "r2-resume")
        lineage = {l.step_name: l
                   for l in dag_service.lineage_of_run(resumed.run_id)}
        # 沿用步骤 a 的血缘誊自源运行并标注来源
        assert lineage["a"].carried_from_run_id == first.run_id
        assert lineage["a"].outputs == {"by": "a"}
        # 重跑的 b 以上游沿用结果为输入，血缘完整
        assert lineage["b"].carried_from_run_id is None
        assert lineage["b"].inputs == {"a": {"by": "a"}}
        assert lineage["c"].inputs == {"b": {"b_out": "a"}}

    def test_source_run_record_is_not_modified(self, dag_service):
        first = dag_service.trigger("resume-dag", "r3")
        before = dag_service.reproduce(first.run_id)
        dag_service._fail_flag["fail_b"] = False
        dag_service.resume(first.run_id, "r3-resume")
        after = dag_service.reproduce(first.run_id)
        assert before == after  # 源运行记录与血缘不被续跑改动
        source = dag_service.get_run(first.run_id)
        assert source.status == RunStatus.FAILED
        assert source.step_runs["b"].status == StepStatus.FAILED

    def test_resume_of_cancelled_run(self, service):
        entered, release = threading.Event(), threading.Event()

        def first(config, upstream):
            CALLS.append("s1")
            return {"done": True}

        def blocking(config, upstream):
            entered.set()
            release.wait(timeout=5)
            CALLS.append("s2")
            return {}

        def third(config, upstream):
            CALLS.append("s3")
            return {}

        service.register_handler("first", first)
        service.register_handler("blocking", blocking)
        service.register_handler("third", third)
        service.register_pipeline(PipelineDefinition("cancel-src", [
            StepDefinition("s1", "first"),
            StepDefinition("s2", "blocking", ["s1"]),
            StepDefinition("s3", "third", ["s2"]),
        ]))
        CALLS.clear()
        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(service.trigger("cancel-src", "c1")))
        t.start()
        assert entered.wait(timeout=5)
        run_id = service._store.find("runs", pipeline_name="cancel-src")[0]["run_id"]
        service.cancel(run_id)
        release.set()
        t.join(timeout=5)
        cancelled = holder[0]
        assert cancelled.status == RunStatus.CANCELLED
        CALLS.clear()

        resumed = service.resume(cancelled.run_id, "c1-resume")
        assert resumed.status == RunStatus.SUCCEEDED
        assert CALLS == ["s3"]  # s1/s2 当时已成功，全部沿用；只补跑 s3
        assert resumed.step_runs["s1"].origin_run_id == cancelled.run_id
        assert resumed.step_runs["s2"].origin_run_id == cancelled.run_id
        assert resumed.step_runs["s3"].origin_run_id is None


class TestResumeVersionPinning:
    def test_resume_uses_source_version_despite_changes(self, service):
        """配置变更、步骤改名、版本回滚之后，续跑仍按源运行的版本快照。"""
        flag = {"fail": True}
        service.register_handler("extract", lambda cfg, up: {"rows": cfg["n"]})

        def load(config, upstream):
            if flag["fail"]:
                raise RuntimeError("load 失败")
            return {"loaded": upstream["extract"]["rows"]}

        service.register_handler("load", load)
        service.register_pipeline(PipelineDefinition("etl", [
            StepDefinition("extract", "extract", [], {"n": 10}),
            StepDefinition("load", "load", ["extract"]),
        ]))
        first = service.trigger("etl", "v1-run")
        assert first.status == RunStatus.FAILED
        assert first.pipeline_version == 1

        # 中间发生：配置变更 + 步骤改名（v2），再回滚到 v1（v3）
        service.update_pipeline(PipelineDefinition("etl", [
            StepDefinition("extract_v2", "extract", [], {"n": 99}),
            StepDefinition("load", "load", ["extract_v2"]),
        ]), note="改名+调量")
        service.rollback_pipeline("etl", 1)

        flag["fail"] = False
        resumed = service.resume(first.run_id, "v1-resume")
        # 续跑严格按源运行的 v1 快照：旧步骤名、旧配置，不用最新版本
        assert resumed.pipeline_version == 1
        assert [s.name for s in resumed.definition_snapshot.steps] == ["extract", "load"]
        assert resumed.step_runs["extract"].origin_run_id == first.run_id
        assert resumed.step_runs["extract"].outputs == {"rows": 10}
        assert resumed.step_runs["load"].outputs == {"loaded": 10}
        # 与源运行可对着看：同版本、同快照、结论可复现
        replay = service.reproduce(resumed.run_id)
        assert replay["run"]["resumed_from_run_id"] == first.run_id
        steps = {s["step_name"]: s for s in replay["steps"]}
        assert steps["extract"]["origin_run_id"] == first.run_id
        assert steps["extract"]["lineage"]["carried_from_run_id"] == first.run_id
        assert steps["extract"]["lineage"]["step_config"] == {"n": 10}
        assert steps["load"]["lineage"]["inputs"] == {"extract": {"rows": 10}}


class TestResumeFailureAndCancel:
    def test_resume_fails_again_skips_downstream(self, dag_service):
        first = dag_service.trigger("resume-dag", "rf1")
        second = dag_service.resume(first.run_id, "rf1-resume")  # b 仍失败
        assert second.status == RunStatus.FAILED
        assert second.step_runs["a"].status == StepStatus.SUCCEEDED  # 沿用
        assert second.step_runs["b"].status == StepStatus.FAILED
        # 三种拒绝原因不混淆：直接上游失败 / 更上游连累 / 取消
        assert second.step_runs["c"].status == StepStatus.SKIPPED_UPSTREAM_FAILED
        assert second.step_runs["d"].status == StepStatus.SKIPPED_UPSTREAM_SKIPPED
        assert "b=FAILED" in (second.step_runs["c"].error or "")
        # 续跑的续跑：修复后从 second 接续，最终成功
        dag_service._fail_flag["fail_b"] = False
        third = dag_service.resume(second.run_id, "rf1-resume-2")
        assert third.status == RunStatus.SUCCEEDED
        assert third.resumed_from_run_id == second.run_id
        assert third.step_runs["a"].origin_run_id == second.run_id

    def test_cancel_during_resume_stops_consistent(self, dag_service):
        first = dag_service.trigger("resume-dag", "rc1")
        dag_service._fail_flag["fail_b"] = False
        entered, release = threading.Event(), threading.Event()

        def slow_c(config, upstream):
            entered.set()
            release.wait(timeout=5)
            CALLS.append("c")
            return {"by": "c"}

        dag_service.register_handler("ok_c", slow_c)
        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(dag_service.resume(first.run_id, "rc1-r")))
        t.start()
        assert entered.wait(timeout=5)  # 续跑执行到 c
        resume_id = holder_run_id = dag_service._store.find(
            "runs", pipeline_name="resume-dag",
            idempotency_key="rc1-r")[0]["run_id"]
        dag_service.cancel(resume_id)
        release.set()
        t.join(timeout=5)

        final = dag_service.get_run(holder[0].run_id)
        assert final.status == RunStatus.CANCELLED
        # 沿用的 a 与已完成的 b/c 保留；未开始的 d 以取消原因拒绝，无半生效态
        assert final.step_runs["a"].status == StepStatus.SUCCEEDED
        assert final.step_runs["a"].origin_run_id == first.run_id
        assert final.step_runs["b"].status == StepStatus.SUCCEEDED
        assert final.step_runs["c"].status == StepStatus.SUCCEEDED
        assert final.step_runs["d"].status == StepStatus.SKIPPED_CANCELLED
        # 被取消的续跑本身也是可续跑的历史运行
        dag_service.register_handler("ok_c", lambda cfg, up: {"by": "c"})
        third = dag_service.resume(final.run_id, "rc1-r2")
        assert third.status == RunStatus.SUCCEEDED
        assert third.step_runs["c"].origin_run_id == final.run_id


class TestInvalidResume:
    def test_resume_fully_succeeded_run_rejected(self, dag_service):
        dag_service._fail_flag["fail_b"] = False
        ok_run = dag_service.trigger("resume-dag", "ok-run")
        assert ok_run.status == RunStatus.SUCCEEDED
        with pytest.raises(ValidationError):
            dag_service.resume(ok_run.run_id, "bad-resume")

    def test_resume_missing_run_rejected(self, dag_service):
        with pytest.raises(NotFoundError):
            dag_service.resume("no-such-run", "k")

    def test_resume_active_run_rejected(self, service):
        entered, release = threading.Event(), threading.Event()

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
            target=lambda: holder.append(service.trigger("slow", "k1")))
        t.start()
        assert entered.wait(timeout=5)
        run_id = service._store.find("runs", pipeline_name="slow")[0]["run_id"]
        with pytest.raises(ConflictError):
            service.resume(run_id, "k2")  # 源运行尚未结束，不能续跑
        release.set()
        t.join(timeout=5)


class TestConcurrentResume:
    def test_same_key_concurrent_resume_executes_once(self, dag_service):
        first = dag_service.trigger("resume-dag", "cc1")
        dag_service._fail_flag["fail_b"] = False
        CALLS.clear()
        results, errors = [], []

        def work():
            try:
                results.append(dag_service.resume(first.run_id, "cc1-resume"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=work) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len({r.run_id for r in results}) == 1  # 同键并发只产生一次续跑
        assert CALLS == ["b", "c", "d"]  # 重跑部分只执行一次，无重复副作用

    def test_same_key_resubmit_returns_same_run(self, dag_service):
        first = dag_service.trigger("resume-dag", "cc2")
        dag_service._fail_flag["fail_b"] = False
        resumed = dag_service.resume(first.run_id, "cc2-resume")
        CALLS.clear()
        again = dag_service.resume(first.run_id, "cc2-resume")
        assert again.run_id == resumed.run_id
        assert CALLS == []  # 幂等命中：任何步骤都不重算

    def test_different_key_resume_during_active_resume_rejected(self, dag_service):
        first = dag_service.trigger("resume-dag", "cc3")
        dag_service._fail_flag["fail_b"] = False
        entered, release = threading.Event(), threading.Event()

        def slow_b(config, upstream):
            entered.set()
            release.wait(timeout=5)
            CALLS.append("b")
            return {"b_out": "a"}

        dag_service.register_handler("maybe_fail_b", slow_b)
        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(dag_service.resume(first.run_id, "cc3-r1")))
        t.start()
        assert entered.wait(timeout=5)  # 续跑进行中
        with pytest.raises(ConflictError):
            dag_service.resume(first.run_id, "cc3-r2")  # 异键：单活动运行约束
        release.set()
        t.join(timeout=5)
        assert holder[0].status == RunStatus.SUCCEEDED


class TestResumeApi:
    def test_http_resume_matches_direct_call(self):
        """HTTP 入口与直接调用共享同一门面实例，结论一致。"""
        import os
        os.environ["PIPELINE_DB"] = ":memory:"
        import main
        from fastapi.testclient import TestClient

        flag = {"fail": True}
        main.service.register_handler(
            "api_boom",
            lambda cfg, up: (_ for _ in ()).throw(RuntimeError("x"))
            if flag["fail"] else {"ok": True})
        client = TestClient(main.app)
        resp = client.post("/pipelines", json={
            "name": "api-pipe",
            "steps": [{"name": "only", "handler": "api_boom"}],
        })
        assert resp.status_code == 201
        run = client.post("/pipelines/api-pipe/runs",
                          json={"idempotency_key": "api-r1"}).json()
        assert run["status"] == "FAILED"

        flag["fail"] = False
        resp = client.post(f"/runs/{run['run_id']}/resume",
                           json={"idempotency_key": "api-r1-resume"})
        assert resp.status_code == 201
        resumed = resp.json()
        assert resumed["status"] == "SUCCEEDED"
        assert resumed["resumed_from_run_id"] == run["run_id"]
        # 与直接调用结论一致：同一运行、同一步骤结论
        direct = main.service.get_run(resumed["run_id"])
        assert direct.run_id == resumed["run_id"]
        assert direct.step_runs["only"].status.value == \
            resumed["step_runs"]["only"]["status"]
        # 全成功运行续跑：HTTP 层同样明确拒绝（422）
        resp = client.post(f"/runs/{resumed['run_id']}/resume",
                           json={"idempotency_key": "api-r2"})
        assert resp.status_code == 422
