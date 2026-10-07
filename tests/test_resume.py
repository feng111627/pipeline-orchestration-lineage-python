"""业务域：定点续跑与跨运行追溯。

基于一次已结束且有未成功步骤的历史运行续跑：成功步骤沿用旧结果不重算，
只重跑失败步骤及被连累的下游；定义/配置按源运行当时的版本；
源运行只读不改；非法续跑被明确拒绝；并发/重复续跑无重复副作用。
"""

from __future__ import annotations

import threading
import time

import pytest

from pipeline.errors import (ConflictError, NotFoundError,
                             ResumeNotPossibleError)
from pipeline.models import (PipelineDefinition, RunStatus, StepDefinition,
                             StepStatus)

EFFECTS: list[str] = []


def _ok(name):
    def handler(config, upstream):
        EFFECTS.append(name)
        return {"by": name, "from": dict(upstream)}
    return handler


def _boom(config, upstream):
    EFFECTS.append("boom")
    raise RuntimeError("磁盘写满")


@pytest.fixture()
def dag_service(service):
    """a 成功 → b 失败 → c（直接被拖累）→ d（间接被连累）。"""
    service.register_handler("ok_a", _ok("a"))
    service.register_handler("ok_c", _ok("c"))
    service.register_handler("ok_d", _ok("d"))
    service.register_handler("boom", _boom)
    service.register_pipeline(PipelineDefinition("dag", [
        StepDefinition("a", "ok_a"),
        StepDefinition("b", "boom", ["a"]),
        StepDefinition("c", "ok_c", ["b"]),
        StepDefinition("d", "ok_d", ["c"]),
    ]))
    EFFECTS.clear()
    return service


def _failed_run(dag_service, key="k-src"):
    run = dag_service.trigger("dag", key)
    assert run.status == RunStatus.FAILED
    return run


class TestResumeReusesSuccessfulSteps:
    def test_resume_is_new_run_with_source_link(self, dag_service):
        src = _failed_run(dag_service)
        # 修好 b：把 boom 换成成功 handler 后续跑
        dag_service.register_handler("boom", _ok("b-fixed"))
        resumed = dag_service.resume(src.run_id, "k-resume-1")

        assert resumed.run_id != src.run_id          # 全新运行
        assert resumed.resumed_from_run_id == src.run_id  # 交代来源
        assert resumed.status == RunStatus.SUCCEEDED

    def test_successful_steps_are_reused_not_rerun(self, dag_service):
        src = _failed_run(dag_service)
        dag_service.register_handler("boom", _ok("b-fixed"))
        resumed = dag_service.resume(src.run_id, "k-resume-2")

        # a 沿用旧结果（不重跑）；b/c/d 本次重算
        a = resumed.step_runs["a"]
        assert a.status == StepStatus.SUCCEEDED and a.reused is True
        assert a.outputs == {"by": "a", "from": {}}  # 旧输出直接交给后续
        for name in ("b", "c", "d"):
            sr = resumed.step_runs[name]
            assert sr.status == StepStatus.SUCCEEDED
            assert sr.reused is False
        # 副作用台账：源运行只执行了 a、b(失败)；续跑只执行 b、c、d，a 未再算
        assert EFFECTS.count("a") == 1
        assert EFFECTS[-3:] == ["b-fixed", "c", "d"]

    def test_reused_output_feeds_downstream(self, dag_service):
        src = _failed_run(dag_service)
        dag_service.register_handler("boom", _ok("b-fixed"))
        resumed = dag_service.resume(src.run_id, "k-resume-3")
        # c 的输入直接来自沿用/重算链路上的输出
        c = resumed.step_runs["c"]
        assert c.outputs["from"] == {"b": {"by": "b-fixed",
                                           "from": {"a": {"by": "a",
                                                          "from": {}}}}}

    def test_source_run_is_not_modified(self, dag_service):
        src = _failed_run(dag_service)
        dag_service.register_handler("boom", _ok("b-fixed"))
        dag_service.resume(src.run_id, "k-resume-4")
        src_after = dag_service.get_run(src.run_id)
        # 源运行结论、字段与沿用标记都不被改动或覆盖
        assert src_after.status == RunStatus.FAILED
        assert src_after.resumed_from_run_id is None
        assert src_after.step_runs["a"].reused is False
        assert src_after.step_runs["b"].status == StepStatus.FAILED
        assert src_after.step_runs["c"].status == StepStatus.SKIPPED_UPSTREAM_FAILED
        # 源运行血缘未被追加（沿用血缘只写在新运行上）
        assert {l.step_name for l in
                dag_service.lineage_of_run(src.run_id)} == {"a"}

    def test_resume_uses_source_version_after_definition_changed(self, service):
        service.register_handler("extract", lambda cfg, up: {"rows": cfg["n"]})
        service.register_handler("boom", _boom)
        service.register_handler(
            "load", lambda cfg, up: {"loaded": up["mid"]["rows"]})
        service.register_pipeline(PipelineDefinition("etl", [
            StepDefinition("extract", "extract", [], {"n": 10}),
            StepDefinition("mid", "boom", ["extract"]),
            StepDefinition("load", "load", ["mid"]),
        ]))
        src = service.trigger("etl", "e-v1")
        assert src.pipeline_version == 1
        # 中间改配置、改名、并把最新版本推进到与源运行不同的形态
        service.update_pipeline(PipelineDefinition("etl", [
            StepDefinition("extract", "extract", [], {"n": 99}),
            StepDefinition("mid_x", "ok_c", ["extract"]),  # 步骤改名
            StepDefinition("load", "load", ["mid_x"]),
        ]), note="改名+改配置")
        service.rollback_pipeline("etl", 1)  # 最新版本也不是源运行版本号

        # 修好 mid：透传 extract 的行数
        service.register_handler(
            "boom", lambda cfg, up: {"rows": up["extract"]["rows"]})
        resumed = service.resume(src.run_id, "e-resume")
        # 续跑仍按源运行当时的 v1：步骤名、配置都来自旧快照，不是最新定义
        assert resumed.pipeline_version == 1
        assert set(resumed.step_runs) == {"extract", "mid", "load"}
        assert resumed.step_runs["extract"].reused is True
        assert resumed.step_runs["load"].outputs == {"loaded": 10}


class TestResumeFailurePropagation:
    def test_resume_failure_again_skips_downstream_with_distinct_reasons(
            self, dag_service):
        src = _failed_run(dag_service)
        # b 仍然失败：c 直接被拖累，d 更上游间接连累
        resumed = dag_service.resume(src.run_id, "k-resume-fail")
        assert resumed.status == RunStatus.FAILED
        assert resumed.step_runs["a"].reused is True
        assert resumed.step_runs["b"].status == StepStatus.FAILED
        assert resumed.step_runs["b"].reused is False
        assert (resumed.step_runs["c"].status
                == StepStatus.SKIPPED_UPSTREAM_FAILED)
        assert resumed.step_runs["d"].status == StepStatus.SKIPPED_TRANSITIVE
        # 三种拒绝原因互不混淆
        statuses = {resumed.step_runs[n].status
                    for n in ("c", "d")}
        assert StepStatus.SKIPPED_CANCELLED not in statuses
        assert "直接上游步骤失败" in (resumed.step_runs["c"].error or "")
        assert "间接" in (resumed.step_runs["d"].error or "")


class TestResumeRejected:
    def test_resume_succeeded_run_rejected(self, service):
        service.register_handler("h", lambda cfg, up: {"ok": True})
        service.register_pipeline(PipelineDefinition(
            "p", [StepDefinition("a", "h")]))
        ok_run = service.trigger("p", "ok")
        with pytest.raises(ResumeNotPossibleError) as exc:
            service.resume(ok_run.run_id, "r")
        assert "全部成功" in str(exc.value)

    def test_resume_unknown_run_rejected(self, service):
        with pytest.raises(NotFoundError):
            service.resume("ghost-run", "r")

    def test_resume_active_run_rejected(self, service):
        entered = threading.Event()
        release = threading.Event()

        def blocking(config, upstream):
            entered.set()
            release.wait(timeout=5)
            return {}

        service.register_handler("block", blocking)
        service.register_pipeline(PipelineDefinition(
            "slow", [StepDefinition("s", "block")]))
        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(service.trigger("slow", "k")))
        t.start()
        assert entered.wait(timeout=5)
        # 实际活动运行 id 从存储取
        row = service._store.find_one("runs", pipeline_name="slow")
        with pytest.raises(ResumeNotPossibleError):
            service.resume(row["run_id"], "r3")
        release.set()
        t.join(timeout=5)

    def test_resume_cancelled_source_run(self, service):
        # 被取消的运行同样"已结束且有未成功步骤"，应能续跑并沿用已成功部分
        entered = threading.Event()
        release = threading.Event()

        def first(config, upstream):
            return {"v": 1}

        def blocking(config, upstream):
            entered.set()
            release.wait(timeout=5)
            # 取消信号到达后，以失败结束（模拟"跑到一半被叫停"的未成功步骤）
            raise RuntimeError("已取消，中止计算")

        service.register_handler("first", first)
        service.register_handler("block", blocking)
        service.register_handler("tail", lambda cfg, up: {"done": True})
        service.register_pipeline(PipelineDefinition("cp", [
            StepDefinition("s1", "first"),
            StepDefinition("s2", "block", ["s1"]),
            StepDefinition("s3", "tail", ["s2"]),
        ]))
        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(service.trigger("cp", "c-src")))
        t.start()
        assert entered.wait(timeout=5)
        row = service._store.find_one("runs", pipeline_name="cp")
        service.cancel(row["run_id"])
        release.set()
        t.join(timeout=5)
        src = service.get_run(row["run_id"])
        assert src.status == RunStatus.CANCELLED
        assert src.step_runs["s2"].status == StepStatus.FAILED

        # 修好 s2 后续跑：s1 沿用，s2/s3 重算
        service.register_handler("block", lambda cfg, up: {"ok": True})
        resumed = service.resume(src.run_id, "c-resume")
        assert resumed.status == RunStatus.SUCCEEDED
        assert resumed.step_runs["s1"].reused is True  # 取消前已成功：沿用
        assert resumed.step_runs["s2"].reused is False
        assert resumed.step_runs["s3"].reused is False
        assert resumed.step_runs["s3"].outputs == {"done": True}

    def test_resume_blocked_by_another_active_run(self, dag_service):
        src = _failed_run(dag_service)
        entered = threading.Event()
        release = threading.Event()

        def blocking(config, upstream):
            entered.set()
            release.wait(timeout=5)
            return {}

        # 另一条同管道的活动运行（用一个会阻塞的单步管道无法共用 dag，
        # 这里直接占用：先起一条 dag 的新运行并让其卡在 b 的 handler）
        dag_service.register_handler("ok_a", _ok("a"))
        # 让 a 之后的 b 阻塞：替换 boom
        def blocked_boom(config, upstream):
            entered.set()
            release.wait(timeout=5)
            return {}
        dag_service.register_handler("boom", blocked_boom)
        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(
                dag_service.trigger("dag", "active-key")))
        t.start()
        assert entered.wait(timeout=5)
        with pytest.raises(ConflictError):
            dag_service.resume(src.run_id, "resume-while-active")
        release.set()
        t.join(timeout=5)


class TestConcurrentDuplicateResume:
    def test_concurrent_same_key_resume_runs_once(self, dag_service):
        src = _failed_run(dag_service)
        fixed = _ok("b-fixed")
        dag_service.register_handler("boom", fixed)

        results, errors = [], []

        def work():
            try:
                results.append(dag_service.resume(src.run_id, "dup-key"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=work) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        run_ids = {r.run_id for r in results}
        assert len(run_ids) == 1                 # 幂等：同一续跑运行
        resumed = results[0]
        assert resumed.status == RunStatus.SUCCEEDED
        # 修复后的 b 只执行一次：无重复执行、无重复副作用
        assert EFFECTS.count("b-fixed") == 1

    def test_duplicate_resume_returns_same_run(self, dag_service):
        src = _failed_run(dag_service)
        dag_service.register_handler("boom", _ok("b-fixed"))
        first = dag_service.resume(src.run_id, "same-resume")
        second = dag_service.resume(src.run_id, "same-resume")
        assert second.run_id == first.run_id
        assert EFFECTS.count("b-fixed") == 1

    def test_concurrent_different_keys_only_one_resume_runs(self, dag_service):
        src = _failed_run(dag_service)
        started = threading.Event()

        def slow_fixed(config, upstream):
            started.set()
            time.sleep(0.3)  # 停留在活动运行窗口，放大并发续跑的竞态
            EFFECTS.append("b-fixed")
            return {"by": "b-fixed"}

        dag_service.register_handler("boom", slow_fixed)
        outcomes: list = []

        def work(key):
            try:
                outcomes.append(("ok", dag_service.resume(src.run_id, key)))
            except Exception as exc:  # noqa: BLE001
                outcomes.append(("conflict", exc))

        t1 = threading.Thread(target=work, args=("dk-1",))
        t1.start()
        assert started.wait(timeout=5)  # 第一个续跑已进入活动状态
        t2 = threading.Thread(target=work, args=("dk-2",))
        t2.start()  # 异键并发续跑同一源运行：必须被单活动运行约束拒绝
        t1.join(timeout=10); t2.join(timeout=10)

        statuses = sorted(o[0] for o in outcomes)
        assert statuses == ["conflict", "ok"]
        assert isinstance(next(o[1] for o in outcomes if o[0] == "conflict"),
                          ConflictError)
        # 只有一个续跑真正执行：修复步骤只跑一次，无重复副作用
        assert EFFECTS.count("b-fixed") == 1


class TestResumeCancellation:
    def test_cancel_resume_stops_consistently(self, service):
        entered = threading.Event()
        release = threading.Event()
        service.register_handler("first", _ok("first"))

        # s2 源运行失败；续跑时让它修好并快速成功，让执行推进到阻塞步骤 s3
        def boom_fixed(config, upstream):
            return {"rows": 1}

        def block(config, upstream):
            entered.set()
            release.wait(timeout=5)
            EFFECTS.append("block-ran")
            return {"done": True}

        def never(config, upstream):
            EFFECTS.append("never-ran")
            return {}

        service.register_handler("boom", boom_fixed)
        service.register_handler("block", block)
        service.register_handler("never", never)
        service.register_pipeline(PipelineDefinition("rc", [
            StepDefinition("s1", "first"),
            StepDefinition("s2", "boom", ["s1"]),
            StepDefinition("s3", "block", ["s2"]),
            StepDefinition("s4", "never", ["s3"]),
        ]))
        # 源运行里 s2 必须是失败的：触发时临时用会抛错的 handler
        def boom(config, upstream):
            raise RuntimeError("x")
        service.register_handler("boom", boom)
        src = service.trigger("rc", "rc-src")
        assert src.status == RunStatus.FAILED
        # 修好 s2 后续跑（s3 仍会阻塞以便测试取消）
        service.register_handler("boom", boom_fixed)

        holder: list = []
        t = threading.Thread(
            target=lambda: holder.append(service.resume(src.run_id, "rc-res")))
        t.start()
        assert entered.wait(timeout=5)  # 续跑卡在重跑步骤 s3
        # 从存储定位续跑运行
        for _ in range(100):
            row = service._store.find_one(
                "runs", resumed_from_run_id=src.run_id)
            if row:
                break
            import time
            time.sleep(0.01)
        cancelled = service.cancel(row["run_id"])
        release.set()
        t.join(timeout=5)

        final = service.get_run(cancelled.run_id)
        assert final.status == RunStatus.CANCELLED
        assert final.step_runs["s1"].reused is True       # 沿用保留
        assert final.step_runs["s2"].status == StepStatus.SUCCEEDED
        # 步骤是原子单位：取消发生时 s3 正在执行，允许其完成并生效一次
        assert final.step_runs["s3"].status == StepStatus.SUCCEEDED
        # 取消在步骤边界生效：未开始的 s4 是取消跳过，而非上游连累
        assert final.step_runs["s4"].status == StepStatus.SKIPPED_CANCELLED
        assert final.step_runs["s4"].status != StepStatus.SKIPPED_TRANSITIVE
        assert "never-ran" not in EFFECTS


class TestResumeLineageTraceability:
    def test_reused_lineage_marked_and_reproduce_shows_source(self, dag_service):
        src = _failed_run(dag_service)
        dag_service.register_handler("boom", _ok("b-fixed"))
        resumed = dag_service.resume(src.run_id, "k-resume-lineage")

        lineage = {l.step_name: l
                   for l in dag_service.lineage_of_run(resumed.run_id)}
        # 沿用步骤的血缘挂在新运行下，但标注了来源运行
        assert set(lineage) == {"a", "b", "c", "d"}
        assert lineage["a"].reused_from_run_id == src.run_id
        assert lineage["b"].reused_from_run_id is None
        # 沿用血缘中的输入/输出与源运行当时一致
        assert lineage["a"].outputs == {"by": "a", "from": {}}

        replay = dag_service.reproduce(resumed.run_id)
        assert replay["run"]["resumed_from_run_id"] == src.run_id
        steps = {s["step_name"]: s for s in replay["steps"]}
        assert steps["a"]["reused"] is True
        assert steps["a"]["lineage"]["reused_from_run_id"] == src.run_id
        assert steps["b"]["reused"] is False
        # 源运行复现结论不受影响
        src_replay = dag_service.reproduce(src.run_id)
        assert src_replay["run"]["resumed_from_run_id"] is None
        assert src_replay["run"]["status"] == RunStatus.FAILED.value


class TestResumeApiParity:
    def test_http_resume_matches_direct_call(self, service):
        from fastapi.testclient import TestClient

        import main
        main.service = service  # HTTP 入口与测试共享同一内存服务实例
        client = TestClient(main.app)

        service.register_handler("ok_a", _ok("a"))
        service.register_handler("boom", _boom)
        service.register_handler("ok_c", _ok("c"))
        service.register_pipeline(PipelineDefinition("api_dag", [
            StepDefinition("a", "ok_a"),
            StepDefinition("b", "boom", ["a"]),
            StepDefinition("c", "ok_c", ["b"]),
        ]))
        r1 = client.post("/pipelines/api_dag/runs",
                         json={"idempotency_key": "h1"})
        src_id = r1.json()["run_id"]
        assert r1.json()["status"] == "FAILED"

        service.register_handler("boom", _ok("b-fixed"))
        r2 = client.post(f"/runs/{src_id}/resume",
                         json={"idempotency_key": "h-resume"})
        assert r2.status_code == 201
        body = r2.json()
        assert body["resumed_from_run_id"] == src_id
        assert body["status"] == "SUCCEEDED"
        assert body["step_runs"]["a"]["reused"] is True
        assert body["step_runs"]["b"]["reused"] is False

        # 与直接调用的结论一致：同键重复续跑返回同一运行
        direct = service.resume(src_id, "h-resume")
        assert direct.run_id == body["run_id"]

    def test_http_resume_rejected_is_explicit(self, service):
        from fastapi.testclient import TestClient

        import main
        main.service = service
        client = TestClient(main.app)
        service.register_handler("h", lambda cfg, up: {})
        service.register_pipeline(PipelineDefinition(
            "okp", [StepDefinition("a", "h")]))
        r1 = client.post("/pipelines/okp/runs", json={"idempotency_key": "ok"})
        ok_id = r1.json()["run_id"]
        r2 = client.post(f"/runs/{ok_id}/resume",
                         json={"idempotency_key": "r"})
        assert r2.status_code == 409  # 明确拒绝，而不是 200/201 的空运行
        assert "全部成功" in r2.json()["detail"]
