"""业务域：依赖失败传播。

上游失败时下游不得执行，且拒绝原因必须与"取消跳过"可区分；
失败步骤不得留下任何副作用。
"""

from __future__ import annotations

import pytest

from pipeline.models import (PipelineDefinition, RunStatus, StepDefinition,
                             StepStatus)

EFFECTS: list[str] = []  # 模拟外部副作用台账


def _ok(name):
    def handler(config, upstream):
        EFFECTS.append(name)
        return {"by": name}
    return handler


def _boom(config, upstream):
    # handler 契约：副作用只能通过返回的 outputs 由编排器在成功后统一生效；
    # 这里在产生任何输出前失败，模拟"计算中途崩溃"。
    raise RuntimeError("磁盘写满")


@pytest.fixture()
def dag_service(service):
    service.register_handler("ok_a", _ok("a"))
    service.register_handler("ok_c", _ok("c"))
    service.register_handler("boom", _boom)
    service.register_pipeline(PipelineDefinition("dag", [
        StepDefinition("a", "ok_a"),
        StepDefinition("b", "boom", ["a"]),
        StepDefinition("c", "ok_c", ["b"]),
        StepDefinition("d", "ok_a", ["c"]),  # 更下游，间接受影响
    ]))
    EFFECTS.clear()
    return service


class TestDependencyFailurePropagation:
    def test_upstream_failure_skips_downstream(self, dag_service):
        run = dag_service.trigger("dag", "k-fail-1")
        assert run.status == RunStatus.FAILED
        assert run.step_runs["a"].status == StepStatus.SUCCEEDED
        assert run.step_runs["b"].status == StepStatus.FAILED
        # 直接与间接受影响步骤都被拒绝执行
        assert run.step_runs["c"].status == StepStatus.SKIPPED_UPSTREAM_FAILED
        assert run.step_runs["d"].status == StepStatus.SKIPPED_UPSTREAM_FAILED

    def test_skip_reason_is_distinguishable(self, dag_service):
        run = dag_service.trigger("dag", "k-fail-2")
        # 拒绝原因可区分：上游失败 ≠ 取消
        assert run.step_runs["c"].status != StepStatus.SKIPPED_CANCELLED
        assert "b=FAILED" in (run.step_runs["c"].error or "")

    def test_failed_step_has_no_side_effects(self, dag_service):
        dag_service.trigger("dag", "k-fail-3")
        # 失败步骤自身及下游都不产生业务副作用（只有 a 生效）
        assert EFFECTS == ["a"]
        run = dag_service.get_run(
            dag_service.trigger("dag", "k-fail-3").run_id)  # 幂等返回同一运行
        assert run.step_runs["b"].outputs == {}  # 失败步骤零输出

    def test_failed_step_writes_no_lineage(self, dag_service):
        run = dag_service.trigger("dag", "k-fail-4")
        lineage_steps = {l.step_name for l in dag_service.lineage_of_run(run.run_id)}
        assert lineage_steps == {"a"}  # 只有成功步骤有血缘
