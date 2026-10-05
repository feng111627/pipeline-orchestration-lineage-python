"""业务域：血缘审计 —— 每次运行的输入、输出与配置版本可追溯。"""
from __future__ import annotations

import pytest

from pipeline.errors import NotFoundError
from pipeline.models import StepDef


def test_lineage_records_inputs_outputs_and_config_version(registry, executor, lineage):
    """成功运行的每个步骤都有血缘记录，含运行输入、上游输出与锚定版本。"""
    executor.register_handler("produce", lambda c, i, u: {"rows": c["n"]})
    executor.register_handler("consume", lambda c, i, u: {"seen": u["up"]["rows"]})
    registry.create_version("p", [
        StepDef("up", handler="produce", config={"n": 7}),
        StepDef("down", ("up",), handler="consume"),
    ])
    run = executor.trigger("p", inputs={"src": "db"})

    records = {r.step_name: r for r in lineage.lineage_of_run(run.run_id)}
    assert set(records) == {"up", "down"}
    up, down = records["up"], records["down"]
    assert up.pipeline_version == 1
    assert up.inputs["run_inputs"] == {"src": "db"}
    assert up.outputs == {"rows": 7}
    # 下游血缘中可追溯到上游输出
    assert down.inputs["upstream"] == {"up": {"rows": 7}}
    assert down.outputs == {"seen": 7}


def test_failed_run_lineage_only_covers_executed_steps(registry, executor, lineage):
    """失败运行中，未执行/被跳过的步骤不产生血缘记录。"""
    executor.register_handler("boom", lambda c, i, u: (_ for _ in ()).throw(RuntimeError("x")))
    registry.create_version("p", [StepDef("a"), StepDef("b", ("a",), handler="boom"),
                                  StepDef("c", ("b",))])
    run = executor.trigger("p")

    steps = {r.step_name for r in lineage.lineage_of_run(run.run_id)}
    assert steps == {"a"}  # b 失败无输出、c 被跳过，均无血缘


def test_lineage_of_unknown_run_is_rejected(lineage):
    """查询不存在运行的血缘必须明确报错。"""
    with pytest.raises(NotFoundError):
        lineage.lineage_of_run("no-such-run")


def test_lineage_references_pinned_version_after_later_changes(registry, executor, lineage):
    """后续版本变更后，历史运行的血缘仍锚定其执行时的版本号。"""
    registry.create_version("p", [StepDef("a", config={"v": 1})])
    run = executor.trigger("p")
    registry.create_version("p", [StepDef("a", config={"v": 2})])
    registry.rollback("p", 1)

    records = lineage.lineage_of_run(run.run_id)
    assert len(records) == 1
    assert records[0].pipeline_version == 1
