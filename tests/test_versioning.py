"""业务域：版本管理与历史复现 —— 配置变更/改名/回滚不破坏历史运行的可复现性。"""
from __future__ import annotations

import pytest

from pipeline.errors import NotFoundError, ValidationError
from pipeline.models import RunStatus, StepDef


def test_versions_are_immutable_and_monotonic(registry):
    """版本号单调递增，历史版本内容不可变。"""
    v1 = registry.create_version("p", [StepDef("a", config={"k": 1})])
    v2 = registry.create_version("p", [StepDef("a", config={"k": 2})])
    assert (v1.version, v2.version) == (1, 2)
    assert registry.get("p", 1).steps[0].config == {"k": 1}
    assert registry.get("p").version == 2  # 默认取最新


def test_rollback_creates_new_version_without_touching_history(registry):
    """回滚产生指向旧内容的新版本，历史版本原样保留。"""
    registry.create_version("p", [StepDef("a", config={"k": 1})])
    registry.create_version("p", [StepDef("b", config={"k": 2})])
    v3 = registry.rollback("p", to_version=1)

    assert v3.version == 3
    assert [s.name for s in v3.steps] == ["a"]
    assert v3.steps[0].config == {"k": 1}
    # 历史版本未被修改
    assert [s.name for s in registry.get("p", 2).steps] == ["b"]
    assert registry.get("p", 1).steps[0].config == {"k": 1}


def test_rollback_to_missing_version_is_rejected(registry):
    """回滚到不存在的版本必须明确报错。"""
    registry.create_version("p", [StepDef("a")])
    with pytest.raises(NotFoundError):
        registry.rollback("p", to_version=99)


def test_invalid_definitions_are_rejected(registry):
    """依赖缺失、依赖环、重名步骤都在注册期被拒绝。"""
    with pytest.raises(ValidationError):
        registry.create_version("bad1", [StepDef("a", ("ghost",))])
    with pytest.raises(ValidationError):
        registry.create_version("bad2", [StepDef("x", ("y",)), StepDef("y", ("x",))])
    with pytest.raises(ValidationError):
        registry.create_version("bad3", [StepDef("a"), StepDef("a")])


def test_historical_run_reproducible_after_config_change_and_rename(registry, executor, lineage):
    """配置变更与步骤改名后，历史运行仍按当时版本复现出相同结论。"""
    registry.create_version("p", [StepDef("extract", config={"path": "/v1"}),
                                  StepDef("load", ("extract",))])
    run_v1 = executor.trigger("p", inputs={"date": "2026-10-01"})
    assert run_v1.pipeline_version == 1

    # 配置变更 + 步骤改名
    registry.create_version("p", [StepDef("extract_v2", config={"path": "/v2"})])
    run_v2 = executor.trigger("p")
    assert run_v2.pipeline_version == 2

    report = lineage.replay(run_v1.run_id)
    assert report["pipeline_version"] == 1
    assert report["status"] == RunStatus.SUCCEEDED.value
    assert report["inputs"] == {"date": "2026-10-01"}
    assert report["steps"][0]["step_name"] == "extract"
    assert report["steps"][0]["config"] == {"path": "/v1"}  # 当时的配置版本


def test_replay_survives_rollback(registry, executor, lineage):
    """回滚之后，回滚前后各次运行的结论都可复现且互不干扰。"""
    registry.create_version("p", [StepDef("a", config={"k": 1})])
    run1 = executor.trigger("p")
    registry.create_version("p", [StepDef("a", config={"k": 2})])
    run2 = executor.trigger("p")
    registry.rollback("p", 1)
    run3 = executor.trigger("p")

    assert run3.pipeline_version == 3
    r1 = lineage.replay(run1.run_id)
    r2 = lineage.replay(run2.run_id)
    r3 = lineage.replay(run3.run_id)
    assert r1["steps"][0]["config"] == {"k": 1}
    assert r2["steps"][0]["config"] == {"k": 2}
    assert r3["steps"][0]["config"] == {"k": 1}  # 回滚后的新运行用旧内容
    # 三次运行各自锚定不同版本号，可追溯性未被回滚破坏
    assert (r1["pipeline_version"], r2["pipeline_version"], r3["pipeline_version"]) == (1, 2, 3)
