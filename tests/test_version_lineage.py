"""业务域：版本与血缘。

配置变更、步骤改名、版本回滚之后，历史运行的结论仍可复现；
回滚以"新版本指向旧定义"实现，不修改任何历史版本。
"""

from __future__ import annotations

import pytest

from pipeline.errors import NotFoundError
from pipeline.models import (PipelineDefinition, RunStatus, StepDefinition)


@pytest.fixture()
def etl_service(service):
    service.register_handler("extract", lambda cfg, up: {"rows": cfg["n"]})
    service.register_handler("load", lambda cfg, up: {"loaded": up["extract"]["rows"]})
    service.register_pipeline(PipelineDefinition("etl", [
        StepDefinition("extract", "extract", [], {"n": 10}),
        StepDefinition("load", "load", ["extract"]),
    ]))
    return service


class TestVersionRollbackAndReproduce:
    def test_config_change_does_not_alter_history(self, etl_service):
        old_run = etl_service.trigger("etl", "run-v1")
        assert old_run.pipeline_version == 1

        etl_service.update_pipeline(PipelineDefinition("etl", [
            StepDefinition("extract", "extract", [], {"n": 99}),
            StepDefinition("load", "load", ["extract"]),
        ]), note="调大提取量")
        new_run = etl_service.trigger("etl", "run-v2")
        assert new_run.pipeline_version == 2
        assert new_run.step_runs["load"].outputs == {"loaded": 99}

        # 历史运行复现：仍是 v1 的配置与结论
        replay = etl_service.reproduce(old_run.run_id)
        assert replay["run"]["pipeline_version"] == 1
        steps = {s["step_name"]: s for s in replay["steps"]}
        assert steps["extract"]["outputs"] == {"rows": 10}
        assert steps["extract"]["lineage"]["step_config"] == {"n": 10}
        assert steps["load"]["lineage"]["inputs"] == {"extract": {"rows": 10}}

    def test_step_rename_does_not_alter_history(self, etl_service):
        old_run = etl_service.trigger("etl", "run-before-rename")
        etl_service.update_pipeline(PipelineDefinition("etl", [
            StepDefinition("extract_v2", "extract", [], {"n": 10}),
            StepDefinition("load", "load", ["extract_v2"]),
        ]), note="步骤改名")
        replay = etl_service.reproduce(old_run.run_id)
        # 快照里仍是旧步骤名，结论可复现
        assert [s["step_name"] for s in replay["steps"]] == ["extract", "load"]

    def test_rollback_creates_new_version_and_keeps_traceability(self, etl_service):
        run_v1 = etl_service.trigger("etl", "r1")
        etl_service.update_pipeline(PipelineDefinition("etl", [
            StepDefinition("extract", "extract", [], {"n": 99}),
            StepDefinition("load", "load", ["extract"]),
        ]))
        v3 = etl_service.rollback_pipeline("etl", 1)
        assert v3.version == 3
        assert v3.note == "rollback to v1"
        # 回滚后的最新定义与 v1 一致
        assert v3.definition.steps[0].config == {"n": 10}
        # 历史版本未被修改
        versions = etl_service.list_versions("etl")
        assert [v.version for v in versions] == [1, 2, 3]
        assert versions[0].definition.steps[0].config == {"n": 10}
        assert versions[1].definition.steps[0].config == {"n": 99}
        # 回滚后的新运行使用 v3（=v1 定义），且 v1 历史运行仍可复现
        run_v3 = etl_service.trigger("etl", "r3")
        assert run_v3.pipeline_version == 3
        assert run_v3.step_runs["load"].outputs == {"loaded": 10}
        replay_v1 = etl_service.reproduce(run_v1.run_id)
        assert replay_v1["run"]["status"] == RunStatus.SUCCEEDED.value

    def test_rollback_to_missing_version_rejected(self, etl_service):
        with pytest.raises(NotFoundError):
            etl_service.rollback_pipeline("etl", 42)
