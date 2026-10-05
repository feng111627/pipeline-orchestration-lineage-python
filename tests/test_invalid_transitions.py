"""业务域：非法状态流转与非法定义。

非法流转（取消终态运行、重复注册、更新不存在的管道、依赖环等）
必须被明确拒绝，而不是被静默覆盖。
"""

from __future__ import annotations

import pytest

from pipeline.errors import (ConflictError, InvalidTransitionError,
                             NotFoundError, ValidationError)
from pipeline.models import (PipelineDefinition, RunStatus, StepDefinition)


@pytest.fixture()
def basic_service(service):
    service.register_handler("h", lambda cfg, up: {})
    service.register_pipeline(PipelineDefinition("p", [StepDefinition("a", "h")]))
    return service


class TestInvalidTransitions:
    def test_cancel_terminal_run_rejected(self, basic_service):
        run = basic_service.trigger("p", "t1")
        assert run.status == RunStatus.SUCCEEDED
        with pytest.raises(InvalidTransitionError):
            basic_service.cancel(run.run_id)

    def test_duplicate_register_rejected(self, basic_service):
        with pytest.raises(ConflictError):
            basic_service.register_pipeline(
                PipelineDefinition("p", [StepDefinition("a", "h")]))

    def test_update_missing_pipeline_rejected(self, basic_service):
        with pytest.raises(NotFoundError):
            basic_service.update_pipeline(
                PipelineDefinition("ghost", [StepDefinition("a", "h")]))

    def test_get_missing_run_rejected(self, basic_service):
        with pytest.raises(NotFoundError):
            basic_service.get_run("no-such-run")

    def test_reproduce_missing_run_rejected(self, basic_service):
        with pytest.raises(NotFoundError):
            basic_service.reproduce("no-such-run")


class TestInvalidDefinitions:
    """定义校验同样属于"明确拒绝"：非法定义不得进入版本历史。"""

    @pytest.mark.parametrize("steps,frag", [
        ([StepDefinition("a", "h", ["b"]), StepDefinition("b", "h", ["a"])], "环"),
        ([StepDefinition("a", "h", ["ghost"])], "未知"),
        ([StepDefinition("a", "h"), StepDefinition("a", "h")], "重复"),
        ([StepDefinition("a", "h", ["a"])], "自身"),
        ([], "至少"),
    ])
    def test_invalid_definition_rejected(self, basic_service, steps, frag):
        with pytest.raises(ValidationError) as exc:
            basic_service.register_pipeline(PipelineDefinition("bad", steps))
        assert frag in str(exc.value)
        # 被拒绝的定义不产生版本
        with pytest.raises(NotFoundError):
            basic_service.list_versions("bad")
