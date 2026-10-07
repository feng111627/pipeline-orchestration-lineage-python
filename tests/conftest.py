"""测试公共装置与业务覆盖日志。

按业务域组织测试类，conftest 在会话结束时汇总输出
各业务域覆盖的用例数与通过情况，让单测日志直接可读。
"""

from __future__ import annotations

import logging

import pytest

from pipeline.service import PipelineService

logger = logging.getLogger("business-coverage")

# 测试类名 -> 业务域说明
BUSINESS_AREAS = {
    "TestDependencyFailurePropagation": "依赖失败传播：上游失败下游拒绝执行且原因可区分",
    "TestConcurrentDuplicateTrigger": "并发与重复提交：单活动运行、幂等去重、无副作用重复",
    "TestVersionRollbackAndReproduce": "版本与血缘：配置变更/改名/回滚后历史运行可复现",
    "TestCancellationConsistency": "取消一致性：运行停在一致状态、无半生效中间态",
    "TestInvalidTransitions": "非法状态流转：明确拒绝而非静默覆盖",
    "TestInvalidDefinitions": "非法定义校验：依赖环/未知依赖/重名/自依赖均被拒绝",
    "TestResumeFromFailure": "定点续跑：沿用已成功步骤旧结果，只重跑失败与受连累步骤",
    "TestResumeVersionPinning": "续跑版本锚定：配置变更/改名/回滚后仍按源运行版本快照续跑",
    "TestResumeFailureAndCancel": "续跑异常：再次失败三分类拒绝传播、续跑取消停在一致状态",
    "TestInvalidResume": "非法续跑：全成功/不存在/未结束的运行明确拒绝",
    "TestConcurrentResume": "并发与重复续跑：同键幂等、异键单活动运行、无重复副作用",
    "TestResumeApi": "续跑服务接口：HTTP 入口与直接调用结论一致",
}


@pytest.fixture()
def service() -> PipelineService:
    return PipelineService()  # :memory: 存储，每个用例独立


def pytest_configure(config):
    config.addinivalue_line("markers", "business: 业务域标记")
    logging.basicConfig(level=logging.INFO, format="%(message)s")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        item.session._coverage_records = getattr(
            item.session, "_coverage_records", [])
        item.session._coverage_records.append((item.cls.__name__ if item.cls else "",
                                               item.name, report.outcome))


def pytest_sessionfinish(session, exitstatus):
    records = getattr(session, "_coverage_records", [])
    logger.info("=" * 70)
    logger.info("业务覆盖汇总")
    logger.info("=" * 70)
    for cls, desc in BUSINESS_AREAS.items():
        cases = [(n, o) for c, n, o in records if c == cls]
        passed = sum(1 for _, o in cases if o == "passed")
        logger.info(f"[{desc}]")
        logger.info(f"  用例 {passed}/{len(cases)} 通过")
        for name, outcome in cases:
            logger.info(f"    - {name}: {outcome}")
