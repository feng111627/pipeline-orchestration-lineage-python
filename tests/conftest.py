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
    "TestResumeReusesSuccessfulSteps": "定点续跑：沿用旧结果、按源版本续跑、源运行只读不改",
    "TestResumeFailurePropagation": "续跑失败传播：再次失败下游被拒且三类原因可区分",
    "TestResumeRejected": "非法续跑拒绝：未结束/全成功/被活动运行占用均明确拒绝",
    "TestConcurrentDuplicateResume": "并发与重复续跑：幂等返回同一运行、无重复执行副作用",
    "TestResumeCancellation": "续跑取消：停在一致状态、沿用保留、未开始步骤取消跳过",
    "TestResumeLineageTraceability": "跨运行追溯：沿用血缘标注来源、复现交代续跑链",
    "TestResumeApiParity": "续跑接口：HTTP 发起续跑与直接调用结论一致、拒绝可识别",
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
