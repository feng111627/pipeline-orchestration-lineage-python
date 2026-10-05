"""测试基础设施：隔离的临时存储 + 业务覆盖日志。

运行 pytest -v 时，每个用例会打印其所属业务域与目的，
使测试日志能直接看出覆盖了哪些业务及各用例的验证点。
"""
from __future__ import annotations

import pytest

from pipeline.executor import Executor
from pipeline.lineage import LineageService
from pipeline.registry import Registry
from pipeline.store import Store

BUSINESS_AREAS = {
    "test_failure_propagation": "依赖失败传播",
    "test_concurrency": "并发与重复提交控制",
    "test_cancellation": "运行取消一致性",
    "test_versioning": "版本管理与历史复现",
    "test_lineage": "血缘审计",
}


@pytest.hookimpl(wrapper=True)
def pytest_runtest_protocol(item, nextitem):
    module = item.module.__name__.rsplit(".", 1)[-1]
    area = BUSINESS_AREAS.get(module, module)
    doc = (item.obj.__doc__ or "").strip().splitlines()
    purpose = doc[0] if doc else ""
    print(f"\n[业务域] {area} | [用例] {item.name} | [验证点] {purpose}")
    return (yield)


@pytest.fixture()
def store(tmp_path):
    return Store(tmp_path / "data")


@pytest.fixture()
def registry(store):
    return Registry(store)


@pytest.fixture()
def executor(store):
    return Executor(store)


@pytest.fixture()
def lineage(store):
    return LineageService(store)
