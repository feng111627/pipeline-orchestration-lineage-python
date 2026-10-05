"""管道定义注册与版本管理。

每次定义变更（含回滚）都产生新的不可变版本号；历史版本永不修改，
因此历史运行引用的版本始终可解析、可复现。
"""

from __future__ import annotations

import time

from .errors import ConflictError, NotFoundError, ValidationError
from .models import (PipelineDefinition, PipelineVersion, definition_from_dict,
                     definition_to_dict, version_from_dict, version_to_dict)
from .store import JsonStore

_COLLECTION = "pipeline_versions"


def validate_definition(definition: PipelineDefinition) -> None:
    """校验定义：非空、步骤名唯一、依赖存在、无环。"""
    if not definition.name:
        raise ValidationError("管道名不能为空")
    if not definition.steps:
        raise ValidationError("管道至少需要一个步骤")
    names = [s.name for s in definition.steps]
    if len(set(names)) != len(names):
        raise ValidationError(f"步骤名重复: {sorted({n for n in names if names.count(n) > 1})}")
    known = set(names)
    for s in definition.steps:
        if not s.handler:
            raise ValidationError(f"步骤 {s.name} 缺少 handler")
        for dep in s.depends_on:
            if dep not in known:
                raise ValidationError(f"步骤 {s.name} 依赖未知步骤 {dep}")
            if dep == s.name:
                raise ValidationError(f"步骤 {s.name} 不能依赖自身")
    # 拓扑排序检测环
    indegree = {n: 0 for n in names}
    for s in definition.steps:
        for _ in s.depends_on:
            indegree[s.name] += 1
    queue = [n for n, d in indegree.items() if d == 0]
    visited = 0
    deps_of = {s.name: list(s.depends_on) for s in definition.steps}
    dependents: dict[str, list[str]] = {n: [] for n in names}
    for s in definition.steps:
        for dep in s.depends_on:
            dependents[dep].append(s.name)
    while queue:
        node = queue.pop()
        visited += 1
        for child in dependents[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if visited != len(names):
        raise ValidationError("依赖关系存在环")


def topo_order(definition: PipelineDefinition) -> list[str]:
    """返回依赖优先的步骤名顺序（稳定：按定义顺序打破平局）。"""
    order = [s.name for s in definition.steps]
    deps = {s.name: set(s.depends_on) for s in definition.steps}
    result: list[str] = []
    done: set[str] = set()
    remaining = list(order)
    while remaining:
        progressed = False
        for name in list(remaining):
            if deps[name] <= done:
                result.append(name)
                done.add(name)
                remaining.remove(name)
                progressed = True
        if not progressed:  # 有环时不应到达（定义已校验）
            raise ValidationError("依赖关系存在环")
    return result


class PipelineRegistry:
    def __init__(self, store: JsonStore):
        self._store = store

    def _versions_of(self, pipeline_name: str) -> list[PipelineVersion]:
        rows = self._store.find(_COLLECTION, pipeline_name=pipeline_name)
        return sorted((version_from_dict(r) for r in rows), key=lambda v: v.version)

    def register(self, definition: PipelineDefinition) -> PipelineVersion:
        """注册新管道（version=1）。已存在同名管道则抛 ConflictError。"""
        validate_definition(definition)
        with self._store.lock:
            if self._versions_of(definition.name):
                raise ConflictError(f"管道 {definition.name} 已存在，请使用 update")
            return self._append_version(definition, note="register")

    def update(self, definition: PipelineDefinition, note: str = "") -> PipelineVersion:
        """提交定义变更，产生新版本。定义非法抛 ValidationError。"""
        validate_definition(definition)
        with self._store.lock:
            if not self._versions_of(definition.name):
                raise NotFoundError(f"管道 {definition.name} 不存在，请先 register")
            return self._append_version(definition, note=note or "update")

    def rollback(self, pipeline_name: str, to_version: int) -> PipelineVersion:
        """回滚：以历史版本的定义创建一个全新版本，不修改任何历史版本。"""
        with self._store.lock:
            target = self.get_version(pipeline_name, to_version)
            # 深拷贝历史定义，避免共享可变对象
            restored = definition_from_dict(definition_to_dict(target.definition))
            return self._append_version(restored, note=f"rollback to v{to_version}")

    def _append_version(self, definition: PipelineDefinition, note: str) -> PipelineVersion:
        existing = self._versions_of(definition.name)
        version = PipelineVersion(
            pipeline_name=definition.name,
            version=(existing[-1].version + 1) if existing else 1,
            definition=definition,
            created_at=time.time(),
            note=note,
        )
        self._store.append(_COLLECTION, version_to_dict(version))
        return version

    def get_version(self, pipeline_name: str, version: int) -> PipelineVersion:
        row = self._store.find_one(_COLLECTION, pipeline_name=pipeline_name,
                                   version=version)
        if row is None:
            raise NotFoundError(f"管道 {pipeline_name} 的版本 v{version} 不存在")
        return version_from_dict(row)

    def latest(self, pipeline_name: str) -> PipelineVersion:
        versions = self._versions_of(pipeline_name)
        if not versions:
            raise NotFoundError(f"管道 {pipeline_name} 不存在")
        return versions[-1]

    def list_versions(self, pipeline_name: str) -> list[PipelineVersion]:
        versions = self._versions_of(pipeline_name)
        if not versions:
            raise NotFoundError(f"管道 {pipeline_name} 不存在")
        return versions
