"""管道定义的版本化注册：创建新版本、回滚（产生指向旧内容的新版本）。

规则：
- 版本号单调递增，历史版本不可变、不可删除 —— 历史运行的可追溯性由此保证。
- 回滚不是"把指针拨回去"，而是用历史版本的内容生成一个新版本，
  因此已完成运行引用的旧版本号依然有效。
"""
from __future__ import annotations

from datetime import datetime, timezone

from .errors import NotFoundError, ValidationError
from .models import PipelineVersion, StepDef
from .store import Store


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_steps(steps: list[StepDef]) -> None:
    """校验定义：步骤名唯一、依赖存在、无依赖环。"""
    names = [s.name for s in steps]
    if len(names) != len(set(names)):
        dup = sorted({n for n in names if names.count(n) > 1})
        raise ValidationError(f"步骤名重复: {dup}")
    known = set(names)
    for s in steps:
        for dep in s.depends_on:
            if dep not in known:
                raise ValidationError(f"步骤 {s.name} 依赖不存在的步骤: {dep}")
            if dep == s.name:
                raise ValidationError(f"步骤 {s.name} 不能依赖自身")
    # 拓扑检测环
    color: dict[str, int] = {}  # 0=未访问 1=访问中 2=完成
    deps = {s.name: s.depends_on for s in steps}

    def visit(n: str, stack: tuple[str, ...]) -> None:
        color[n] = 1
        for d in deps[n]:
            if color.get(d, 0) == 1:
                raise ValidationError(f"依赖环: {' -> '.join(stack + (n, d))}")
            if color.get(d, 0) == 0:
                visit(d, stack + (n,))
        color[n] = 2

    for n in deps:
        if color.get(n, 0) == 0:
            visit(n, ())


class Registry:
    def __init__(self, store: Store) -> None:
        self._store = store

    def create_version(self, pipeline_id: str, steps: list[StepDef], comment: str = "") -> PipelineVersion:
        """校验定义后追加一个不可变新版本。"""
        if not steps:
            raise ValidationError("管道至少需要一个步骤")
        validate_steps(steps)
        latest = self._store.latest_version(pipeline_id)
        next_version = (latest.version + 1) if latest else 1
        version = PipelineVersion(pipeline_id=pipeline_id, version=next_version,
                                  steps=tuple(steps), created_at=_now(), comment=comment)
        self._store.save_pipeline_version(version)
        return version

    def rollback(self, pipeline_id: str, to_version: int, comment: str = "") -> PipelineVersion:
        """回滚：以历史版本内容生成新版本，不修改任何历史版本。"""
        target = self._store.get_pipeline_version(pipeline_id, to_version)
        if target is None:
            raise NotFoundError(f"回滚目标不存在: {pipeline_id}@v{to_version}")
        latest = self._store.latest_version(pipeline_id)
        assert latest is not None  # target 存在则 latest 必存在
        new_version = PipelineVersion(
            pipeline_id=pipeline_id, version=latest.version + 1,
            steps=target.steps, created_at=_now(),
            comment=comment or f"rollback to v{to_version}")
        self._store.save_pipeline_version(new_version)
        return new_version

    def get(self, pipeline_id: str, version: int | None = None) -> PipelineVersion:
        """取指定版本；version 为 None 时取最新。"""
        if version is None:
            found = self._store.latest_version(pipeline_id)
        else:
            found = self._store.get_pipeline_version(pipeline_id, version)
        if found is None:
            raise NotFoundError(f"管道版本不存在: {pipeline_id}@v{version}")
        return found
