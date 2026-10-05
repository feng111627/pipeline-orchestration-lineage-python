"""管道定义的版本化注册：创建新版本、回滚（产生指向旧内容的新版本）。"""
from __future__ import annotations

from .models import PipelineVersion, StepDef
from .store import Store


class Registry:
    def __init__(self, store: Store) -> None:
        self._store = store
        raise NotImplementedError

    def create_version(self, pipeline_id: str, steps: list[StepDef], comment: str = "") -> PipelineVersion:
        """校验定义（依赖存在、无环、步骤名唯一）后追加一个不可变新版本。"""
        raise NotImplementedError

    def rollback(self, pipeline_id: str, to_version: int, comment: str = "") -> PipelineVersion:
        """回滚：以历史版本内容生成新版本，不修改任何历史版本。"""
        raise NotImplementedError

    def get(self, pipeline_id: str, version: int | None = None) -> PipelineVersion:
        """取指定版本；version 为 None 时取最新。"""
        raise NotImplementedError
