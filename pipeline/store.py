"""本地 JSON 存储：带进程内锁的原子读写，模拟持久层。

所有集合共享一把 RLock，保证"检查-写入"复合操作的原子性，
这是并发触发控制与状态流转校验的基础。
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from .errors import NotFoundError


class JsonStore:
    """以单个 JSON 文件持久化多个命名集合（dict[str, list[dict]]）。

    传入 ":memory:" 时不落盘，仅进程内保存（测试用）。
    """

    def __init__(self, path: str | Path = ":memory:"):
        self._memory_only = str(path) == ":memory:"
        self._path = None if self._memory_only else Path(path)
        self._lock = threading.RLock()
        self._data: dict[str, list[dict[str, Any]]] = {}
        self._load()

    def _load(self) -> None:
        if self._memory_only or not self._path.exists():
            return
        with self._lock:
            self._data = json.loads(self._path.read_text(encoding="utf-8"))

    def _flush(self) -> None:
        """原子写盘：先写临时文件再 rename，避免半写状态。"""
        if self._memory_only:
            return
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        os.replace(tmp, self._path)

    def append(self, collection: str, record: dict[str, Any]) -> None:
        with self._lock:
            self._data.setdefault(collection, []).append(record)
            self._flush()

    def all(self, collection: str) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._data.get(collection, [])]

    def find(self, collection: str, **match: Any) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._data.get(collection, [])
                    if all(r.get(k) == v for k, v in match.items())]

    def find_one(self, collection: str, **match: Any) -> dict[str, Any] | None:
        rows = self.find(collection, **match)
        return rows[0] if rows else None

    def update_one(self, collection: str, match: dict[str, Any],
                   patch: dict[str, Any]) -> dict[str, Any]:
        """更新第一条匹配记录并返回更新后的副本；无匹配抛 NotFoundError。"""
        with self._lock:
            for r in self._data.get(collection, []):
                if all(r.get(k) == v for k, v in match.items()):
                    r.update(patch)
                    self._flush()
                    return dict(r)
        raise NotFoundError(f"{collection} 中无匹配记录: {match}")

    @property
    def lock(self) -> threading.RLock:
        return self._lock
