"""编排执行器：依赖排序推进、失败传播、取消、并发与幂等控制。"""
from __future__ import annotations

from typing import Any, Callable

from .models import RunRecord
from .store import Store

# 步骤处理器签名：接收 (step_config, run_inputs, upstream_outputs)，返回输出 dict。
Handler = Callable[[dict, dict, dict], dict]


class Executor:
    def __init__(self, store: Store) -> None:
        self._store = store
        self._handlers: dict[str, Handler] = {}
        raise NotImplementedError

    def register_handler(self, name: str, handler: Handler) -> None:
        raise NotImplementedError

    def trigger(self, pipeline_id: str, inputs: dict[str, Any] | None = None,
                idempotency_key: str | None = None) -> RunRecord:
        """触发一次运行。同一管道同时只允许一个活动运行；幂等键去重。"""
        raise NotImplementedError

    def cancel(self, run_id: str) -> RunRecord:
        """取消运行：运行中步骤完成后不再调度新步骤，未开始步骤标记 CANCELLED。"""
        raise NotImplementedError

    def get_run(self, run_id: str) -> RunRecord:
        raise NotImplementedError
