"""HTTP 入口：管道编排与血缘审计服务。

所有入口共享同一套 Store/Registry/Executor/LineageService，
因此编排执行、失败传播与历史复现在各入口下结论一致。
"""
from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from pipeline.errors import (
    ConflictError,
    InvalidStateError,
    NotFoundError,
    PipelineError,
    ValidationError,
)
from pipeline.executor import Executor
from pipeline.lineage import LineageService
from pipeline.models import StepDef
from pipeline.registry import Registry
from pipeline.store import Store

DATA_DIR = os.environ.get("PIPELINE_DATA_DIR", "./data")

store = Store(DATA_DIR)
registry = Registry(store)
executor = Executor(store)
lineage = LineageService(store)

app = FastAPI(title="pipeline-orchestration-lineage")


@app.exception_handler(PipelineError)
def _pipeline_error_handler(request, exc: PipelineError):  # noqa: ANN001
    status = {NotFoundError: 404, ConflictError: 409,
              InvalidStateError: 409, ValidationError: 422}.get(type(exc), 400)
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=status, content={"detail": str(exc)})


# ---- 请求模型 ----

class StepIn(BaseModel):
    name: str
    depends_on: list[str] = []
    handler: str = "noop"
    config: dict[str, Any] = {}


class CreateVersionIn(BaseModel):
    steps: list[StepIn]
    comment: str = ""


class RollbackIn(BaseModel):
    to_version: int
    comment: str = ""


class TriggerIn(BaseModel):
    inputs: dict[str, Any] = {}
    idempotency_key: str | None = None


# ---- 管道定义 ----

@app.post("/pipelines/{pipeline_id}/versions", status_code=201)
def create_version(pipeline_id: str, body: CreateVersionIn) -> dict:
    steps = [StepDef(name=s.name, depends_on=tuple(s.depends_on),
                     handler=s.handler, config=s.config) for s in body.steps]
    v = registry.create_version(pipeline_id, steps, comment=body.comment)
    return {"pipeline_id": v.pipeline_id, "version": v.version, "created_at": v.created_at}


@app.post("/pipelines/{pipeline_id}/rollback", status_code=201)
def rollback(pipeline_id: str, body: RollbackIn) -> dict:
    v = registry.rollback(pipeline_id, body.to_version, comment=body.comment)
    return {"pipeline_id": v.pipeline_id, "version": v.version, "comment": v.comment}


@app.get("/pipelines/{pipeline_id}/versions/{version}")
def get_version(pipeline_id: str, version: int) -> dict:
    v = registry.get(pipeline_id, version)
    return {"pipeline_id": v.pipeline_id, "version": v.version,
            "steps": [{"name": s.name, "depends_on": list(s.depends_on),
                       "handler": s.handler, "config": s.config} for s in v.steps],
            "created_at": v.created_at, "comment": v.comment}


# ---- 运行 ----

def _run_view(run) -> dict:  # noqa: ANN001
    return {
        "run_id": run.run_id, "pipeline_id": run.pipeline_id,
        "pipeline_version": run.pipeline_version,
        "idempotency_key": run.idempotency_key, "status": run.status.value,
        "inputs": run.inputs, "created_at": run.created_at,
        "started_at": run.started_at, "finished_at": run.finished_at,
        "steps": {n: {"status": sr.status.value,
                      "skip_reason": sr.skip_reason.value if sr.skip_reason else None,
                      "error": sr.error, "outputs": sr.outputs}
                  for n, sr in run.step_runs.items()},
    }


@app.post("/pipelines/{pipeline_id}/runs", status_code=201)
def trigger(pipeline_id: str, body: TriggerIn) -> dict:
    run = executor.trigger(pipeline_id, inputs=body.inputs,
                           idempotency_key=body.idempotency_key)
    return _run_view(run)


@app.get("/runs/{run_id}")
def get_run(run_id: str) -> dict:
    return _run_view(executor.get_run(run_id))


@app.post("/runs/{run_id}/cancel")
def cancel(run_id: str) -> dict:
    return _run_view(executor.cancel(run_id))


# ---- 血缘与复现 ----

@app.get("/runs/{run_id}/lineage")
def get_lineage(run_id: str) -> dict:
    records = lineage.lineage_of_run(run_id)
    return {"run_id": run_id, "records": [
        {"step_name": r.step_name, "pipeline_version": r.pipeline_version,
         "inputs": r.inputs, "outputs": r.outputs, "recorded_at": r.recorded_at}
        for r in records]}


@app.get("/runs/{run_id}/replay")
def replay(run_id: str) -> dict:
    report = lineage.replay(run_id)
    # LineageRecord 不可直接 JSON 序列化，转换为 dict
    for step in report["steps"]:
        rec = step.get("lineage")
        if rec is not None:
            step["lineage"] = {"inputs": rec.inputs, "outputs": rec.outputs,
                               "recorded_at": rec.recorded_at}
    return report


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
