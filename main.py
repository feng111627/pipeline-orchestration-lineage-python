"""HTTP 入口：把门面服务暴露为 REST API。

所有入口（本 API、单测、CLI 脚本）共享同一个 PipelineService 实例，
因此并发控制、幂等与状态流转规则在任何入口下结论一致。
"""

from __future__ import annotations

import os
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from pipeline.errors import (ConflictError, InvalidTransitionError,
                             NotFoundError, PipelineError, ValidationError)
from pipeline.models import (PipelineDefinition, StepDefinition,
                             run_to_dict, version_to_dict)
from pipeline.service import PipelineService

DB_PATH = os.environ.get("PIPELINE_DB", "pipeline_db.json")
service = PipelineService(DB_PATH)
app = FastAPI(title="本地数据管道编排与血缘审计服务")


class StepIn(BaseModel):
    name: str
    handler: str
    depends_on: list[str] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)


class DefinitionIn(BaseModel):
    name: str
    steps: list[StepIn]

    def to_domain(self) -> PipelineDefinition:
        return PipelineDefinition(
            name=self.name,
            steps=[StepDefinition(s.name, s.handler, s.depends_on, s.config)
                   for s in self.steps],
        )


class TriggerIn(BaseModel):
    idempotency_key: str


@app.exception_handler(NotFoundError)
def _not_found(_, exc: NotFoundError):
    raise HTTPException(status_code=404, detail=str(exc))


@app.exception_handler(ConflictError)
def _conflict(_, exc: ConflictError):
    raise HTTPException(status_code=409, detail=str(exc))


@app.exception_handler(InvalidTransitionError)
def _bad_transition(_, exc: InvalidTransitionError):
    raise HTTPException(status_code=409, detail=str(exc))


@app.exception_handler(ValidationError)
def _invalid(_, exc: ValidationError):
    raise HTTPException(status_code=422, detail=str(exc))


@app.exception_handler(PipelineError)
def _business(_, exc: PipelineError):
    raise HTTPException(status_code=400, detail=str(exc))


@app.post("/pipelines", status_code=201)
def register_pipeline(body: DefinitionIn):
    return version_to_dict(service.register_pipeline(body.to_domain()))


@app.put("/pipelines/{name}", status_code=201)
def update_pipeline(name: str, body: DefinitionIn, note: str = ""):
    if body.name != name:
        raise HTTPException(status_code=422, detail="路径与请求体的管道名不一致")
    return version_to_dict(service.update_pipeline(body.to_domain(), note=note))


@app.post("/pipelines/{name}/rollback/{version}", status_code=201)
def rollback_pipeline(name: str, version: int):
    return version_to_dict(service.rollback_pipeline(name, version))


@app.get("/pipelines/{name}/versions")
def list_versions(name: str):
    return [version_to_dict(v) for v in service.list_versions(name)]


@app.post("/pipelines/{name}/runs", status_code=201)
def trigger(name: str, body: TriggerIn):
    return run_to_dict(service.trigger(name, body.idempotency_key))


@app.post("/runs/{run_id}/resume", status_code=201)
def resume(run_id: str, body: TriggerIn):
    return run_to_dict(service.resume(run_id, body.idempotency_key))


@app.post("/runs/{run_id}/cancel")
def cancel(run_id: str):
    return run_to_dict(service.cancel(run_id))


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    return run_to_dict(service.get_run(run_id))


@app.get("/runs/{run_id}/lineage")
def lineage(run_id: str):
    from pipeline.models import lineage_to_dict
    return [lineage_to_dict(l) for l in service.lineage_of_run(run_id)]


@app.get("/runs/{run_id}/reproduce")
def reproduce(run_id: str):
    return service.reproduce(run_id)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
