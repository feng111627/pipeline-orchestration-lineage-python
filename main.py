"""HTTP 入口：管道编排与血缘审计服务。"""
from fastapi import FastAPI

app = FastAPI(title="pipeline-orchestration-lineage")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
