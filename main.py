"""本地湖仓表读写服务：FastAPI 入口。"""

from __future__ import annotations

import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from lakehouse.errors import LakehouseError
from lakehouse.table import TableRegistry

DATA_DIR = Path(os.environ.get("LAKEHOUSE_DATA_DIR", "data"))

app = FastAPI(title="lakehouse-snapshot-store")
registry = TableRegistry(DATA_DIR)


class WriteRequest(BaseModel):
    model_config = {"populate_by_name": True}

    batch_id: str = Field(min_length=1)
    rows: list[dict] = Field(default_factory=list)
    table_schema: dict | None = Field(default=None, alias="schema")


@app.exception_handler(LakehouseError)
async def lakehouse_error_handler(_: Request, exc: LakehouseError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.http_status,
        content={"error": {"code": exc.code, "message": exc.message, "detail": exc.detail}},
    )


@app.exception_handler(Exception)
async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
    # 不把底层异常透给调用方，只返回可区分的错误码。
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "internal_error", "message": "内部错误", "detail": {}}},
    )


@app.post("/tables/{name}/writes", status_code=200)
def write_rows(name: str, req: WriteRequest) -> dict:
    """幂等写入一批数据；可携带新结构完成结构演进。"""
    return registry.get(name).write(req.batch_id, req.rows, req.table_schema)


@app.get("/tables/{name}/rows")
def read_rows(name: str, version: int | None = None) -> dict:
    """读取某个快照（默认当前版本）；历史快照按落地时的结构返回。"""
    return registry.get(name).read(version)


@app.get("/tables/{name}/versions")
def list_versions(name: str) -> list[dict]:
    return registry.get(name).versions()


@app.get("/tables/{name}/schema")
def get_schema(name: str, version: int | None = None) -> dict:
    return registry.get(name).schema_at(version)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
