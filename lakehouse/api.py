"""FastAPI 路由：把存储层错误映射为可区分的 HTTP 响应。

错误映射：LakehouseError.code -> HTTP 状态码 + {"error": {"code", "detail"}}，
底层异常（如 OSError）一律包装为 500 INTERNAL_ERROR，不直接透出。
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from lakehouse.errors import ErrorCode, LakehouseError
from lakehouse.models import Column, ColumnType, TableSchema
from lakehouse.store import SnapshotStore

logger = logging.getLogger("lakehouse.api")

_STATUS_BY_CODE = {
    ErrorCode.TABLE_NOT_FOUND: 404,
    ErrorCode.VERSION_NOT_FOUND: 404,
    ErrorCode.TABLE_ALREADY_EXISTS: 409,
    ErrorCode.PRIMARY_KEY_CONFLICT: 409,
    ErrorCode.SCHEMA_INCOMPATIBLE: 422,
    ErrorCode.MISSING_FIELD: 422,
    ErrorCode.TYPE_MISMATCH: 422,
    ErrorCode.UNKNOWN_FIELD: 422,
    ErrorCode.INVALID_BATCH: 409,
}


class ColumnIn(BaseModel):
    name: str
    type: ColumnType
    nullable: bool = False


class CreateTableIn(BaseModel):
    name: str
    primary_key: str
    columns: list[ColumnIn]


class EvolveSchemaIn(BaseModel):
    primary_key: str
    columns: list[ColumnIn]


class CommitBatchIn(BaseModel):
    batch_id: str = Field(min_length=1)
    rows: list[dict[str, Any]]


def _to_schema(primary_key: str, columns: list[ColumnIn]) -> TableSchema:
    return TableSchema(
        primary_key=primary_key,
        columns=tuple(Column(c.name, c.type, c.nullable) for c in columns),
    )


def _schema_out(schema: TableSchema) -> dict[str, Any]:
    return {
        "primary_key": schema.primary_key,
        "columns": [
            {"name": c.name, "type": c.type.value, "nullable": c.nullable}
            for c in schema.columns
        ],
    }


def create_app(store: SnapshotStore) -> FastAPI:
    app = FastAPI(title="lakehouse-snapshot-store")

    @app.exception_handler(LakehouseError)
    async def lakehouse_error_handler(_: Request, exc: LakehouseError) -> JSONResponse:
        status = _STATUS_BY_CODE.get(exc.code, 400)
        return JSONResponse(
            status_code=status,
            content={"error": {"code": exc.code.value, "detail": exc.detail}},
        )

    @app.exception_handler(Exception)
    async def unknown_error_handler(_: Request, exc: Exception) -> JSONResponse:
        logger.exception("未预期的内部错误")
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "INTERNAL_ERROR", "detail": "内部错误"}},
        )

    @app.post("/tables", status_code=201)
    def create_table(body: CreateTableIn) -> dict[str, Any]:
        store.create_table(body.name, _to_schema(body.primary_key, body.columns))
        return {"table": body.name, "version": 0}

    @app.post("/tables/{name}/schema")
    def evolve_schema(name: str, body: EvolveSchemaIn) -> dict[str, Any]:
        store.evolve_schema(name, _to_schema(body.primary_key, body.columns))
        return {"table": name, "version": store.current_version(name)}

    @app.get("/tables/{name}/schema")
    def get_schema(name: str, version: int | None = None) -> dict[str, Any]:
        schema = store.get_schema(name, version)
        return {"table": name, "schema": _schema_out(schema)}

    @app.post("/tables/{name}/batches")
    def commit_batch(name: str, body: CommitBatchIn) -> dict[str, Any]:
        version = store.commit_batch(name, body.batch_id, body.rows)
        return {"table": name, "batch_id": body.batch_id, "version": version}

    @app.get("/tables/{name}/rows")
    def read_rows(name: str, version: int | None = None) -> dict[str, Any]:
        ver, schema, rows = store.read_snapshot(name, version)
        return {
            "table": name,
            "version": ver,
            "schema": _schema_out(schema),
            "rows": rows,
        }

    @app.get("/tables/{name}/version")
    def current_version(name: str) -> dict[str, Any]:
        return {"table": name, "version": store.current_version(name)}

    return app
