"""业务错误类型：所有写入/结构失败都以可区分的错误码抛出，不透出底层异常。"""

from __future__ import annotations


class LakehouseError(Exception):
    """所有业务错误的基类。"""

    code: str = "internal_error"
    http_status: int = 500

    def __init__(self, message: str = "", *, detail: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}


class TableNotFoundError(LakehouseError):
    code = "table_not_found"
    http_status = 404


class SnapshotNotFoundError(LakehouseError):
    code = "snapshot_not_found"
    http_status = 404


class DuplicateKeyError(LakehouseError):
    code = "duplicate_key"
    http_status = 409


class MissingFieldError(LakehouseError):
    code = "missing_field"
    http_status = 422


class TypeMismatchError(LakehouseError):
    code = "type_mismatch"
    http_status = 422


class SchemaIncompatibleError(LakehouseError):
    code = "schema_incompatible"
    http_status = 422


class SchemaRequiredError(LakehouseError):
    code = "schema_required"
    http_status = 422
