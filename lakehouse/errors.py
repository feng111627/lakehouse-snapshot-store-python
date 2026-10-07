"""可区分的业务错误类型，避免把底层异常直接透出。"""

from __future__ import annotations

import enum


class ErrorCode(str, enum.Enum):
    TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
    TABLE_ALREADY_EXISTS = "TABLE_ALREADY_EXISTS"
    SCHEMA_INCOMPATIBLE = "SCHEMA_INCOMPATIBLE"
    PRIMARY_KEY_CONFLICT = "PRIMARY_KEY_CONFLICT"
    MISSING_FIELD = "MISSING_FIELD"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    UNKNOWN_FIELD = "UNKNOWN_FIELD"
    VERSION_NOT_FOUND = "VERSION_NOT_FOUND"
    INVALID_BATCH = "INVALID_BATCH"


class LakehouseError(Exception):
    """业务错误：code 可区分失败原因，detail 面向调用方可读。"""

    def __init__(self, code: ErrorCode, detail: str) -> None:
        super().__init__(f"{code.value}: {detail}")
        self.code = code
        self.detail = detail
