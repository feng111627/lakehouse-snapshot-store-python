"""表结构与数据行的类型定义。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class ColumnType(str, enum.Enum):
    INT = "int"
    FLOAT = "float"
    STR = "str"
    BOOL = "bool"


@dataclass(frozen=True)
class Column:
    name: str
    type: ColumnType
    nullable: bool = False


@dataclass(frozen=True)
class TableSchema:
    """某一版本的表结构。主键列必须存在且非空。"""

    primary_key: str
    columns: tuple[Column, ...] = field(default_factory=tuple)

    def column_map(self) -> dict[str, Column]:
        return {c.name: c for c in self.columns}


Row = dict[str, Any]


def schema_to_dict(schema: TableSchema) -> dict[str, Any]:
    return {
        "primary_key": schema.primary_key,
        "columns": [
            {"name": c.name, "type": c.type.value, "nullable": c.nullable}
            for c in schema.columns
        ],
    }


def schema_from_dict(data: dict[str, Any]) -> TableSchema:
    return TableSchema(
        primary_key=data["primary_key"],
        columns=tuple(
            Column(
                name=c["name"],
                type=ColumnType(c["type"]),
                nullable=bool(c.get("nullable", False)),
            )
            for c in data["columns"]
        ),
    )
