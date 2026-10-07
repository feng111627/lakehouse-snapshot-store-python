"""表结构定义与演进兼容规则。

兼容规则（从旧结构 old 演进到新结构 new）：
1. 主键不允许变更（名字或类型变了都拒绝）。
2. 新增列：允许，但必须 nullable（历史行读出来补 null）。
3. 删除列：允许，旧快照仍按写入时的结构读，新快照不再返回该列。
4. 类型变更：只允许 _TYPE_PROMOTIONS 里声明的提升（目前 int -> float），
   其余一律抛 SchemaIncompatibleError。
5. 已保留列的 nullable 只能从 False 放宽为 True，不能收紧。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from lakehouse.errors import (
    MissingFieldError,
    SchemaIncompatibleError,
    TypeMismatchError,
)

# 允许的类型提升：int -> float；其余类型变更一律拒绝。
_TYPE_PROMOTIONS: dict[str, set[str]] = {
    "int": {"float"},
}

SUPPORTED_TYPES = {"int", "float", "string", "bool"}

# Python 类型 -> 结构类型名；注意 bool 是 int 的子类，必须先判 bool。
_PY_TYPE_NAMES: list[tuple[type, str]] = [
    (bool, "bool"),
    (int, "int"),
    (float, "float"),
    (str, "string"),
]


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = False

    def to_dict(self) -> dict:
        return {"name": self.name, "type": self.type, "nullable": self.nullable}

    @staticmethod
    def from_dict(data: dict) -> "Column":
        return Column(
            name=str(data["name"]),
            type=str(data["type"]),
            nullable=bool(data.get("nullable", False)),
        )


@dataclass(frozen=True)
class Schema:
    primary_key: str
    columns: tuple[Column, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict:
        return {
            "primary_key": self.primary_key,
            "columns": [c.to_dict() for c in self.columns],
        }

    @staticmethod
    def from_dict(data: dict) -> "Schema":
        if not isinstance(data, dict) or "primary_key" not in data or "columns" not in data:
            raise SchemaIncompatibleError(
                "结构定义必须包含 primary_key 和 columns",
                detail={"received": data},
            )
        columns = tuple(Column.from_dict(c) for c in data["columns"])
        schema = Schema(primary_key=str(data["primary_key"]), columns=columns)
        schema._check_self()
        return schema

    def _check_self(self) -> None:
        names = [c.name for c in self.columns]
        if self.primary_key not in names:
            raise SchemaIncompatibleError(
                f"主键 '{self.primary_key}' 不在列定义中",
                detail={"columns": names},
            )
        if len(set(names)) != len(names):
            raise SchemaIncompatibleError("列名重复", detail={"columns": names})
        for col in self.columns:
            if col.type not in SUPPORTED_TYPES:
                raise SchemaIncompatibleError(
                    f"列 '{col.name}' 的类型 '{col.type}' 不受支持",
                    detail={"supported": sorted(SUPPORTED_TYPES)},
                )
        pk = self.column(self.primary_key)
        if pk is not None and pk.nullable:
            raise SchemaIncompatibleError("主键列不允许为 nullable")

    def column(self, name: str) -> Column | None:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def check_compatible(self, new: "Schema") -> None:
        """校验从 self 演进到 new 是否兼容；不兼容抛 SchemaIncompatibleError。"""
        if new.primary_key != self.primary_key:
            raise SchemaIncompatibleError(
                "主键不允许变更",
                detail={"old": self.primary_key, "new": new.primary_key},
            )
        old_pk = self.column(self.primary_key)
        new_pk = new.column(new.primary_key)
        if old_pk is not None and new_pk is not None and old_pk.type != new_pk.type:
            raise SchemaIncompatibleError(
                "主键类型不允许变更",
                detail={"old": old_pk.type, "new": new_pk.type},
            )
        for new_col in new.columns:
            old_col = self.column(new_col.name)
            if old_col is None:
                # 新增列：必须 nullable，历史行才能补 null。
                if not new_col.nullable:
                    raise SchemaIncompatibleError(
                        f"新增列 '{new_col.name}' 必须声明为 nullable，"
                        "否则历史快照无法兼容",
                        detail={"column": new_col.to_dict()},
                    )
                continue
            if old_col.type != new_col.type:
                allowed = _TYPE_PROMOTIONS.get(old_col.type, set())
                if new_col.type not in allowed:
                    raise SchemaIncompatibleError(
                        f"列 '{new_col.name}' 的类型不允许从 "
                        f"'{old_col.type}' 变更为 '{new_col.type}'",
                        detail={
                            "column": new_col.name,
                            "old_type": old_col.type,
                            "new_type": new_col.type,
                            "allowed_promotions": sorted(allowed),
                        },
                    )
            if old_col.nullable and not new_col.nullable:
                raise SchemaIncompatibleError(
                    f"列 '{new_col.name}' 不允许从 nullable 收紧为非空",
                    detail={"column": new_col.name},
                )

    def validate_row(self, row: dict) -> None:
        """校验单行：主键存在、必填字段齐全、类型匹配。"""
        pk = self.primary_key
        if pk not in row or row[pk] is None:
            raise MissingFieldError(
                f"缺少主键字段 '{pk}'",
                detail={"field": pk, "row": row},
            )
        for col in self.columns:
            if col.name not in row or row[col.name] is None:
                if not col.nullable:
                    raise MissingFieldError(
                        f"缺少必填字段 '{col.name}'",
                        detail={"field": col.name, "row": row},
                    )
                continue
            self._check_type(col, row[col.name])

    @staticmethod
    def _check_type(col: Column, value: object) -> None:
        actual = next((name for py, name in _PY_TYPE_NAMES if isinstance(value, py)), None)
        if actual == col.type:
            return
        # 写入侧同样允许声明过的类型提升（如列是 float，写入 int）。
        if actual is not None and col.type in _TYPE_PROMOTIONS.get(actual, set()):
            return
        raise TypeMismatchError(
            f"字段 '{col.name}' 期望类型 '{col.type}'，实际为 '{actual}'",
            detail={"field": col.name, "expected": col.type, "actual": actual},
        )
