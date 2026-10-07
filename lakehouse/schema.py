"""结构演进规则：加列、删列、改类型的兼容性判断。

兼容规则（old -> new）：
1. 主键列不允许删除、不允许改类型。
2. 新增列：允许。历史快照按旧结构读取，新列在旧数据上读为 None。
3. 删除列：允许（主键除外）。旧快照仍保留旧结构，读旧版本不受影响。
4. 改类型：只允许"提升"：int -> float。其余（含 float -> int、任意 -> str/bool）
   一律拒绝，抛 SCHEMA_INCOMPATIBLE。
5. 列的 nullable 只能由 False 放宽为 True，不能收紧。
"""

from __future__ import annotations

from lakehouse.errors import ErrorCode, LakehouseError
from lakehouse.models import Column, ColumnType, TableSchema

# 允许的类型提升：源类型 -> 可提升到的目标类型集合
_WIDENING: dict[ColumnType, frozenset[ColumnType]] = {
    ColumnType.INT: frozenset({ColumnType.FLOAT}),
}


def is_type_widening(old: Column, new: Column) -> bool:
    """判断类型变更是否为允许的提升（当前仅 int -> float）。"""
    return new.type in _WIDENING.get(old.type, frozenset())


def check_evolution(old: TableSchema, new: TableSchema) -> None:
    """校验 old -> new 是否兼容；不兼容时抛 LakehouseError(SCHEMA_INCOMPATIBLE)。"""
    if old.primary_key != new.primary_key:
        raise LakehouseError(
            ErrorCode.SCHEMA_INCOMPATIBLE,
            f"主键不允许变更: {old.primary_key!r} -> {new.primary_key!r}",
        )

    old_cols = old.column_map()
    new_cols = new.column_map()

    if old.primary_key not in new_cols:
        raise LakehouseError(
            ErrorCode.SCHEMA_INCOMPATIBLE,
            f"主键列 {old.primary_key!r} 不允许删除",
        )

    for name, new_col in new_cols.items():
        old_col = old_cols.get(name)
        if old_col is None:
            continue  # 新增列，允许
        if new_col.type != old_col.type and not is_type_widening(old_col, new_col):
            raise LakehouseError(
                ErrorCode.SCHEMA_INCOMPATIBLE,
                f"列 {name!r} 类型不允许 {old_col.type.value} -> {new_col.type.value}"
                "（仅允许 int -> float 提升）",
            )
        if old_col.nullable and not new_col.nullable:
            raise LakehouseError(
                ErrorCode.SCHEMA_INCOMPATIBLE,
                f"列 {name!r} 不允许从可空收紧为非空",
            )
