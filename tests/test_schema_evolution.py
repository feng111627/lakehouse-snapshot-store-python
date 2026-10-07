"""业务场景二：表结构演进。

覆盖：新增列、删除列、类型提升（int->float）、非法类型变更、
主键保护，以及"历史快照按写入时的结构读取、不被后续写入改写"。
"""

import pytest

from lakehouse import Column, ColumnType, TableSchema
from lakehouse.errors import ErrorCode, LakehouseError


def _schema(pk="id", cols=()):
    return TableSchema(pk, cols)


class TestSchemaEvolution:
    """结构变更的兼容规则与历史快照隔离。"""

    def test_新增列_允许且旧数据读为null(self, store_with_table):
        store = store_with_table
        store.commit_batch("users", "b1", [{"id": 1, "name": "a"}])
        store.evolve_schema(
            "users",
            _schema("id", (
                Column("id", ColumnType.INT),
                Column("name", ColumnType.STR),
                Column("email", ColumnType.STR, nullable=True),
            )),
        )
        _, _, rows = store.read_snapshot("users")
        assert rows == [{"id": 1, "name": "a", "email": None}]

    def test_删除列_允许且旧版本仍按旧结构读取(self, store_with_table):
        store = store_with_table
        v1 = store.commit_batch("users", "b1", [{"id": 1, "name": "a"}])
        store.evolve_schema("users", _schema("id", (Column("id", ColumnType.INT),)))
        # 新版本没有 name 列
        _, new_schema, new_rows = store.read_snapshot("users")
        assert "name" not in new_schema.column_map()
        assert new_rows == [{"id": 1}]
        # 旧版本仍按写入时的结构读取，内容不被改写
        _, old_schema, old_rows = store.read_snapshot("users", version=v1)
        assert "name" in old_schema.column_map()
        assert old_rows == [{"id": 1, "name": "a"}]

    def test_类型提升_int到float_允许且存量值被提升(self, store_with_table):
        store = store_with_table
        store.commit_batch("users", "b1", [{"id": 1, "name": "a"}])
        store.evolve_schema(
            "users",
            _schema("id", (
                Column("id", ColumnType.INT),
                Column("name", ColumnType.STR),
                Column("score", ColumnType.INT, nullable=True),
            )),
        )
        store.commit_batch("users", "b2", [{"id": 1, "name": "a", "score": 7}])
        store.evolve_schema(
            "users",
            _schema("id", (
                Column("id", ColumnType.INT),
                Column("name", ColumnType.STR),
                Column("score", ColumnType.FLOAT, nullable=True),
            )),
        )
        _, _, rows = store.read_snapshot("users")
        assert rows[0]["score"] == 7.0 and isinstance(rows[0]["score"], float)

    @pytest.mark.parametrize(
        "old,new",
        [
            (ColumnType.FLOAT, ColumnType.INT),
            (ColumnType.STR, ColumnType.INT),
            (ColumnType.INT, ColumnType.STR),
            (ColumnType.BOOL, ColumnType.INT),
        ],
    )
    def test_非法类型变更_拒绝并报结构不兼容(self, store, old, new):
        store.create_table(
            "t", _schema("id", (Column("id", ColumnType.INT), Column("v", old)))
        )
        with pytest.raises(LakehouseError) as exc:
            store.evolve_schema(
                "t", _schema("id", (Column("id", ColumnType.INT), Column("v", new)))
            )
        assert exc.value.code == ErrorCode.SCHEMA_INCOMPATIBLE

    def test_删除主键列_拒绝(self, store_with_table):
        with pytest.raises(LakehouseError) as exc:
            store_with_table.evolve_schema(
                "users", _schema("id", (Column("name", ColumnType.STR),))
            )
        assert exc.value.code == ErrorCode.SCHEMA_INCOMPATIBLE

    def test_变更主键_拒绝(self, store_with_table):
        with pytest.raises(LakehouseError) as exc:
            store_with_table.evolve_schema(
                "users",
                _schema("name", (
                    Column("id", ColumnType.INT),
                    Column("name", ColumnType.STR),
                )),
            )
        assert exc.value.code == ErrorCode.SCHEMA_INCOMPATIBLE

    def test_演进后新写入不影响旧快照(self, store_with_table):
        store = store_with_table
        v1 = store.commit_batch("users", "b1", [{"id": 1, "name": "a"}])
        store.evolve_schema(
            "users",
            _schema("id", (
                Column("id", ColumnType.INT),
                Column("name", ColumnType.STR),
                Column("age", ColumnType.INT, nullable=True),
            )),
        )
        store.commit_batch("users", "b2", [{"id": 2, "name": "b", "age": 30}])
        # 旧快照逐字节不变
        _, _, old_rows = store.read_snapshot("users", version=v1)
        assert old_rows == [{"id": 1, "name": "a"}]
        # 演进失败时版本不回退也不前进
        with pytest.raises(LakehouseError):
            store.evolve_schema(
                "users",
                _schema("id", (Column("id", ColumnType.STR), Column("name", ColumnType.STR))),
            )
        _, _, old_rows2 = store.read_snapshot("users", version=v1)
        assert old_rows2 == [{"id": 1, "name": "a"}]
