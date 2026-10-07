"""快照存储：写入走 暂存->原子提交，读取按版本快照，读写互不阻塞。

一致性设计：
- 快照文件（snapshots/、schema_history/）一旦提交即不可变，读者永远读到完整版本。
- CURRENT 指针通过 写临时文件 + os.replace 原子切换；replace 之前的任何失败
  都不会影响已提交版本，即"回滚"就是丢弃暂存文件。
- 写路径用每表一把 threading.Lock 串行化；读路径不加锁，天然快照隔离。
- 幂等：batches.json 记录 batch_id -> (version, 请求指纹)。同一 batch_id 重复
  提交且内容一致时直接返回原版本号；内容不一致抛 INVALID_BATCH。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from pathlib import Path

from lakehouse.errors import ErrorCode, LakehouseError
from lakehouse.models import (
    Row,
    TableSchema,
    schema_from_dict,
    schema_to_dict,
)
from lakehouse.schema import check_evolution


class SnapshotStore:
    """多表本地快照存储。

    磁盘布局（每张表）：
        <root>/<table>/schema_history/000000.json   每个版本的表结构
        <root>/<table>/snapshots/000000.json        每个版本的完整数据快照（不可变）
        <root>/<table>/batches.json                 幂等键 -> {version, digest}
        <root>/<table>/CURRENT                      当前已提交版本号（原子替换）
        <root>/<table>/_staging/                    写入暂存区，提交前对读者不可见
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ---------- 路径与锁 ----------

    def _table_dir(self, name: str) -> Path:
        if not name or name in (".", "..") or "/" in name or "\\" in name:
            raise LakehouseError(ErrorCode.TABLE_NOT_FOUND, f"非法表名: {name!r}")
        return self.root / name

    def _require_table(self, name: str) -> Path:
        table_dir = self._table_dir(name)
        if not (table_dir / "CURRENT").is_file():
            raise LakehouseError(ErrorCode.TABLE_NOT_FOUND, f"表不存在: {name!r}")
        return table_dir

    def _lock_for(self, name: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(name, threading.Lock())

    # ---------- 元信息读写 ----------

    @staticmethod
    def _read_current(table_dir: Path) -> int:
        return int((table_dir / "CURRENT").read_text(encoding="utf-8").strip())

    @staticmethod
    def _atomic_write_text(path: Path, text: str) -> None:
        tmp = path.with_name(path.name + f".tmp-{uuid.uuid4().hex}")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def _load_batches(self, table_dir: Path) -> dict[str, dict]:
        path = table_dir / "batches.json"
        if not path.is_file():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_batches(self, table_dir: Path, batches: dict[str, dict]) -> None:
        self._atomic_write_text(table_dir / "batches.json", json.dumps(batches, ensure_ascii=False, indent=1))

    # ---------- 公开接口 ----------

    def create_table(self, name: str, schema: TableSchema) -> None:
        self._validate_schema(schema)
        table_dir = self._table_dir(name)
        with self._lock_for(name):
            if (table_dir / "CURRENT").is_file():
                raise LakehouseError(ErrorCode.TABLE_ALREADY_EXISTS, f"表已存在: {name!r}")
            (table_dir / "schema_history").mkdir(parents=True, exist_ok=True)
            (table_dir / "snapshots").mkdir(parents=True, exist_ok=True)
            (table_dir / "_staging").mkdir(parents=True, exist_ok=True)
            self._write_version_files(table_dir, 0, schema, [])
            self._save_batches(table_dir, {})
            self._atomic_write_text(table_dir / "CURRENT", "0")

    def evolve_schema(self, name: str, new_schema: TableSchema) -> None:
        self._validate_schema(new_schema)
        table_dir = self._require_table(name)
        with self._lock_for(name):
            version = self._read_current(table_dir)
            old_schema = self._load_schema(table_dir, version)
            check_evolution(old_schema, new_schema)
            rows = self._load_rows(table_dir, version)
            new_rows = [self._migrate_row(r, old_schema, new_schema) for r in rows]
            new_version = version + 1
            self._commit(table_dir, new_version, new_schema, new_rows)

    def commit_batch(self, name: str, batch_id: str, rows: list[Row]) -> int:
        """幂等提交一批行（按主键 upsert），返回版本号。"""
        if not batch_id:
            raise LakehouseError(ErrorCode.INVALID_BATCH, "batch_id 不能为空")
        if not isinstance(rows, list):
            raise LakehouseError(ErrorCode.INVALID_BATCH, "rows 必须是数组")
        table_dir = self._require_table(name)
        digest = hashlib.sha256(
            json.dumps(rows, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()

        with self._lock_for(name):
            batches = self._load_batches(table_dir)
            known = batches.get(batch_id)
            if known is not None:
                if known["digest"] == digest:
                    return known["version"]  # 重复提交/断点重放：直接返回原版本
                raise LakehouseError(
                    ErrorCode.INVALID_BATCH,
                    f"batch_id {batch_id!r} 已提交过不同内容，拒绝重用以避免歧义",
                )

            version = self._read_current(table_dir)
            schema = self._load_schema(table_dir, version)
            current_rows = self._load_rows(table_dir, version)

            upserts = self._validate_batch(schema, rows)
            merged = {self._pk_key(schema, r): r for r in current_rows}
            for pk, row in upserts.items():
                merged[pk] = row

            new_version = version + 1
            self._commit(table_dir, new_version, schema, list(merged.values()))
            batches[batch_id] = {"version": new_version, "digest": digest}
            self._save_batches(table_dir, batches)
            return new_version

    def read_snapshot(
        self, name: str, version: int | None = None
    ) -> tuple[int, TableSchema, list[Row]]:
        """读取某个完整版本；version 为 None 时读当前版本。"""
        table_dir = self._require_table(name)
        if version is None:
            version = self._read_current(table_dir)
        schema = self._load_schema(table_dir, version)
        rows = self._load_rows(table_dir, version)
        return version, schema, rows

    def current_version(self, name: str) -> int:
        return self._read_current(self._require_table(name))

    def get_schema(self, name: str, version: int | None = None) -> TableSchema:
        table_dir = self._require_table(name)
        if version is None:
            version = self._read_current(table_dir)
        return self._load_schema(table_dir, version)

    # ---------- 内部：版本文件 ----------

    def _write_version_files(
        self, table_dir: Path, version: int, schema: TableSchema, rows: list[Row]
    ) -> None:
        tag = f"{version:06d}"
        schema_text = json.dumps(schema_to_dict(schema), ensure_ascii=False, indent=1)
        rows_text = json.dumps(rows, ensure_ascii=False, indent=1)
        # 先落暂存区并 fsync，再原子 rename 到正式位置，读者不会看到半个文件
        for subdir, filename, text in (
            ("schema_history", f"{tag}.json", schema_text),
            ("snapshots", f"{tag}.json", rows_text),
        ):
            staging = table_dir / "_staging" / f"{subdir}-{filename}-{uuid.uuid4().hex}"
            with open(staging, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())
            os.replace(staging, table_dir / subdir / filename)

    def _commit(
        self, table_dir: Path, version: int, schema: TableSchema, rows: list[Row]
    ) -> None:
        """提交新版本：版本文件全部就位后，原子切换 CURRENT。"""
        self._write_version_files(table_dir, version, schema, rows)
        self._atomic_write_text(table_dir / "CURRENT", str(version))

    def _load_schema(self, table_dir: Path, version: int) -> TableSchema:
        path = table_dir / "schema_history" / f"{version:06d}.json"
        if not path.is_file():
            raise LakehouseError(
                ErrorCode.VERSION_NOT_FOUND, f"版本不存在: {version}"
            )
        return schema_from_dict(json.loads(path.read_text(encoding="utf-8")))

    def _load_rows(self, table_dir: Path, version: int) -> list[Row]:
        path = table_dir / "snapshots" / f"{version:06d}.json"
        if not path.is_file():
            raise LakehouseError(
                ErrorCode.VERSION_NOT_FOUND, f"版本不存在: {version}"
            )
        return json.loads(path.read_text(encoding="utf-8"))

    # ---------- 内部：校验与迁移 ----------

    @staticmethod
    def _validate_schema(schema: TableSchema) -> None:
        cols = schema.column_map()
        if len(cols) != len(schema.columns):
            raise LakehouseError(ErrorCode.SCHEMA_INCOMPATIBLE, "列名重复")
        pk = cols.get(schema.primary_key)
        if pk is None:
            raise LakehouseError(
                ErrorCode.SCHEMA_INCOMPATIBLE,
                f"主键列 {schema.primary_key!r} 不在列定义中",
            )
        if pk.nullable:
            raise LakehouseError(
                ErrorCode.SCHEMA_INCOMPATIBLE, "主键列不允许为可空"
            )

    @staticmethod
    def _pk_key(schema: TableSchema, row: Row) -> str:
        return repr(row[schema.primary_key])

    def _validate_batch(
        self, schema: TableSchema, rows: list[Row]
    ) -> dict[str, Row]:
        """校验一批行并按主键去重；同主键内容冲突抛 PRIMARY_KEY_CONFLICT。"""
        cols = schema.column_map()
        upserts: dict[str, Row] = {}
        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                raise LakehouseError(
                    ErrorCode.TYPE_MISMATCH, f"第 {i} 行不是对象: {row!r}"
                )
            for key in row:
                if key not in cols:
                    raise LakehouseError(
                        ErrorCode.UNKNOWN_FIELD,
                        f"第 {i} 行含未知字段 {key!r}（当前结构无此列）",
                    )
            for col in schema.columns:
                if col.name not in row:
                    if col.nullable:
                        continue
                    raise LakehouseError(
                        ErrorCode.MISSING_FIELD,
                        f"第 {i} 行缺少必填字段 {col.name!r}",
                    )
                self._check_type(i, col.name, row[col.name], col)
            pk = self._pk_key(schema, row)
            prev = upserts.get(pk)
            if prev is not None and prev != row:
                raise LakehouseError(
                    ErrorCode.PRIMARY_KEY_CONFLICT,
                    f"批次内主键 {schema.primary_key}={row[schema.primary_key]!r} "
                    "出现两条内容不同的记录",
                )
            upserts[pk] = row
        return upserts

    @staticmethod
    def _check_type(row_no: int, name: str, value, col) -> None:
        from lakehouse.models import ColumnType

        if value is None:
            if col.nullable:
                return
            raise LakehouseError(
                ErrorCode.TYPE_MISMATCH,
                f"第 {row_no} 行字段 {name!r} 为 null，但该列非空",
            )
        ok = {
            ColumnType.INT: lambda v: isinstance(v, int) and not isinstance(v, bool),
            ColumnType.FLOAT: lambda v: isinstance(v, (int, float))
            and not isinstance(v, bool),
            ColumnType.STR: lambda v: isinstance(v, str),
            ColumnType.BOOL: lambda v: isinstance(v, bool),
        }[col.type](value)
        if not ok:
            raise LakehouseError(
                ErrorCode.TYPE_MISMATCH,
                f"第 {row_no} 行字段 {name!r} 期望 {col.type.value}，"
                f"实际值 {value!r}（{type(value).__name__}）",
            )

    @staticmethod
    def _migrate_row(row: Row, old: TableSchema, new: TableSchema) -> Row:
        """结构演进时迁移存量行：新列补 None，删列丢弃，int->float 提升。"""
        old_cols = old.column_map()
        out: Row = {}
        for col in new.columns:
            if col.name in row and col.name in old_cols:
                value = row[col.name]
                if (
                    value is not None
                    and col.type != old_cols[col.name].type
                    and isinstance(value, int)
                    and not isinstance(value, bool)
                ):
                    value = float(value)  # int -> float 提升
                out[col.name] = value
            else:
                out[col.name] = None  # 新增列
        return out
