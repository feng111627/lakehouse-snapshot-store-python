"""表对象：幂等写入、结构演进、快照读取。

幂等规则：
- 每批写入必须携带 batch_id；同一 batch_id 重复提交/断点重放/并发提交，
  只有第一个生效，后续返回首次提交的结果（replayed=True），不产生重复数据。
- 写锁保证同一时刻只有一个提交在演进版本，并发重放在锁内被识别。
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path

from lakehouse.errors import (
    DuplicateKeyError,
    SchemaRequiredError,
    TableNotFoundError,
)
from lakehouse.schema import Schema
from lakehouse.storage import SnapshotStore


class Table:
    def __init__(self, root: Path, name: str) -> None:
        self._name = name
        self._store = SnapshotStore(root, name)

    @property
    def name(self) -> str:
        return self._name

    def write(self, batch_id: str, rows: list[dict], schema: dict | None = None) -> dict:
        """幂等写入一批数据；返回提交结果（版本号、是否重放）。"""
        if not batch_id:
            raise SchemaRequiredError("写入必须携带非空 batch_id 以保证幂等")
        with self._store.lock:
            current = self._store.current_version()

            # 幂等检查：batch_id 已应用过，直接返回首次提交的结果。
            if current > 0:
                meta = self._store.load_meta(current)
                if batch_id in meta.get("batch_ids", []):
                    return {
                        "table": self._name,
                        "version": current,
                        "schema_version": meta["schema_version"],
                        "batch_id": batch_id,
                        "rows_written": 0,
                        "replayed": True,
                    }

            # 确定本次写入生效的结构。
            if current == 0:
                if schema is None:
                    raise SchemaRequiredError(
                        "首次写入必须提供表结构",
                        detail={"table": self._name},
                    )
                effective = Schema.from_dict(schema)
                new_schema_dict = schema
                old_rows: list[dict] = []
                batch_ids: list[str] = []
            else:
                old_schema = Schema.from_dict(
                    self._store.load_schema(self._store.load_meta(current)["schema_version"])
                )
                if schema is not None:
                    effective = Schema.from_dict(schema)
                    old_schema.check_compatible(effective)
                    new_schema_dict = schema
                else:
                    effective = old_schema
                    new_schema_dict = None
                old_rows = self._store.read_rows(current)
                batch_ids = list(self._store.load_meta(current).get("batch_ids", []))

            # 校验本批数据：主键冲突 / 字段缺失 / 类型不匹配分别报错。
            seen: set = set()
            for row in rows:
                effective.validate_row(row)
                pk = row[effective.primary_key]
                if pk in seen:
                    raise DuplicateKeyError(
                        f"同一批次内主键 '{pk}' 重复",
                        detail={"primary_key": pk, "batch_id": batch_id},
                    )
                seen.add(pk)

            # 归一化：让快照内所有行都符合生效结构（新列补 null、删列剔除），
            # 保证每个快照按自己落地时的结构即可自洽读取。
            merged = {row[effective.primary_key]: row for row in old_rows}
            for row in rows:
                merged[row[effective.primary_key]] = row
            normalized = [
                self._normalize(row, effective) for row in merged.values()
            ]
            normalized.sort(key=lambda r: r[effective.primary_key])

            version = self._store.commit(
                normalized,
                meta={
                    "batch_id": batch_id,
                    "batch_ids": batch_ids + [batch_id],
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
                schema=new_schema_dict,
            )
            return {
                "table": self._name,
                "version": version,
                "schema_version": self._store.load_meta(version)["schema_version"],
                "batch_id": batch_id,
                "rows_written": len(rows),
                "replayed": False,
            }

    @staticmethod
    def _normalize(row: dict, schema: Schema) -> dict:
        out = {}
        for col in schema.columns:
            out[col.name] = row.get(col.name)  # 缺失的 nullable 列补 None
        return out

    def read(self, version: int | None = None) -> dict:
        """读取某个快照；不传版本则读当前版本。历史快照按落地时的结构返回。"""
        if version is None:
            version = self._store.current_version()
            if version == 0:
                raise TableNotFoundError(
                    f"表 '{self._name}' 还没有任何快照",
                    detail={"table": self._name},
                )
        meta = self._store.load_meta(version)
        return {
            "table": self._name,
            "version": version,
            "schema_version": meta["schema_version"],
            "schema": self._store.load_schema(meta["schema_version"]),
            "rows": self._store.read_rows(version),
            "row_count": meta["row_count"],
            "created_at": meta["created_at"],
        }

    def versions(self) -> list[dict]:
        out = []
        for v in self._store.list_versions():
            meta = self._store.load_meta(v)
            out.append({
                "version": v,
                "schema_version": meta["schema_version"],
                "batch_id": meta["batch_id"],
                "row_count": meta["row_count"],
                "created_at": meta["created_at"],
            })
        return out

    def schema_at(self, version: int | None = None) -> dict:
        if version is None:
            version = self._store.current_version()
            if version == 0:
                raise TableNotFoundError(
                    f"表 '{self._name}' 还没有任何快照",
                    detail={"table": self._name},
                )
        meta = self._store.load_meta(version)
        return {
            "table": self._name,
            "version": version,
            "schema_version": meta["schema_version"],
            "schema": self._store.load_schema(meta["schema_version"]),
        }


class TableRegistry:
    """按名字缓存 Table，保证同一张表共享同一把写锁。"""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._tables: dict[str, Table] = {}
        self._lock = threading.Lock()

    def get(self, name: str) -> Table:
        with self._lock:
            table = self._tables.get(name)
            if table is None:
                table = Table(self._root, name)
                self._tables[name] = table
            return table
