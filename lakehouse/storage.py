"""本地磁盘快照存储：版本化快照目录 + 原子指针切换 + 失败回滚。

一致性保证：
- 快照目录 snapshots/<version>/ 一旦从 .tmp 改名就位，就永远不再修改（不可变）。
- CURRENT 指针文件通过 "写临时文件 + os.replace" 原子切换，
  读取方任意时刻只能看到某一个完整版本号，不会读到写了一半的状态。
- 提交中途任何失败都会清理临时目录/临时指针，CURRENT 保持指向旧版本，
  即回滚到前一个完整快照，已落地数据不受影响。
"""

from __future__ import annotations

import json
import os
import shutil
import threading
from pathlib import Path

from lakehouse.errors import SnapshotNotFoundError


class SnapshotStore:
    """管理单张表的磁盘快照。

    目录布局：
        <root>/<table>/
            snapshots/<version>/data.jsonl   # 该版本的完整数据（不可变）
            snapshots/<version>/meta.json    # 版本元信息（含已应用 batch_id、结构版本）
            schemas/<schema_version>.json    # 结构历史
            CURRENT                          # 当前版本号指针文件（原子替换）
    """

    def __init__(self, root: Path, table: str) -> None:
        self._root = Path(root)
        self._table = table
        self.lock = threading.Lock()

    # ---- 路径 ----
    @property
    def table_dir(self) -> Path:
        return self._root / self._table

    @property
    def _snapshots_dir(self) -> Path:
        return self.table_dir / "snapshots"

    @property
    def _schemas_dir(self) -> Path:
        return self.table_dir / "schemas"

    @property
    def _pointer(self) -> Path:
        return self.table_dir / "CURRENT"

    def _snapshot_dir(self, version: int) -> Path:
        return self._snapshots_dir / str(version)

    # ---- 快照元信息 ----
    def current_version(self) -> int:
        """返回当前版本号；表还没有任何快照时返回 0。"""
        try:
            return int(self._pointer.read_text().strip())
        except (FileNotFoundError, ValueError):
            return 0

    def load_meta(self, version: int) -> dict:
        path = self._snapshot_dir(version) / "meta.json"
        if not path.is_file():
            raise SnapshotNotFoundError(
                f"表 '{self._table}' 不存在版本 {version} 的快照",
                detail={"table": self._table, "version": version},
            )
        return json.loads(path.read_text())

    def list_versions(self) -> list[int]:
        if not self._snapshots_dir.is_dir():
            return []
        versions = []
        for entry in self._snapshots_dir.iterdir():
            if entry.is_dir() and entry.name.isdigit():
                versions.append(int(entry.name))
        return sorted(versions)

    # ---- 结构历史 ----
    def load_schema(self, schema_version: int) -> dict:
        path = self._schemas_dir / f"{schema_version}.json"
        if not path.is_file():
            raise SnapshotNotFoundError(
                f"表 '{self._table}' 不存在结构版本 {schema_version}",
                detail={"table": self._table, "schema_version": schema_version},
            )
        return json.loads(path.read_text())

    def latest_schema_version(self) -> int:
        if not self._schemas_dir.is_dir():
            return 0
        versions = [
            int(p.stem) for p in self._schemas_dir.glob("*.json") if p.stem.isdigit()
        ]
        return max(versions, default=0)

    # ---- 数据读取 ----
    def read_rows(self, version: int) -> list[dict]:
        path = self._snapshot_dir(version) / "data.jsonl"
        if not path.is_file():
            raise SnapshotNotFoundError(
                f"表 '{self._table}' 不存在版本 {version} 的快照",
                detail={"table": self._table, "version": version},
            )
        rows = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    # ---- 原子提交 ----
    def commit(self, rows: list[dict], meta: dict, schema: dict | None) -> int:
        """把新快照写入临时目录，成功后原子切换指针；失败清理临时目录并回滚。

        调用方必须持有 self.lock。返回新版本号。
        """
        new_version = self.current_version() + 1
        tmp_dir = self._snapshots_dir / f"{new_version}.tmp"
        tmp_pointer = self.table_dir / "CURRENT.tmp"
        try:
            self._snapshots_dir.mkdir(parents=True, exist_ok=True)
            self._schemas_dir.mkdir(parents=True, exist_ok=True)
            tmp_dir.mkdir()

            # 结构演进：传了新结构就落一个新的结构版本，否则沿用最新版本。
            if schema is not None:
                schema_version = self.latest_schema_version() + 1
                (self._schemas_dir / f"{schema_version}.json").write_text(
                    json.dumps(schema, ensure_ascii=False, indent=2)
                )
            else:
                schema_version = self.latest_schema_version()

            meta = dict(meta, version=new_version, schema_version=schema_version,
                        row_count=len(rows))

            data_path = tmp_dir / "data.jsonl"
            with data_path.open("w", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            (tmp_dir / "meta.json").write_text(
                json.dumps(meta, ensure_ascii=False, indent=2)
            )

            # 快照目录就位（rename 在同一文件系统内是原子的）。
            os.rename(tmp_dir, self._snapshot_dir(new_version))

            # 最后一步才切换指针：此前任何失败都不会影响当前可读版本。
            tmp_pointer.write_text(str(new_version))
            os.replace(tmp_pointer, self._pointer)
            return new_version
        except BaseException:
            # 回滚：清理临时产物，指针保持指向旧版本。
            shutil.rmtree(tmp_dir, ignore_errors=True)
            tmp_pointer.unlink(missing_ok=True)
            raise
