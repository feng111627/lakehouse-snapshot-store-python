"""场景：重复写入与失败分类 —— 幂等、断点重放、并发提交、可区分的失败原因。"""

from __future__ import annotations

import threading

import main
from lakehouse.table import TableRegistry

SCHEMA = {
    "primary_key": "id",
    "columns": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
    ],
}
ROWS = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def _write(client, batch_id, rows, schema=None):
    payload = {"batch_id": batch_id, "rows": rows}
    if schema is not None:
        payload["schema"] = schema
    return client.post("/tables/t/writes", json=payload)


def test_replay_same_batch_is_noop(client):
    """场景：重复写入 —— 同一批数据重复提交不产生重复数据"""
    r1 = _write(client, "b1", ROWS, SCHEMA)
    assert r1.status_code == 200 and r1.json()["replayed"] is False

    r2 = _write(client, "b1", ROWS, SCHEMA)
    assert r2.status_code == 200
    assert r2.json()["replayed"] is True
    assert r2.json()["version"] == r1.json()["version"]

    data = client.get("/tables/t/rows").json()
    assert data["row_count"] == 2  # 没有重复行
    assert len(client.get("/tables/t/versions").json()) == 1  # 没有多余版本


def test_replay_after_crash_resume(client):
    """场景：重复写入 —— 断点重放同一 batch_id 不丢不重"""
    _write(client, "b1", ROWS, SCHEMA)
    _write(client, "b2", [{"id": 3, "name": "c"}])
    # 模拟断点重放：把 b2 原样再发一遍
    r = _write(client, "b2", [{"id": 3, "name": "c"}])
    assert r.json()["replayed"] is True
    data = client.get("/tables/t/rows").json()
    assert data["row_count"] == 3
    assert sorted(row["id"] for row in data["rows"]) == [1, 2, 3]


def test_concurrent_same_batch_commits_once(client, tmp_path, monkeypatch):
    """场景：重复写入 —— 并发提交同一批次只生效一次"""
    registry = main.registry  # client 夹具已替换为独立 registry
    table = registry.get("t")
    results = []
    errors = []

    def submit():
        try:
            results.append(table.write("b1", ROWS, SCHEMA))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    committed = [r for r in results if not r["replayed"]]
    replayed = [r for r in results if r["replayed"]]
    assert len(committed) == 1  # 只有一个真正提交
    assert len(replayed) == 7
    assert table.read()["row_count"] == 2


def test_duplicate_key_in_batch_rejected(client):
    """场景：重复写入 —— 同批主键冲突返回可区分的 duplicate_key"""
    _write(client, "b1", ROWS, SCHEMA)
    r = _write(client, "b2", [{"id": 5, "name": "x"}, {"id": 5, "name": "y"}])
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "duplicate_key"


def test_missing_field_rejected(client):
    """场景：重复写入 —— 字段缺失返回可区分的 missing_field"""
    _write(client, "b1", ROWS, SCHEMA)
    r = _write(client, "b2", [{"id": 5}])
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "missing_field"


def test_type_mismatch_rejected(client):
    """场景：重复写入 —— 类型不匹配返回可区分的 type_mismatch"""
    _write(client, "b1", ROWS, SCHEMA)
    r = _write(client, "b2", [{"id": "not-an-int", "name": "x"}])
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "type_mismatch"


def test_failed_batch_leaves_no_trace(client):
    """场景：重复写入 —— 校验失败的批次不产生新版本，可修正后重提"""
    _write(client, "b1", ROWS, SCHEMA)
    _write(client, "b2", [{"id": 5}])  # 缺字段，失败
    assert len(client.get("/tables/t/versions").json()) == 1
    # 修正后用同一 batch_id 重提，成功生效
    r = _write(client, "b2", [{"id": 5, "name": "fixed"}])
    assert r.json()["replayed"] is False
    assert client.get("/tables/t/rows").json()["row_count"] == 3


def test_mid_commit_failure_rolls_back(client, monkeypatch):
    """场景：重复写入 —— 提交中途失败回滚到前一个完整快照"""
    import lakehouse.storage as storage

    _write(client, "b1", ROWS, SCHEMA)

    real_rename = storage.os.rename

    def boom(*args, **kwargs):
        raise OSError("模拟磁盘故障")

    monkeypatch.setattr(storage.os, "rename", boom)
    r = _write(client, "b2", [{"id": 3, "name": "c"}])
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "internal_error"  # 不透出底层 OSError
    monkeypatch.setattr(storage.os, "rename", real_rename)

    # 指针仍指向版本 1，旧数据完整可读
    data = client.get("/tables/t/rows").json()
    assert data["version"] == 1
    assert data["row_count"] == 2
    # 故障恢复后重提同一批次，正常生效
    r = _write(client, "b2", [{"id": 3, "name": "c"}])
    assert r.json()["replayed"] is False
    assert client.get("/tables/t/rows").json()["row_count"] == 3
