"""场景：结构演进 —— 加列、删列、类型提升的兼容规则与历史快照读取。"""

from __future__ import annotations

SCHEMA_V1 = {
    "primary_key": "id",
    "columns": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "score", "type": "int", "nullable": True},
    ],
}

# 加列（nullable）+ score 从 int 提升为 float：兼容
SCHEMA_V2 = {
    "primary_key": "id",
    "columns": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "score", "type": "float", "nullable": True},
        {"name": "email", "type": "string", "nullable": True},
    ],
}

# 删掉 score 列：兼容
SCHEMA_V3 = {
    "primary_key": "id",
    "columns": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
    ],
}


def _write(client, batch_id, rows, schema=None):
    payload = {"batch_id": batch_id, "rows": rows}
    if schema is not None:
        payload["schema"] = schema
    return client.post("/tables/t/writes", json=payload)


def test_add_nullable_column_allowed(client):
    """场景：结构演进 —— 新增 nullable 列允许，历史行读出来补 null"""
    _write(client, "b1", [{"id": 1, "name": "a", "score": 10}], SCHEMA_V1)
    r = _write(client, "b2", [{"id": 2, "name": "b", "score": 20, "email": "b@x"}], SCHEMA_V2)
    assert r.status_code == 200

    cur = client.get("/tables/t/rows").json()
    old_row = next(row for row in cur["rows"] if row["id"] == 1)
    assert old_row["email"] is None  # 历史行新列补 null
    assert cur["schema_version"] == 2


def test_add_non_nullable_column_rejected(client):
    """场景：结构演进 —— 新增非空列拒绝，返回 schema_incompatible"""
    _write(client, "b1", [{"id": 1, "name": "a"}], SCHEMA_V1)
    bad = {
        "primary_key": "id",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "score", "type": "int", "nullable": True},
            {"name": "email", "type": "string"},  # 非 nullable
        ],
    }
    r = _write(client, "b2", [{"id": 2, "name": "b", "email": "b@x"}], bad)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "schema_incompatible"


def test_drop_column_allowed(client):
    """场景：结构演进 —— 删列允许，新快照不再返回该列"""
    _write(client, "b1", [{"id": 1, "name": "a", "score": 10}], SCHEMA_V1)
    r = _write(client, "b2", [{"id": 2, "name": "b"}], SCHEMA_V3)
    assert r.status_code == 200
    cur = client.get("/tables/t/rows").json()
    assert all("score" not in row for row in cur["rows"])


def test_type_promotion_allowed(client):
    """场景：结构演进 —— int 提升为 float 允许，历史 int 值仍可读"""
    _write(client, "b1", [{"id": 1, "name": "a", "score": 10}], SCHEMA_V1)
    r = _write(client, "b2", [{"id": 2, "name": "b", "score": 9.5}], SCHEMA_V2)
    assert r.status_code == 200
    cur = client.get("/tables/t/rows").json()
    scores = {row["id"]: row["score"] for row in cur["rows"]}
    assert scores == {1: 10, 2: 9.5}


def test_incompatible_type_change_rejected(client):
    """场景：结构演进 —— string 改 int 等不兼容变更被拒绝"""
    _write(client, "b1", [{"id": 1, "name": "a"}], SCHEMA_V1)
    bad = {
        "primary_key": "id",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "int"},  # string -> int 不允许
        ],
    }
    r = _write(client, "b2", [{"id": 2, "name": 3}], bad)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "schema_incompatible"


def test_primary_key_change_rejected(client):
    """场景：结构演进 —— 主键变更被拒绝"""
    _write(client, "b1", [{"id": 1, "name": "a"}], SCHEMA_V1)
    bad = {
        "primary_key": "name",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ],
    }
    r = _write(client, "b2", [{"id": 2, "name": "b"}], bad)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "schema_incompatible"


def test_historical_snapshot_keeps_old_schema(client):
    """场景：结构演进 —— 历史快照按写入时的结构读取，后续演进不影响旧内容"""
    _write(client, "b1", [{"id": 1, "name": "a", "score": 10}], SCHEMA_V1)
    _write(client, "b2", [{"id": 2, "name": "b", "score": 20, "email": "b@x"}], SCHEMA_V2)
    _write(client, "b3", [{"id": 3, "name": "c"}], SCHEMA_V3)

    v1 = client.get("/tables/t/rows", params={"version": 1}).json()
    assert v1["schema_version"] == 1
    assert v1["rows"] == [{"id": 1, "name": "a", "score": 10}]  # 无 email 列
    assert [c["name"] for c in v1["schema"]["columns"]] == ["id", "name", "score"]

    v2 = client.get("/tables/t/rows", params={"version": 2}).json()
    assert v2["schema_version"] == 2
    assert "email" in v2["rows"][0]

    # 再读一次 v1，确认后续写入没有改动已落地的旧快照
    v1_again = client.get("/tables/t/rows", params={"version": 1}).json()
    assert v1_again == v1


def test_schema_history_queryable(client):
    """场景：结构演进 —— 每个快照的结构版本可查询"""
    _write(client, "b1", [{"id": 1, "name": "a"}], SCHEMA_V1)
    _write(client, "b2", [{"id": 2, "name": "b", "email": "b@x"}], SCHEMA_V2)
    s1 = client.get("/tables/t/schema", params={"version": 1}).json()
    s2 = client.get("/tables/t/schema").json()  # 当前版本
    assert s1["schema_version"] == 1
    assert s2["schema_version"] == 2
    assert len(s2["schema"]["columns"]) == len(s1["schema"]["columns"]) + 1
