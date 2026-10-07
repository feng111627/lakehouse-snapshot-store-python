"""业务场景四：HTTP 接口的错误映射与端到端流程。

覆盖：各类业务错误映射为可区分的 HTTP 状态码与错误码，
底层异常不直接透出；建表->写批->演进->读版本的完整链路。
"""


class TestApi:
    def _create(self, client):
        r = client.post(
            "/tables",
            json={
                "name": "users",
                "primary_key": "id",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "str"},
                ],
            },
        )
        assert r.status_code == 201, r.text

    def test_端到端_建表写批演进读版本(self, client):
        self._create(client)
        r = client.post(
            "/tables/users/batches",
            json={"batch_id": "b1", "rows": [{"id": 1, "name": "a"}]},
        )
        assert r.status_code == 200 and r.json()["version"] == 1
        # 重复提交返回相同版本
        r = client.post(
            "/tables/users/batches",
            json={"batch_id": "b1", "rows": [{"id": 1, "name": "a"}]},
        )
        assert r.json()["version"] == 1
        # 演进加列
        r = client.post(
            "/tables/users/schema",
            json={
                "primary_key": "id",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "str"},
                    {"name": "email", "type": "str", "nullable": True},
                ],
            },
        )
        assert r.status_code == 200
        # 读旧版本仍是旧结构
        r = client.get("/tables/users/rows", params={"version": 1})
        body = r.json()
        assert body["rows"] == [{"id": 1, "name": "a"}]
        assert [c["name"] for c in body["schema"]["columns"]] == ["id", "name"]
        # 读当前版本带新列
        r = client.get("/tables/users/rows")
        assert r.json()["rows"] == [{"id": 1, "name": "a", "email": None}]

    def test_错误映射_缺字段返回422与可区分错误码(self, client):
        self._create(client)
        r = client.post(
            "/tables/users/batches", json={"batch_id": "b1", "rows": [{"id": 1}]}
        )
        assert r.status_code == 422
        assert r.json()["error"]["code"] == "MISSING_FIELD"

    def test_错误映射_类型不匹配返回422(self, client):
        self._create(client)
        r = client.post(
            "/tables/users/batches",
            json={"batch_id": "b1", "rows": [{"id": "x", "name": "a"}]},
        )
        assert r.status_code == 422
        assert r.json()["error"]["code"] == "TYPE_MISMATCH"

    def test_错误映射_主键冲突返回409(self, client):
        self._create(client)
        r = client.post(
            "/tables/users/batches",
            json={
                "batch_id": "b1",
                "rows": [{"id": 1, "name": "a"}, {"id": 1, "name": "b"}],
            },
        )
        assert r.status_code == 409
        assert r.json()["error"]["code"] == "PRIMARY_KEY_CONFLICT"

    def test_错误映射_表不存在返回404(self, client):
        r = client.get("/tables/ghost/rows")
        assert r.status_code == 404
        assert r.json()["error"]["code"] == "TABLE_NOT_FOUND"

    def test_错误映射_版本不存在返回404(self, client):
        self._create(client)
        r = client.get("/tables/users/rows", params={"version": 99})
        assert r.status_code == 404
        assert r.json()["error"]["code"] == "VERSION_NOT_FOUND"

    def test_错误映射_非法结构演进返回422(self, client):
        self._create(client)
        r = client.post(
            "/tables/users/schema",
            json={
                "primary_key": "id",
                "columns": [
                    {"name": "id", "type": "str"},
                    {"name": "name", "type": "str"},
                ],
            },
        )
        assert r.status_code == 422
        assert r.json()["error"]["code"] == "SCHEMA_INCOMPATIBLE"
