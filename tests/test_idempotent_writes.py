"""业务场景一：写入幂等。

覆盖：同一批数据重复提交、断点重放、并发提交同一批次、主键冲突、
字段缺失、类型不匹配、批次号复用为不同内容。
"""

import threading

import pytest

from lakehouse.errors import ErrorCode, LakehouseError


class TestIdempotentWrites:
    """重复写入与失败原因区分。"""

    def test_重复提交同一批次_只产生一个版本(self, store_with_table):
        store = store_with_table
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        v1 = store.commit_batch("users", "batch-1", rows)
        v2 = store.commit_batch("users", "batch-1", rows)  # 断点重放
        v3 = store.commit_batch("users", "batch-1", rows)  # 再次重复
        assert v1 == v2 == v3
        assert store.current_version("users") == v1
        _, _, got = store.read_snapshot("users")
        assert sorted(r["id"] for r in got) == [1, 2]

    def test_并发提交同一批次_全部返回相同版本(self, store_with_table):
        store = store_with_table
        rows = [{"id": i, "name": f"n{i}"} for i in range(20)]
        results: list[int] = []
        errors: list[Exception] = []

        def worker():
            try:
                results.append(store.commit_batch("users", "batch-x", rows))
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(set(results)) == 1
        assert store.current_version("users") == results[0]
        _, _, got = store.read_snapshot("users")
        assert len(got) == 20  # 无重复、无丢失

    def test_相同批次号提交不同内容_拒绝并报可区分错误(self, store_with_table):
        store = store_with_table
        store.commit_batch("users", "batch-1", [{"id": 1, "name": "a"}])
        with pytest.raises(LakehouseError) as exc:
            store.commit_batch("users", "batch-1", [{"id": 9, "name": "z"}])
        assert exc.value.code == ErrorCode.INVALID_BATCH

    def test_按主键更新_同主键新数据覆盖旧数据(self, store_with_table):
        store = store_with_table
        store.commit_batch("users", "b1", [{"id": 1, "name": "a"}])
        store.commit_batch("users", "b2", [{"id": 1, "name": "a2"}])
        _, _, rows = store.read_snapshot("users")
        assert rows == [{"id": 1, "name": "a2"}]

    def test_批次内主键冲突_报主键冲突错误(self, store_with_table):
        with pytest.raises(LakehouseError) as exc:
            store_with_table.commit_batch(
                "users", "b1", [{"id": 1, "name": "a"}, {"id": 1, "name": "b"}]
            )
        assert exc.value.code == ErrorCode.PRIMARY_KEY_CONFLICT

    def test_字段缺失_报缺失字段错误(self, store_with_table):
        with pytest.raises(LakehouseError) as exc:
            store_with_table.commit_batch("users", "b1", [{"id": 1}])
        assert exc.value.code == ErrorCode.MISSING_FIELD

    def test_类型不匹配_报类型错误(self, store_with_table):
        with pytest.raises(LakehouseError) as exc:
            store_with_table.commit_batch("users", "b1", [{"id": "x", "name": "a"}])
        assert exc.value.code == ErrorCode.TYPE_MISMATCH

    def test_未知字段_报未知字段错误(self, store_with_table):
        with pytest.raises(LakehouseError) as exc:
            store_with_table.commit_batch(
                "users", "b1", [{"id": 1, "name": "a", "ghost": 1}]
            )
        assert exc.value.code == ErrorCode.UNKNOWN_FIELD

    def test_批次中途校验失败_整体回滚不影响已落地数据(self, store_with_table):
        store = store_with_table
        v1 = store.commit_batch("users", "b1", [{"id": 1, "name": "a"}])
        with pytest.raises(LakehouseError):
            # 第二行缺字段，整批必须失败
            store.commit_batch("users", "b2", [{"id": 2, "name": "b"}, {"id": 3}])
        assert store.current_version("users") == v1
        _, _, rows = store.read_snapshot("users")
        assert rows == [{"id": 1, "name": "a"}]
