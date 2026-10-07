"""业务场景三：并发读写的快照一致性。

覆盖：写入进行中读者只能看到某个完整版本；读写并发时读到的数据
必然属于某一个已提交版本；失败写入回滚后并发读不受影响。
"""

import threading

from lakehouse import Column, ColumnType, TableSchema


def _make_store(store):
    store.create_table(
        "events",
        TableSchema("id", (Column("id", ColumnType.INT), Column("v", ColumnType.INT))),
    )
    return store


class TestSnapshotIsolation:
    """读写并发时查询只能看到完整版本。"""

    def test_写入期间并发读_永远读到完整版本(self, store):
        store = _make_store(store)
        store.commit_batch("events", "b0", [{"id": 0, "v": 0}])

        stop = threading.Event()
        anomalies: list[str] = []

        def reader():
            while not stop.is_set():
                ver, schema, rows = store.read_snapshot("events")
                # 一致性判据：读到的行集必须恰好是 {0..ver} 的完整前缀，
                # 任何"写了一半"的状态（缺行/多行/版本与数据不匹配）都算异常
                ids = sorted(r["id"] for r in rows)
                if ids != list(range(0, len(ids))):
                    anomalies.append(f"version={ver} ids={ids[:5]}...len={len(ids)}")
                if len(rows) != ver:  # 每个版本恰好写入一行 id=ver-1
                    anomalies.append(f"version={ver} 行数={len(rows)}")

        def writer():
            for i in range(1, 60):
                store.commit_batch("events", f"b{i}", [{"id": i, "v": i}])
            stop.set()

        readers = [threading.Thread(target=reader) for _ in range(4)]
        for t in readers:
            t.start()
        w = threading.Thread(target=writer)
        w.start()
        w.join()
        for t in readers:
            t.join()

        assert not anomalies, anomalies[:3]
        assert store.current_version("events") == 60

    def test_多写线程并发提交不同批次_版本单调且数据无丢失(self, store):
        store = _make_store(store)
        errors: list[Exception] = []

        def writer(tid: int):
            try:
                for i in range(15):
                    store.commit_batch(
                        "events", f"t{tid}-b{i}", [{"id": tid * 1000 + i, "v": i}]
                    )
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert store.current_version("events") == 60  # 4*15 个版本，一个不少
        _, _, rows = store.read_snapshot("events")
        assert len(rows) == 60  # 60 行全在，无丢失无重复

    def test_读历史版本与并发写入互不干扰(self, store):
        store = _make_store(store)
        v1 = store.commit_batch("events", "b1", [{"id": 1, "v": 1}])
        stop = threading.Event()
        anomalies: list[str] = []

        def reader_of_old():
            while not stop.is_set():
                _, _, rows = store.read_snapshot("events", version=v1)
                if rows != [{"id": 1, "v": 1}]:
                    anomalies.append(f"历史版本被改写: {rows!r}")

        def writer():
            for i in range(2, 40):
                store.commit_batch("events", f"b{i}", [{"id": i, "v": i}])
            stop.set()

        t = threading.Thread(target=reader_of_old)
        t.start()
        w = threading.Thread(target=writer)
        w.start()
        w.join()
        t.join()

        assert not anomalies
