"""场景：并发读写 —— 读取只能看到完整快照，写一半的状态不可见。"""

from __future__ import annotations

import threading

import main

SCHEMA = {
    "primary_key": "id",
    "columns": [
        {"name": "id", "type": "int"},
        {"name": "val", "type": "int"},
    ],
}


def test_reads_always_see_complete_snapshot(client):
    """场景：并发读写 —— 读写并发时每次读到的都是某一个完整版本"""
    table = main.registry.get("t")
    table.write("b0", [{"id": i, "val": 0} for i in range(50)], SCHEMA)

    violations = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            snap = table.read()
            rows = snap["rows"]
            # 每个版本都是 50 行的完整快照，且 val 全部一致（同事务写入）
            vals = {row["val"] for row in rows}
            if len(rows) != 50 or len(vals) != 1:
                violations.append((snap["version"], len(rows), vals))

    def writer():
        for batch in range(1, 6):
            table.write(f"b{batch}", [{"id": i, "val": batch} for i in range(50)])

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for th in readers:
        th.start()
    writer()
    stop.set()
    for th in readers:
        th.join()

    assert violations == []
    assert table.read()["version"] == 6  # b0..b5 共 6 个版本


def test_concurrent_writers_distinct_batches_all_land(client):
    """场景：并发读写 —— 不同批次并发提交不丢数据，版本连续"""
    table = main.registry.get("t")
    errors = []

    def writer(n):
        try:
            table.write(f"batch-{n}", [{"id": n, "val": n}], SCHEMA)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    snap = table.read()
    assert snap["row_count"] == 20  # 20 个批次的数据全部落地
    assert sorted(row["id"] for row in snap["rows"]) == list(range(20))
    versions = [v["version"] for v in table.versions()]
    assert versions == list(range(1, 21))  # 版本号连续无空洞


def test_read_pinned_version_during_writes(client):
    """场景：并发读写 —— 指定版本的读取在写入进行中内容不变"""
    table = main.registry.get("t")
    table.write("b0", [{"id": 1, "val": 100}], SCHEMA)
    before = table.read(1)

    for batch in range(1, 10):
        table.write(f"b{batch}", [{"id": 1, "val": 100 + batch}])

    after = table.read(1)
    assert after == before  # 旧快照内容不受后续写入影响
    assert table.read()["rows"][0]["val"] == 109  # 当前版本是最新值
