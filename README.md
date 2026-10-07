# lakehouse-snapshot-store-python

一个基于 FastAPI 的本地湖仓表读写服务：用本地磁盘上的文件模拟表的存储，
保证重复写入、结构演进、并发读写场景下的结果可预期、可解释。

## 运行

```bash
uv sync                 # 安装依赖
uv run python main.py # 启动服务（默认数据目录 ./data，可用 LAKEHOUSE_DATA_DIR 覆盖）
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tables/{name}/writes` | 幂等写入一批数据，可携带 `schema` 建表或演进结构 |
| GET | `/tables/{name}/rows?version=N` | 读快照，默认当前版本；历史版本按落地时的结构返回 |
| GET | `/tables/{name}/versions` | 列出全部版本及元信息 |
| GET | `/tables/{name}/schema?version=N` | 查询某个版本生效的结构 |

写入请求体：

```json
{
  "batch_id": "batch-001",
  "rows": [{"id": 1, "name": "a"}],
  "schema": {
    "primary_key": "id",
    "columns": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
      {"name": "email", "type": "string", "nullable": true}
    ]
  }
}
```

错误响应统一为 `{"error": {"code", "message", "detail"}}`，不透出底层异常。
错误码：`duplicate_key`(409)、`missing_field`(422)、`type_mismatch`(422)、
`schema_incompatible`(422)、`schema_required`(422)、
`table_not_found`/`snapshot_not_found`(404)、`internal_error`(500)。

## 快照与版本规则

磁盘布局（`data/<table>/`）：

```
snapshots/<version>/data.jsonl   # 该版本的完整数据，落地后不可变
snapshots/<version>/meta.json    # 版本号、结构版本、已应用 batch_id 列表、行数、时间
schemas/<schema_version>.json    # 结构历史，每个版本一份
CURRENT                          # 当前版本号指针文件
```

- **单调版本**：每次成功提交版本号 +1，从 1 开始连续递增，无空洞。
- **原子提交**：新快照先写入 `snapshots/<v>.tmp` 临时目录，`rename` 就位后，
  最后才用「写临时文件 + `os.replace`」原子切换 `CURRENT` 指针。
  读取方任意时刻只能读到某一个完整版本，不会读到写了一半的状态。
- **失败回滚**：提交中途任何失败都会清理临时目录和临时指针，
  `CURRENT` 保持指向旧版本，已落地数据不受影响；故障恢复后用同一
  `batch_id` 重提即可。
- **幂等写入**：每批数据必须携带 `batch_id`。同一 `batch_id` 重复提交、
  断点重放或并发提交，只有第一个生效，后续返回首次提交的结果
  （`replayed: true`），不产生重复数据、不产生新版本。
  同一张表的写入由写锁串行化，并发重放在锁内被识别。
- **同批主键冲突**：同一批次内主键重复直接以 `duplicate_key` 拒绝，
  整批不落地；不同批次间相同主键按 upsert 覆盖（后写胜出）。
- **快照归一化**：每次提交把所有行归一化到生效结构（新列补 `null`、
  删列剔除），因此每个快照按自己落地时的结构即可自洽读取，
  后续写入不会改变已落地旧快照读出的内容。

## 结构兼容范围

从旧结构演进到新结构时按以下规则校验，不满足则返回 `schema_incompatible`：

| 变更 | 是否允许 | 说明 |
| --- | --- | --- |
| 新增 nullable 列 | ✅ | 历史行读出来补 `null` |
| 新增非空列 | ❌ | 历史快照无法兼容 |
| 删除列 | ✅ | 旧快照仍按写入时结构读，新快照不再返回该列 |
| `int` → `float` | ✅ | 类型提升；写入侧同样允许向 `float` 列写 `int` |
| 其他类型变更 | ❌ | 如 `string` → `int`、`float` → `int` |
| nullable → 非空 | ❌ | 不允许收紧；非空 → nullable 允许 |
| 主键改名/改类型 | ❌ | 主键不可变 |

支持的列类型：`int`、`float`、`string`、`bool`。主键列不允许 nullable。

## 本地验证

```bash
uv run pytest          # 跑全部单测（默认 -v 并输出场景日志）
uv run pytest -q       # 只看结果汇总
```

测试日志按「场景 → 用例 → 结果」输出，可直接看出覆盖了哪些业务：

```
【开始】场景：重复写入 —— 同一批数据重复提交不产生重复数据 | 用例: test_replay_same_batch_is_noop
【结束】场景：重复写入 —— 同一批数据重复提交不产生重复数据 | 用例: test_replay_same_batch_is_noop | 结果: 通过
```

覆盖的业务场景：

- **重复写入**（`tests/test_idempotent_writes.py`）：重复提交、断点重放、
  并发提交同一批次、主键冲突/字段缺失/类型不匹配的可区分报错、
  失败批次不留痕迹、提交中途失败回滚。
- **结构演进**（`tests/test_schema_evolution.py`）：加列/删列/类型提升、
  不兼容变更拒绝、历史快照按写入时结构读取、结构版本历史查询。
- **并发读写**（`tests/test_concurrent_reads.py`）：读写并发只读到完整快照、
  并发写不同批次不丢数据且版本连续、写入进行中指定版本读取内容不变。

手工验证示例：

```bash
uv run python main.py &
curl -X POST localhost:8000/tables/users/writes -H 'Content-Type: application/json' -d '{
  "batch_id": "b1",
  "rows": [{"id": 1, "name": "a"}],
  "schema": {"primary_key": "id", "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]}
}'
curl localhost:8000/tables/users/rows          # 当前快照
curl localhost:8000/tables/users/versions      # 版本列表
```
