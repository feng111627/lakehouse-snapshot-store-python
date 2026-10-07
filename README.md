# lakehouse-snapshot-store

基于 FastAPI 的本地湖仓表读写服务：用本地磁盘文件模拟表存储，
保证重复写入幂等、表结构可演进、并发读取有快照一致性。

## 运行

```bash
uv sync
uv run uvicorn main:app --port 8000
# 数据目录默认 ./data，可用环境变量覆盖：
LAKEHOUSE_ROOT=/tmp/lake uv run uvicorn main:app --port 8000
```

## 快照与版本规则

每张表在磁盘上的布局：

```
<root>/<table>/
  schema_history/000000.json   每个版本的表结构（提交后不可变）
  snapshots/000000.json        每个版本的完整数据快照（提交后不可变）
  batches.json                 幂等键 batch_id -> {version, digest}
  CURRENT                      当前已提交版本号
  _staging/                    写入暂存区，提交前对读者不可见
```

- **版本号**：建表为版本 0；每次成功提交批次或结构演进，版本号 +1。
- **原子提交**：新版本的快照与结构先写入 `_staging/` 并 fsync，再原子 rename
  到正式目录，最后用"写临时文件 + `os.replace`"原子切换 `CURRENT` 指针。
  读者只有看到 `CURRENT` 更新后才能看到新版本，因此永远读不到写了一半的状态。
- **失败回滚**：`CURRENT` 切换之前的任何失败（校验失败、写盘异常）都只是
  丢弃暂存文件，已提交的旧版本不受影响，无需显式回滚动作。
- **快照隔离**：快照文件提交后不可变，读路径不加锁；写路径按表加锁串行化。
  读写并发时，读者看到的必然是某一个完整已提交版本；历史版本可按
  `version` 参数读取，内容不会被后续写入改写。
- **写入幂等**：提交批次必须携带 `batch_id`。同一 `batch_id` 且内容指纹一致
  的重复提交（重试、断点重放、并发提交）直接返回首次提交的版本号，不产生
  新版本、不写重复数据；同一 `batch_id` 携带不同内容会被拒绝（`INVALID_BATCH`）。
  批内按主键 upsert：同主键新行覆盖旧行。

## 结构兼容范围

演进规则（旧结构 -> 新结构），不满足时抛 `SCHEMA_INCOMPATIBLE`：

| 变更 | 是否允许 | 说明 |
| --- | --- | --- |
| 新增列 | 允许 | 存量数据该列读为 `null` |
| 删除列 | 允许（主键列除外） | 旧版本仍按写入时的结构读取 |
| int -> float | 允许 | 唯一的类型提升；存量值自动转为 float |
| 其他类型变更 | 拒绝 | 含 float->int、任意->str/bool 等 |
| 可空 -> 非空 | 拒绝 | 只允许非空放宽为可空 |
| 删除/更换主键 | 拒绝 | 主键列与主键定义不可变 |

每个版本都保存写入当时的完整结构，历史快照按各自版本的结构读取，
后续演进不会改写已落地快照的内容。

## 错误分类

所有业务失败返回统一结构 `{"error": {"code", "detail"}}`，不透出底层异常：

| code | HTTP | 含义 |
| --- | --- | --- |
| `TABLE_NOT_FOUND` | 404 | 表不存在 |
| `VERSION_NOT_FOUND` | 404 | 版本不存在 |
| `TABLE_ALREADY_EXISTS` | 409 | 重复建表 |
| `PRIMARY_KEY_CONFLICT` | 409 | 批内同主键出现不同内容 |
| `INVALID_BATCH` | 409 | batch_id 为空或复用为不同内容 |
| `MISSING_FIELD` | 422 | 缺少必填字段 |
| `TYPE_MISMATCH` | 422 | 字段类型与结构不符 |
| `UNKNOWN_FIELD` | 422 | 出现结构中不存在的字段 |
| `SCHEMA_INCOMPATIBLE` | 422 | 结构演进不兼容 |
| `INTERNAL_ERROR` | 500 | 未预期内部错误（已包装，不透出底层异常） |

## API 一览

- `POST /tables` 建表：`{"name", "primary_key", "columns": [{"name","type","nullable"}]}`
- `POST /tables/{name}/batches` 幂等提交批次：`{"batch_id", "rows": [...]}`
- `POST /tables/{name}/schema` 结构演进（全量新结构）
- `GET  /tables/{name}/rows?version=N` 读快照（缺省读当前版本）
- `GET  /tables/{name}/schema?version=N` 读某版本结构
- `GET  /tables/{name}/version` 当前版本号

## 本地验证

```bash
uv run pytest
```

测试按业务场景分组，`-v` 输出可直接看到每个场景的用例与结果：

- `tests/test_idempotent_writes.py` —— 写入幂等：重复提交、断点重放、
  并发提交同一批次、主键冲突、字段缺失、类型不匹配、失败回滚。
- `tests/test_schema_evolution.py` —— 结构演进：加列、删列、int->float 提升、
  非法变更拒绝、主键保护、历史快照不被改写。
- `tests/test_snapshot_isolation.py` —— 并发读写：写入期间读者只见完整版本、
  多写线程无丢失、历史版本与并发写互不干扰。
- `tests/test_api.py` —— HTTP 层：端到端链路与错误码到 HTTP 状态的映射。

手工冒烟：

```bash
uv run uvicorn main:app --port 8000 &
curl -X POST localhost:8000/tables -H 'content-type: application/json' -d '{
  "name": "users", "primary_key": "id",
  "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "str"}]}'
curl -X POST localhost:8000/tables/users/batches -H 'content-type: application/json' -d '{
  "batch_id": "b1", "rows": [{"id": 1, "name": "alice"}]}'
curl localhost:8000/tables/users/rows
```
