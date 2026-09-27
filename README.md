# 本地实时行情接入与窗口聚合（Python）

一套基于 FastAPI 的本地实时行情接入与时间窗聚合服务。用**本地文件**模拟数据源的
消费位点、已确认事件日志（WAL）与聚合状态快照，使在**重复提交、乱序到达、事件结构
演进与容量受限**下的聚合结果保持**确定、一致、可解释、可恢复**。

## 1. 快速开始

```bash
uv sync
uv run pytest                      # 运行全部单测（日志会打印输入事件与判定依据）
uv run python main.py              # 启动 HTTP 服务（默认 127.0.0.1:8000）
PIPELINE_DATA_DIR=/tmp/mk uv run python main.py   # 指定本地存储目录
```

## 2. 事件模型

单条行情事件为一个 JSON 对象：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `event_id` | string | 是 | 事件全局唯一标识，**去重主键**，不允许空白 |
| `source` | string | 是 | 来源标识，位点按 source 分别维护 |
| `symbol` | string | 是 | 标的代码，聚合维度之一 |
| `price` | number | 是 | 价格，必须为有限正数 |
| `quantity` | number | 是 | 数量，必须为非负有限数 |
| `event_time_ms` | int | 是 | 事件时间（毫秒），非负整数，窗口归属依据 |
| `schema_version` | int | 否 | 结构版本，缺省为 `1` |
| `offset` | int | 否 | 来源内单调序号，用于模拟消费位点 |
| `venue` | string | 否 | **v2 新增**，交易场所；缺失或空白时稳定缺省为 `"DEFAULT"` |

聚合输出按 `(symbol, window_start_ms)` 维护：`count / total_quantity / notional /
min_price / max_price / first_price / last_price / sources / event_ids / closed`。
其中 `first_price/last_price` 以 **`(event_time_ms, event_id)` 确定性决胜**
（最早事件时间的成交价 / 最晚事件时间的成交价，按时间而非按价格），与事件到达顺序
无关，因此乱序到达时结果一致；`min_price/max_price` 才是按价格取极值。

## 3. 去重与窗口规则

### 3.1 去重（确定性幂等）
- 同一 `event_id` 再次提交且**内容一致**：返回 `DUPLICATE`，不重复计入、无副作用。
- 同一 `event_id` 但**内容不同**：返回 `DUPLICATE_ID_CONFLICT`，绝不覆盖已确认事件，
  既有聚合不被污染，同时落死信以便审计。
- 内容一致性通过除 `event_id` 外字段的规范化 JSON 签名比较。

### 3.2 固定时间窗
- 窗口长度 `window_size_ms`（默认 1000ms），
  `window_start = event_time_ms // size * size`，左闭右开。

### 3.3 乱序与迟到（不静默丢弃）
- 水位线：`watermark = 已观测最大事件时间 - allowed_lateness_ms`（单调不减，
  默认允许迟到 500ms）。
- 乱序事件只要其窗口结束时刻仍大于水位线，即归入其**本来所属窗口**，聚合实时更新。
- 窗口结束时刻 `<= watermark` 时该窗口关闭；再来事件明确返回 `WINDOW_CLOSED`
  （而非静默丢弃），并写入死信日志。
- 事件时间超前"已观测最大事件时间"超过 `max_future_skew_ms`（默认 10s）时返回
  `FUTURE_EVENT`（窗口越界的另一种情形，原因可区分）。

## 4. 结构兼容（演进）策略

- **新增字段必须带稳定缺省值**：v2 新增 `venue`，v1 记录与缺省 `venue` 的 v2 记录
  统一按 `"DEFAULT"` 处理，旧数据读入语义不变。
- **向后兼容**：未知的多余字段被忽略，不影响既有聚合。
- **前向安全**：出现服务不支持的高版本 `schema_version` 时返回 `UNSUPPORTED_SCHEMA`
  并落死信，待服务升级后可重新投递。
- 校验失败按**可区分原因**拒绝，且全部落死信、可重新投递：

| reason | 触发条件 |
| --- | --- |
| `MISSING_FIELD` | 必填字段缺失，或字符串字段为空白 |
| `TYPE_MISMATCH` | 字段类型不符（含布尔充数字、NaN/Inf、价格非正等） |
| `UNSUPPORTED_SCHEMA` | schema_version 不受支持 |
| `WINDOW_CLOSED` | 迟到事件，所属窗口已关闭 |
| `FUTURE_EVENT` | 事件时间超前基准超过允许偏移 |
| `WINDOW_FULL` | 单窗事件数达到 `max_events_per_window` |
| `BATCH_TOO_LARGE` | 批量提交条数超过 `max_submit_batch`，整批拒绝 |
| `DUPLICATE_ID_CONFLICT` | 同一 event_id 内容冲突 |

拒绝是**可恢复**的：原始报文保存在死信日志，调用重投接口（或基准时间推进后再投）
即可重新走完整校验；成功后死信条目自动移除。

## 5. 并发、容量与持久化

- 所有写入在一把可重入锁内完成，保证并发提交下聚合与位点的确定性。
- 容量受限：单窗满员拒绝 `WINDOW_FULL`（不同标的窗口相互隔离）；批量超限
  **整批拒绝** `BATCH_TOO_LARGE`，不做部分处理，原因与单条超限明确区分。
- 持久化顺序（每次接受事件）：
  1. 追加 WAL 并 `fsync`（`data/events.log`）；
  2. 更新内存去重表与窗口聚合；
  3. 原子写状态快照 `data/state.json`（tmp + rename）；
  4. 原子写各来源位点 `data/offsets.json`。
- 重启恢复：优先从状态快照恢复去重表/水位线/聚合；快照缺失时从 WAL 全量重放兜底。
  已确认事件与聚合状态**不重复、不丢失**，重启后重复提交仍返回 `DUPLICATE`。
- 被拒绝事件写入 `data/dead_letters.log`，支持按 `event_id` 查询与重投。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查、当前水位线、窗口数 |
| POST | `/events` | 提交单条事件（原始 JSON 透传） |
| POST | `/events/batch` | 批量提交（超上限整批 422） |
| GET | `/aggregates?symbol=` | 查询窗口聚合 |
| GET | `/dead-letters` | 查询被拒绝事件与可区分原因 |
| POST | `/dead-letters/{event_id}/redeliver` | 重新投递死信 |
| POST | `/windows/close` | 显式关闭所有已到期窗口 |
| GET | `/offsets` | 查询各来源消费位点 |

示例：

```bash
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"E1","source":"SRC-A","symbol":"600000","price":10.5,
       "quantity":2,"event_time_ms":10000,"offset":1}'
curl -s localhost:8000/aggregates
curl -s localhost:8000/dead-letters
```

## 7. 验证方法

`uv run pytest`（INFO 级日志实时打印**输入事件**与**判定依据**）覆盖：

- **重复提交**：相同事件多次提交只计一次；同 id 不同内容冲突拒绝（幂等性）。
- **乱序到达**：事件按事件时间归入本来窗口；first/last 价与到达顺序无关；
  超迟到事件以 `WINDOW_CLOSED` 明确拒绝且可重投。
- **结构变更**：v2 新字段缺省稳定；缺失/类型不符/高版本/未来越界原因可区分，
  拒绝不污染聚合且可重投。
- **容量超限**：单窗满员 `WINDOW_FULL`、批量超限 `BATCH_TOO_LARGE`。
- **持久化恢复**：重启后已确认事件不丢不重；快照缺失时 WAL 重放兜底。
- **并发写入**：多线程并发下计数与去重结果确定。
- **HTTP 端到端**：完整提交/拒绝/聚合/重投/位点流程。

## 8. 代码结构

```
app/config.py    窗口、迟到、未来偏移、容量等确定性参数
app/models.py    事件、提交结果、窗口聚合等类型定义
app/storage.py   本地文件存储：WAL / 状态快照 / 位点 / 死信
app/windows.py   窗口分配、水位线、迟到与未来判定、确定性聚合
app/pipeline.py  校验、去重、入窗、持久化、位点推进、死信重投
app/api.py       FastAPI 接入层
tests/           重复/乱序/结构/容量/重启/并发/HTTP 测试
```
