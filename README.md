# 本地实时行情接入与窗口聚合（Python）

一套基于 FastAPI 的本地实时行情接入与时间窗聚合服务。用**本地文件**模拟数据源的
消费位点、已确认事件日志（WAL）与聚合状态快照，使在**重复提交、乱序到达、事件结构
演进、多来源编号撞车、小数金额累计与容量受限**下的聚合结果保持
**确定、一致、精确、可解释、可恢复**。

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
| `event_id` | string | 是 | 事件编号，**来源内**唯一标识，不允许空白；不同来源之间允许重复 |
| `source` | string | 是 | 来源标识，去重命名空间与位点都按 source 分别维护 |
| `symbol` | string | 是 | 标的代码，聚合维度之一 |
| `price` | number | 是 | 价格，有限正数；服务内部按**精确十进制**（Decimal）处理 |
| `quantity` | number | 是 | 数量，非负有限数；同样按精确十进制处理 |
| `event_time_ms` | int | 是 | 事件时间（毫秒），非负整数，窗口归属依据 |
| `schema_version` | int | 否 | 结构版本，缺省为 `1` |
| `offset` | int | 否 | 来源内单调序号，用于模拟消费位点 |
| `venue` | string | 否 | **v2 新增**，交易场所；缺失或空白时稳定缺省为 `"DEFAULT"` |

> **多来源编号语义**：去重主键是**复合键 `(source, event_id)`**。真实接入时不同
> 来源各有自己的编号体系，编号撞车是常态——两个来源提交相同 `event_id` 属合法
> 行情，会被当作两件独立事件分别受理、各自计数、互不覆盖、互不干扰。是否"同一件
> 事"的判断**只在同一个 source 内部**进行（见 3.1）。

聚合输出按 `(symbol, window_start_ms)` 维护：`count / total_quantity / notional /
avg_price / min_price / max_price / first_price / last_price / sources /
event_ids / closed`。其中：

- `notional = Σ price × quantity` 为**累计成交金额**，`total_quantity` 为累计成交量，
  `avg_price = notional / total_quantity` 为**成交量加权平均成交价**；
- 所有金额/数量字段及 `avg_price` 都是**精确十进制**值（HTTP 按精确数字字面量输出，
  如 `11.994`、`0.30`，不做二进制浮点舍入），零成交量窗口不返回 `avg_price`；
- `first_price/last_price` 以 **`(event_time_ms, source, event_id)` 确定性决胜**
  （最早事件时间的成交价 / 最晚事件时间的成交价，按时间而非按价格；多来源 event_id
  可能撞车，故决胜键加入 `source` 保证全局可比较），与事件到达顺序无关；
  `min_price/max_price` 才是按价格取极值。

## 3. 去重与窗口规则

### 3.1 来源内去重（确定性幂等）
- 判断是否同一事件**只看同一个来源内部**：复合键 `(source, event_id)` 相同才视为重复。
- 同一来源内，相同复合键再次提交且**内容一致**：返回 `DUPLICATE`，不重复计入、
  无副作用，也不占用任何容量额度。
- 同一来源内，相同复合键但**内容不同**：返回 `DUPLICATE_ID_CONFLICT`，绝不覆盖
  已确认事件，既有聚合不被污染，同时落死信以便审计。
- **不同来源 event_id 相同不冲突**：各自独立受理、独立计数、互不干扰；
  同一来源的冲突拒绝也不会影响其他来源的同名编号。
- 内容一致性通过"除来源内主键外字段"的规范化精确 JSON 签名比较（Decimal 精确序列化）。

### 3.1.1 小数金额口径（精确十进制，杜绝漂移）
- `price/quantity` 入服务后即转为 `Decimal`，此后解析、累计、均价、快照、WAL、
  HTTP 输出**全程不经过二进制 `float`**，数量再多、金额再大也不会越累越偏。
- `notional`、`total_quantity` 用 Decimal 加法逐项累计，`avg_price` 用
  Decimal 除法（高精度上下文）计算，口径为**成交量加权均价**。
- 同一批事件无论**按什么顺序到达**、中间**怎么重复提交**，最终累计成交金额与
  平均成交价都**完全一致**（精确相等，而非浮点近似）。
- 持久化的 WAL/快照中金额以**精确十进制 JSON 数字**保存（如 `19.99`、`0.30`），
  读回仍是同一个 Decimal；旧版本落盘的 float 数据可直接读入并自动归一化。

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
| `WINDOW_FULL` | 单窗事件数达到 `max_events_per_window`（窗口档） |
| `BATCH_TOO_LARGE` | 批量提交条数超过 `max_submit_batch`，整批拒绝（批次档） |
| `SERVICE_FULL` | **服务级总闸**：跨来源、跨窗口累计已受理数达到 `max_total_events` |
| `DUPLICATE_ID_CONFLICT` | 同一来源内 event_id 相同但内容冲突；或重投死信时同编号跨来源命中多条 |

拒绝是**可恢复**的：原始报文保存在死信日志，调用重投接口（或基准时间推进后再投）
即可重新走完整校验；成功后死信条目自动移除。多来源下若同一 `event_id` 在多个来源
都有死信，重投时需带 `source` 查询参数精确定位；不带且命中多条时返回
`DUPLICATE_ID_CONFLICT` 以提示歧义，绝不随意挑一条。

## 5. 并发、容量与持久化

- 所有写入在一把可重入锁内完成，保证并发提交下去重、聚合、额度与位点的确定性。
- 容量分**三档**，拒绝原因彼此明确区分，一眼能看出是哪一层挡的：

  | 档位 | 配置 | 统计口径 | 拒绝原因 |
  | --- | --- | --- | --- |
  | 窗口档 | `max_events_per_window` | 单个 `(symbol, 窗口)` 事件数 | `WINDOW_FULL` |
  | 批次档 | `max_submit_batch` | 单次批量请求条数（整批拒绝） | `BATCH_TOO_LARGE` |
  | **服务总闸** | `max_total_events`（默认 1,000,000，`0` 表示不限） | **跨来源、跨窗口**累计**已受理**唯一事件数 | `SERVICE_FULL` |

- **服务级总闸规则**：
  - 额度统计的是"已确认受理"的唯一事件数；`DUPLICATE` 与各类 `REJECTED` 都不占额度；
  - 判定与预留都在同一把锁内完成：多个提交并发抢额度时**绝不超发**，恰好占满上限，
    谁先抢到锁谁受理、其余确定地收到 `SERVICE_FULL`，结果可复现；
  - 总闸在去重之后、入窗之前判定（即"全局名额先于单窗名额"）；单窗先到顶而总闸
    仍有余量时，依旧返回 `WINDOW_FULL`，两层原因不会混淆；
  - 已占用额度随每次受理写入快照（`total_accepted`）；重启后从快照恢复，快照缺失时
    从 WAL 重放计数兜底——**已用额度不凭空多、也不凭空少**，恢复后剩余名额精确衔接。
- 持久化顺序（每次接受事件）：
  1. 追加 WAL 并 `fsync`（`data/events.log`）；
  2. 更新内存去重表（复合键）、服务总额度与窗口聚合；
  3. 原子写状态快照 `data/state.json`（tmp + rename）；
  4. 原子写各来源位点 `data/offsets.json`。
- 重启恢复：优先从状态快照恢复去重表/总额度/水位线/聚合；快照缺失时从 WAL 全量
  重放兜底（兼容旧版 `event_id` 全局命名空间的快照：旧编号归入保留命名空间，已确认
  事件仍不重不漏）。已确认事件与聚合状态**不重复、不丢失**，重启后重复提交仍返回
  `DUPLICATE`。
- 被拒绝事件写入 `data/dead_letters.log`（记录含 `source`），支持按
  `(source, event_id)` 精确查询与重投，删除死信限定来源作用域，不误删他源条目。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查、当前水位线、窗口数 |
| POST | `/events` | 提交单条事件（原始 JSON 透传） |
| POST | `/events/batch` | 批量提交（超上限整批 422） |
| GET | `/aggregates?symbol=` | 查询窗口聚合 |
| GET | `/dead-letters` | 查询被拒绝事件与可区分原因（含 source） |
| POST | `/dead-letters/{event_id}/redeliver?source=` | 重新投递死信；多来源撞编号时用 `source` 精确定位 |
| POST | `/windows/close` | 显式关闭所有已到期窗口 |
| GET | `/offsets` | 查询各来源消费位点 |
| GET | `/quota` | 查询服务总闸用量 `accepted_total / max_total_events`（也在 `/health` 返回） |

示例：

```bash
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"E1","source":"SRC-A","symbol":"600000","price":10.5,
       "quantity":2,"event_time_ms":10000,"offset":1}'
curl -s localhost:8000/aggregates
curl -s localhost:8000/dead-letters
curl -s localhost:8000/quota
# 多来源同编号死信重投（必须指定 source）
curl -s -X POST "localhost:8000/dead-letters/LATE9/redeliver?source=SRC-A"
```

## 7. 验证方法

`uv run pytest`（INFO 级日志实时打印**输入事件、判定结果与判定依据**，并对每个
业务场景打印"覆盖了什么、断言依据是什么"）覆盖：

- **重复提交**：相同事件多次提交只计一次；同源同 id 不同内容冲突拒绝（幂等性）。
- **多来源编号独立**：不同来源 `event_id` 撞车各自受理、互不覆盖；同源重复仍幂等、
  同源冲突 `DUPLICATE_ID_CONFLICT` 且隔离于他源；跨来源死信重投需指定 `source`。
- **小数金额精确一致**：含 `19.99×0.30`、`0.07×100` 等经典漂移样本，断言同一批事件
  在**正序与反序**到达下 `notional/avg_price` **精确相等**，并与手算期望值一致；
  乱序且中途重复提交的结果与干净提交完全一致。
- **乱序到达**：事件按事件时间归入本来窗口；first/last 价与到达顺序无关；
  超迟到事件以 `WINDOW_CLOSED` 明确拒绝且可重投。
- **结构变更**：v2 新字段缺省稳定；缺失/类型不符/高版本/未来越界原因可区分，
  拒绝不污染聚合且可重投。
- **三档容量**：单窗满员 `WINDOW_FULL`、批量超限 `BATCH_TOO_LARGE`、
  服务总闸到顶 `SERVICE_FULL`，三者原因互不混淆；单窗先到顶时不误报总闸。
- **并发总闸确定性**：8 线程并发抢 50 个总额度，断言受理数**恰好等于上限、绝不超发**，
  拒绝全部是 `SERVICE_FULL`，WAL 复合键唯一。
- **重启额度一致**：重启后 `accepted_total` 与窗口事件总数、WAL 条数完全对得上，
  剩余名额精确衔接；快照缺失时 WAL 重放同样恢复额度且小数金额无漂移。
- **持久化恢复**：重启后已确认事件不丢不重；快照缺失时 WAL 重放兜底。
- **HTTP 端到端**：完整提交/拒绝/聚合/重投/位点/总额度流程，金额按精确数字字面量输出。

本地验证命令：

```bash
uv run pytest                                  # 全量（26 个用例，日志含判定依据）
uv run pytest tests/test_multi_source_decimal_quota.py -v   # 仅本轮三个新场景
uv run python main.py                          # 起服务后用上面的 curl 手工验证
```

## 8. 代码结构

```
app/config.py    窗口、迟到、未来偏移、三档容量等确定性参数
app/models.py    事件、提交结果、窗口聚合（Decimal 金额 / avg_price）等类型定义
app/storage.py   本地文件存储：Decimal 精确编解码、WAL / 快照 / 位点 / 死信
app/windows.py   窗口分配、水位线、迟到与未来判定、Decimal 确定性聚合
app/pipeline.py  校验、来源内去重、服务总闸、入窗、持久化、位点推进、死信重投
app/api.py       FastAPI 接入层（金额精确数字输出）
tests/           重复/乱序/结构/三档容量/重启/并发/多来源/小数金额/HTTP 测试
```
