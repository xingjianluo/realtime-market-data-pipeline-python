# 本地实时行情接入与窗口聚合（Python）

一套基于 FastAPI 的本地实时行情接入与时间窗聚合服务。用**本地文件**模拟数据源的
消费位点、已确认事件日志（WAL）与聚合状态快照，使在**多来源编号撞车、重复提交、
乱序到达、小数金额累加、事件结构演进与三档容量受限**下的聚合结果保持
**确定、一致、可解释、可恢复**。

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
| `event_id` | string | 是 | 来源内事件编号，**与 `source` 组成复合去重主键**，不允许空白 |
| `source` | string | 是 | 来源标识；不同来源编号体系相互独立，允许同号撞车 |
| `symbol` | string | 是 | 标的代码，聚合维度之一 |
| `price` | number | 是 | 价格，有限正数；**内部以高精度 Decimal 记账** |
| `quantity` | number | 是 | 数量，非负有限数；**内部以高精度 Decimal 记账** |
| `event_time_ms` | int | 是 | 事件时间（毫秒），非负整数，窗口归属依据 |
| `schema_version` | int | 否 | 结构版本，缺省为 `1` |
| `offset` | int | 否 | 来源内单调序号，用于模拟消费位点 |
| `venue` | string | 否 | **v2 新增**，交易场所；缺失或空白时稳定缺省为 `"DEFAULT"` |

聚合输出按 `(symbol, window_start_ms)` 维护：`count / total_quantity / notional /
avg_price / min_price / max_price / first_price / last_price / sources / event_ids /
event_refs / closed`。

- `first_price/last_price` 以 **`(event_time_ms, source, event_id)` 确定性决胜**
  （最早事件时间的成交价 / 最晚事件时间的成交价，按时间而非按价格），与事件到达顺序
  无关，多来源编号撞车时仍为全序；`min_price/max_price` 才是按价格取极值。
- `event_ids` 保留各来源原始编号；`event_refs` 为 `"source/event_id"` 的来源限定
  引用，跨来源同号时可精确区分归属。

## 3. 去重与窗口规则

### 3.1 多来源编号语义（同来源内判重，跨来源独立）
- 去重作用域为**同一来源内部**，唯一键为复合键 `(source, event_id)`。
- **跨来源同号**：不同来源即使 `event_id` 完全相同，也视为各自独立的事件，
  分别受理、分别聚合、互不覆盖、互不干扰。
- **同来源内重复提交**且内容一致：返回 `DUPLICATE`，不重复计入、无副作用。
- **同来源内同号但内容不同**：返回 `DUPLICATE_ID_CONFLICT`，绝不覆盖已确认事件，
  既有聚合不被污染，同时落死信以便审计。
- 内容一致性通过同来源内除 `event_id/source` 外字段的规范化签名比较
  （Decimal 按数值口径比较，`1.0` 与 `1.00` 视为同一内容）。
- 死信重投支持按来源精确定位：同号事件同时存在于多个来源的死信中且未指定
  `source` 时，返回 `AMBIGUOUS_REDELIVERY`，要求显式带来源消歧。

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

### 3.4 小数金额口径（Decimal 精确记账，无精度漂移）
- `price/quantity` 在入服时即经 `str()` 转为标准库 `Decimal`，全链路
  （校验签名、窗口累加、WAL、快照、API 输出）**不使用二进制 float 参与金额运算**，
  因此 `0.1` 这类十进制小数不会出现 `0.30000000000000004` 式漂移。
- 累计金额 `notional = Σ(price × quantity)` 与累计数量 `total_quantity`
  都是 Decimal 精确累加；Decimal 加法满足交换律/结合律，**同一批事件无论按什么
  顺序到达、中间夹杂多少次重复提交，累计金额与数量逐位一致**。
- 平均成交价 `avg_price = notional / total_quantity`，在 40 位精度下计算后
  量化到固定 **1e-10** 位（`ROUND_HALF_UP`），输出确定；累计数量为 0 时返回 `null`。
- 金额类字段在快照中以**精确十进制字符串**落盘、在 HTTP 响应中也以字符串输出
  （如 `"6.06"`），客户端应按十进制数值解析，不要按 float 解析后再比较。
- 兼容范围：升级前由旧版本写入的 float 形式 WAL/快照可正常读回（按字面量转
  Decimal）；旧版本登记的内容签名在重复提交时仍能匹配为 `DUPLICATE`。

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
| `WINDOW_FULL` | **窗口档**：单窗事件数达到 `max_events_per_window` |
| `BATCH_TOO_LARGE` | **批量档**：批量提交条数超过 `max_submit_batch`，整批拒绝 |
| `SERVICE_FULL` | **服务总闸**：服务累计受理数达到 `max_accepted_events` |
| `DUPLICATE_ID_CONFLICT` | 同来源内同一 event_id 内容冲突 |
| `AMBIGUOUS_REDELIVERY` | 重投的 event_id 在多个来源死信中撞号，需指定 source |

拒绝是**可恢复**的：原始报文保存在死信日志，调用重投接口（或基准时间推进后再投）
即可重新走完整校验；成功后死信条目自动移除。

## 5. 并发、容量与持久化

### 5.1 三档容量限制（拒绝原因一眼可分）
| 档位 | 配置 | 计数口径 | 拒绝原因 |
| --- | --- | --- | --- |
| 单窗档 | `max_events_per_window`（默认 1000） | 单个 `(symbol, window)` 内事件数 | `WINDOW_FULL` |
| 批量档 | `max_submit_batch`（默认 500） | 单次请求的数组长度 | `BATCH_TOO_LARGE`（整批拒绝） |
| 服务总闸 | `max_accepted_events`（默认 100000，`None` 表示不限） | **跨来源、跨窗口、跨已关闭窗口**的累计**受理**事件数 | `SERVICE_FULL` |

- 总闸只统计 `ACCEPTED`：重复提交（`DUPLICATE`）、各类拒绝、死信重投失败
  都不占用额度；已确认事件不因窗口关闭而释放额度。
- 判定顺序：结构校验 → 同来源去重/冲突 → **总闸** → 入窗（未来/关闭/单窗），
  即总闸到顶时直接返回 `SERVICE_FULL`，不会落到窗口档，层级清晰。
- 并发提交在同一把锁内串行完成"检查额度→入窗→WAL→计数→快照"，
  **总额度绝不超发**；谁进谁出由锁内的确定顺序决定，接受集合可复现。
- 额度随 `state.json` 持久化，重启后与已确认事件严格对齐；快照缺失走 WAL
  重放兜底，计数同样不凭空增多或减少。可用 `GET /capacity` 查询
  `accepted / limit / remaining`。

### 5.2 持久化与恢复
- 所有写入在一把可重入锁内完成，保证并发提交下聚合、额度与位点的确定性。
- 持久化顺序（每次接受事件）：
  1. 追加 WAL 并 `fsync`（`data/events.log`）；
  2. 更新内存去重表、总额度计数与窗口聚合；
  3. 原子写状态快照 `data/state.json`（tmp + rename，内含 `total_accepted`）；
  4. 原子写各来源位点 `data/offsets.json`。
- 重启恢复：优先从状态快照恢复去重表/水位线/聚合/总额度；旧版快照缺少
  `total_accepted` 时由各窗口 `count` 之和兜底；快照缺失时从 WAL 全量重放。
  已确认事件与聚合状态**不重复、不丢失**，重启后重复提交仍返回 `DUPLICATE`。
- 被拒绝事件写入 `data/dead_letters.log`，记录带来源字段，支持按 `event_id`
  查询、按 `(source, event_id)` 精确移除与重投。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查、当前水位线、窗口数与**总闸占用** |
| POST | `/events` | 提交单条事件（原始 JSON 透传） |
| POST | `/events/batch` | 批量提交（超上限整批 422） |
| GET | `/aggregates?symbol=` | 查询窗口聚合 |
| GET | `/dead-letters` | 查询被拒绝事件与可区分原因 |
| POST | `/dead-letters/{event_id}/redeliver?source=` | 重新投递死信（跨来源撞号须带 `source`） |
| POST | `/windows/close` | 显式关闭所有已到期窗口 |
| GET | `/offsets` | 查询各来源消费位点 |
| GET | `/capacity` | 查询服务总闸占用与剩余额度 |

示例：

```bash
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"E1","source":"SRC-A","symbol":"600000","price":10.5,
       "quantity":2,"event_time_ms":10000,"offset":1}'
curl -s localhost:8000/aggregates
curl -s localhost:8000/dead-letters
```

## 7. 验证方法

`uv run pytest`（INFO 级日志实时打印**输入事件、判定结果与判定依据**）覆盖：

- **重复提交**：同来源相同事件多次提交只计一次；同来源同 id 不同内容冲突拒绝。
- **多来源独立编号**：跨来源同号事件各自受理、互不覆盖；各自重复提交仍幂等；
  死信撞号重投的 `AMBIGUOUS_REDELIVERY` 消歧（带 `source` 后分别受理）。
- **小数金额精度**：含小数报价在**正序/逆序两种到达顺序**下累计金额、数量与
  均价逐位相等，并与手工 Decimal 基准核对；乱序夹杂重复提交不漂移不重计；
  上千笔小额（0.1/1000.01 类）大额累计仍精确；重启后金额逐位一致。
- **乱序到达**：事件按事件时间归入本来窗口；first/last 价与到达顺序无关；
  超迟到事件以 `WINDOW_CLOSED` 明确拒绝且可重投。
- **结构变更**：v2 新字段缺省稳定；缺失/类型不符/高版本/未来越界原因可区分。
- **三档容量**：单窗 `WINDOW_FULL`、批量 `BATCH_TOO_LARGE`、
  服务总闸 `SERVICE_FULL`（跨来源跨窗口累计，拒绝原因与前两档明确区分）。
- **总闸并发确定性**：8 线程 200 次提交争抢 100 额度，接受数恰好 100、
  无超发无重复，拒绝全部为 `SERVICE_FULL`。
- **总闸重启一致**：快照恢复与 WAL 重放两条路径下，已占用额度不凭空增减。
- **持久化恢复**：重启后已确认事件不丢不重；快照缺失时 WAL 重放兜底。
- **HTTP 端到端**：完整提交/拒绝/聚合/重投/位点流程，`/capacity` 与
  Decimal 字符串金额输出。

本地手动验证：

```bash
uv sync
uv run pytest -s tests/test_multi_source_decimal_capacity.py   # 只看三个新场景
uv run pytest                                                   # 全量回归（不得有既有用例失败）

PIPELINE_DATA_DIR=/tmp/mk uv run python main.py                 # 起服务
curl -s localhost:8000/capacity                                 # 查总闸占用
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"1","source":"SRC-A","symbol":"600000","price":10.10,
       "quantity":0.30,"event_time_ms":500}'
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"1","source":"SRC-B","symbol":"600000","price":20.20,
       "quantity":0.10,"event_time_ms":600}'                    # 跨来源同号也受理
curl -s localhost:8000/aggregates                               # notional/avg_price 为精确字符串
```

## 8. 代码结构

```
app/config.py    窗口、迟到、未来偏移、三档容量等确定性参数
app/models.py    事件、提交结果、窗口聚合等类型定义；Decimal 口径与均价
app/storage.py   本地文件存储：WAL / 状态快照 / 位点 / 死信（可按来源移除）
app/windows.py   窗口分配、水位线、迟到与未来判定、Decimal 确定性聚合
app/pipeline.py  校验、来源限定去重、总闸、入窗、持久化、位点、死信重投
app/api.py       FastAPI 接入层：总容量视图与精确金额字符串输出
tests/           重复/多来源/小数金额/三档容量/重启/并发/HTTP 测试
```
