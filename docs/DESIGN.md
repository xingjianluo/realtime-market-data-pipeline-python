# 本地实时行情接入与窗口聚合 — 设计说明

本文档说明事件模型、去重与窗口规则、结构兼容策略，以及本地验证方法。
实现位于 `market_pipeline/`，全部状态保存在本地文件（`data/` 目录），
用「追加事件日志 + 原子快照 + 消费位点文件」模拟数据源与消息系统。

## 1. 事件模型

### 1.1 字段（schema v1）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `event_id` | 非空字符串 | 来源内唯一的事件标识 |
| `source` | 非空字符串 | 行情来源标识 |
| `symbol` | 非空字符串 | 标的代码 |
| `price` | 非负整数 | 整数计价单位（如「厘」），杜绝浮点误差 |
| `quantity` | 正整数 | 成交量 |
| `event_time_ms` | 非负整数 | 事件发生时间（毫秒） |
| `schema_version` | 整数 | 结构版本，当前支持 `1`、`2` |

金额全程为整数；VWAP 仅在查询输出时做一次除法（保留 6 位小数），
因此聚合可精确重放。

### 1.2 schema v2（演进示例）

v2 在 v1 基础上**新增可选字段** `trade_side`，取值 `"buy"` / `"sell"`。
缺省语义稳定为 `"unspecified"`：

- v1 事件不受任何影响，聚合结果与升级前逐字节一致；
- v2 事件缺省该字段时计入 `side_unspecified`，不报错；
- v2 事件给了非法值，按 `schema_type_mismatch` 拒绝。

未声明字段一律忽略，不进入聚合，避免脏数据污染。

### 1.3 处理结论（Ack）

每条事件都返回确定结论，**不存在静默丢弃**：

```json
{
  "event_id": "evt-1",
  "decision": "accepted | rejected",
  "reason": null,
  "window_start_ms": 10000,
  "detail": "接受并入窗口 [10000,20000)，来源位点 3"
}
```

拒绝原因（`reason`）枚举，彼此可区分：

| 原因 | 含义 | 可否重投 |
| --- | --- | --- |
| `duplicate` | 同一 `(source, event_id)` 已确认接受 | 幂等无意义（直接返回原窗口） |
| `schema_field_missing` | 必填字段缺失 | 补字段后重投 |
| `schema_type_mismatch` | 字段类型不符（含布尔冒充整数、非正数） | 修正后重投 |
| `schema_unknown_version` | 不支持的 schema 版本 | 升级/降级版本后重投 |
| `window_too_late` | 超出迟到容忍或窗口已关闭 | 可在放宽容忍/重开窗后重投 |
| `window_out_of_range` | 超出允许的未来时间上界 | 时钟校正后重投 |
| `window_capacity_exceeded` | 目标窗口事件数达上限 | 扩容或新窗口后重投 |
| `global_capacity_exceeded` | 全局事件数达上限 | 扩容后重投 |
| `batch_too_large` | 批量条数超上限（整批拒绝） | 拆小批次后重投 |

拒绝事件的**原始载荷原样记录在事件日志**中（`raw` 字段），
消费位点照常前进，调用方可凭日志/返回结论重新投递。

## 2. 去重与窗口规则

### 2.1 去重

- 去重键为 `(source, event_id)`：跨来源允许相同 `event_id`。
- 已**接受确认**的键再次提交：返回 `duplicate`，
  **不前进消费位点、不重复计数、不修改任何聚合**，并返回原归属窗口（幂等）。
- 被**拒绝**的事件不占用去重键：同 `event_id` 修正载荷后重投可被接受。
- 重复提交仍写一条审计日志（仅日志序号前进，来源位点不变），可解释、可追溯。

### 2.2 窗口归位

- 固定大小滚动窗口（默认 10s）：

  ```
  window_start = event_time_ms - (event_time_ms % window_size_ms)
  窗口区间     = [window_start, window_start + window_size_ms)
  ```

- 归属**只取决于事件时间**，与到达顺序无关，故乱序到达的归位是确定的。
- 窗口聚合只使用可交换归约量，乱序并入结果相同：
  - `count / sum_quantity / sum(price*quantity) / high / low`；
  - `open` = 窗口内顺序键最小者，`close` = 最大者；
    顺序键为 `(event_time_ms, source, event_id)`，同一时刻也有确定结果；
  - `vwap = sum(price*quantity) / sum(quantity)`，输出时才做除法。

### 2.3 水位线、迟到与越界

- 水位线 `watermark` 单调不减，只随显式关窗调用
  （`POST /windows/close`，入参为「处理时间」）推进。
- 迟到容忍：事件时间 `< watermark - allowed_lateness_ms` → `window_too_late`。
  容忍范围内的迟到事件正常归位并入既有窗口。
- 窗口定型：窗口结束时刻 `<= watermark` 时窗口关闭；之后到达 → `window_too_late`。
- 越界未来：事件时间 `> process_time + max_future_ms` → `window_out_of_range`。
  （`process_time_ms` 由调用方在提交时给定，保证测试与行为确定。）

### 2.4 容量策略

判定顺序固定为「结构 → 去重 → 窗口时间 → 全局容量 → 窗口容量 → 接受」：

- 全局容量 `max_events_global`：超限返回 `global_capacity_exceeded`；
- 单窗容量 `max_events_per_window`：超限返回 `window_capacity_exceeded`，
  只影响该窗口，不影响其他窗口/标的；
- 批量容量 `max_batch_size`：超限**整批拒绝**（不处理任何一条），
  每条都得到 `batch_too_large`；未超限则按提交顺序逐条给出结论（可部分成功）。

## 3. 并发与持久化

### 3.1 并发确定性

- 引擎内用一把可重入锁串行化所有写入；「检查—判定—落盘—聚合」为原子过程，
  因此高并发下接受/拒绝集合与最终聚合确定，容量名额不会超发。
- 查询为同一把锁下的一致读。

### 3.2 本地持久化（重启不丢不重）

`data_dir` 下三个文件：

| 文件 | 写法 | 内容 |
| --- | --- | --- |
| `events.log` | 追加 JSONL，每行 flush + fsync | 全部已确认记录（接受/拒绝/重复审计），含序号、位点、原始载荷 |
| `snapshot.json` | 临时文件 + `os.replace` 原子覆写 | 窗口聚合、去重键→窗口映射、去重集合、全局计数、水位线 |
| `offsets.json` | 原子覆写 | 各来源已确认的最大事件序号（消费位点） |

恢复策略：

1. 优先加载 `snapshot.json` 得到最近一致状态；
2. 无快照时从 `events.log` 重放所有「accepted」记录重建聚合；
3. 加载 `offsets.json` 恢复消费位点。

由于去重集合与聚合同时持久化，重启后重复提交仍被识别，
已确认事件与聚合状态**不重复、不丢失**。

## 4. HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /events?process_time_ms=` | 接入单条；接受 200，拒绝 422（结构化 `reason`） |
| `POST /events/batch?process_time_ms=` | 批量接入；全接受 200，含拒绝 207；超量整批拒绝 |
| `GET /windows?symbol=` | 列出窗口聚合（按窗口起点、标的排序） |
| `GET /windows/{symbol}/{window_start_ms}` | 查询单窗 |
| `POST /windows/close` | body `{"now_ms": ...}`，推进水位线并关窗 |
| `GET /offsets` | 各来源消费位点 |
| `GET /health` | 存活与当前水位线 |

启动：`uv run python main.py`（数据目录默认 `./data`）。

## 5. 验证方法

### 5.1 运行测试

```bash
uv sync          # 安装 fastapi / pytest / httpx
uv run pytest    # addopts 含 -s，直接打印每条输入事件与判定依据
```

测试矩阵（共 30 个用例）：

| 文件 | 覆盖场景 |
| --- | --- |
| `tests/test_dedup.py` | 重复提交不重复计入、跨来源同 id、改价重复幂等 |
| `tests/test_windows.py` | 乱序归位、顺序无关一致性、容忍内迟到、超迟拒绝、关窗后拒绝、未来越界 |
| `tests/test_schema_evolution.py` | v1 稳定、v2 新字段缺省/聚合、缺失 vs 类型不符、未知版本、拒绝后修正重投、未声明字段忽略 |
| `tests/test_capacity.py` | 窗口容量、全局容量、批量超限整批拒绝、混合批次原因可区分 |
| `tests/test_durability_concurrency.py` | 快照恢复、日志重放恢复、并发不丢不重、并发容量名额确定 |
| `tests/test_api.py` | HTTP 端到端接入/查询/关窗/位点/状态码 |

每个用例通过 `conftest.log_decision` 打印一行，形如：

```
用例=超出迟到容忍 | 输入={'event_id': 'too-late', ...} | 判定=rejected
| 原因=window_too_late | 窗口=None | 依据=事件时间 5000 早于水位线 40000
允许迟到 20000ms 的下界（来源位点 1，可重投）
```

即「输入事件 + 判定结论 + 判定依据」三要素齐全。

### 5.2 手工验证示例

```bash
uv run python main.py
curl -s -XPOST localhost:8000/events?process_time_ms=11000 \
  -H 'content-type: application/json' \
  -d '{"event_id":"e1","source":"s1","symbol":"AAA","price":100,
       "quantity":2,"event_time_ms":10500,"schema_version":2}'
curl -s localhost:8000/windows/AAA/10000
```
