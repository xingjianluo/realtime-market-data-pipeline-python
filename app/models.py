"""事件模型、拒绝原因与聚合结果的类型定义。

事件结构演进策略：
- schema_version=1 的字段为 event_id/source/symbol/price/quantity/event_time_ms；
- schema_version=2 新增可选字段 venue（缺省语义稳定为 "DEFAULT"）；
- 未知的高版本 schema 一律以 UNSUPPORTED_SCHEMA 拒绝，待服务升级后可重新投递。

编号语义（多来源）：
- 去重主键是**来源内复合键 (source, event_id)**；不同来源即使 event_id 相同，
  也是互相独立的事件，分别受理、互不覆盖、互不干扰；
- 同一来源内部：event_id 相同且内容一致按 DUPLICATE 幂等忽略，
  内容不一致以 DUPLICATE_ID_CONFLICT 明确拒绝。

金额口径：
- price/quantity 以及全部累计金额、均价一律用 Decimal 精确十进制表示，
  不使用二进制浮点，保证任意到达顺序、任意重复提交下结果完全一致。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from enum import Enum

# 金额/均价全程精确十进制；提高运算精度，覆盖大额累计与除均价场景
getcontext().prec = 50


class EventStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    REJECTED = "REJECTED"


class RejectReason(str, Enum):
    MISSING_FIELD = "MISSING_FIELD"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    UNSUPPORTED_SCHEMA = "UNSUPPORTED_SCHEMA"
    WINDOW_CLOSED = "WINDOW_CLOSED"
    FUTURE_EVENT = "FUTURE_EVENT"
    WINDOW_FULL = "WINDOW_FULL"
    BATCH_TOO_LARGE = "BATCH_TOO_LARGE"
    SERVICE_FULL = "SERVICE_FULL"
    DUPLICATE_ID_CONFLICT = "DUPLICATE_ID_CONFLICT"
    STORAGE_ERROR = "STORAGE_ERROR"


@dataclass(frozen=True)
class MarketEvent:
    event_id: str
    source: str
    symbol: str
    price: Decimal
    quantity: Decimal
    event_time_ms: int
    schema_version: int = 1
    venue: str = "DEFAULT"

    @property
    def key(self) -> tuple[str, str]:
        """来源内事件唯一键：去重只在同一 source 内部判定。"""
        return (self.source, self.event_id)


@dataclass(frozen=True)
class SubmitResult:
    status: EventStatus
    event_id: str
    reason: RejectReason | None = None
    detail: str = ""
    window_start_ms: int | None = None


@dataclass
class WindowAggregate:
    symbol: str
    window_start_ms: int
    window_end_ms: int
    count: int = 0
    total_quantity: Decimal = Decimal(0)
    notional: Decimal = Decimal(0)
    min_price: Decimal | None = None
    max_price: Decimal | None = None
    first_price: Decimal | None = None
    last_price: Decimal | None = None
    event_ids: list[str] = field(default_factory=list)
    sources: set[str] = field(default_factory=set)
    closed: bool = False
    # first/last 价的确定性决胜键 (事件时间, 来源, 事件ID)
    first_key: tuple[int, str, str] | None = None
    last_key: tuple[int, str, str] | None = None

    @property
    def avg_price(self) -> Decimal | None:
        """成交量加权平均成交价 = 累计成交金额 / 累计成交量；零成交量时为 None。"""
        if self.count == 0 or self.total_quantity == 0:
            return None
        return self.notional / self.total_quantity
