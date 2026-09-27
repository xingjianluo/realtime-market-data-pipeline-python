"""事件模型、拒绝原因与聚合结果的类型定义。

事件结构演进策略：
- schema_version=1 的字段为 event_id/source/symbol/price/quantity/event_time_ms；
- schema_version=2 新增可选字段 venue（缺省语义稳定为 "DEFAULT"）；
- 未知的高版本 schema 一律以 UNSUPPORTED_SCHEMA 拒绝，待服务升级后可重新投递。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


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
    DUPLICATE_ID_CONFLICT = "DUPLICATE_ID_CONFLICT"
    STORAGE_ERROR = "STORAGE_ERROR"


@dataclass(frozen=True)
class MarketEvent:
    event_id: str
    source: str
    symbol: str
    price: float
    quantity: float
    event_time_ms: int
    schema_version: int = 1
    venue: str = "DEFAULT"


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
    total_quantity: float = 0.0
    notional: float = 0.0
    min_price: float | None = None
    max_price: float | None = None
    first_price: float | None = None
    last_price: float | None = None
    event_ids: list[str] = field(default_factory=list)
    sources: set[str] = field(default_factory=set)
    closed: bool = False
    # first/last 价的确定性决胜键 (事件时间, 事件ID)
    first_key: tuple[int, str] | None = None
    last_key: tuple[int, str] | None = None
