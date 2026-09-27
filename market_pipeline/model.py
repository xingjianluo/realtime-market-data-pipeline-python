"""事件模型与通用类型定义。

约定：
- 金额相关一律用整数计价单位（如「厘」），避免浮点误差；VWAP 在输出层才做除法。
- 事件标识在同一来源内唯一；跨来源允许相同 event_id。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Decision(str, Enum):
    """单条事件的判定结果。"""

    ACCEPTED = "accepted"
    REJECTED = "rejected"


class RejectReason(str, Enum):
    """拒绝原因，必须可区分、稳定且可被重新投递。

    - DUPLICATE：同一 (source, event_id) 已确认过（重复提交）。
    - SCHEMA_FIELD_MISSING：必填字段缺失。
    - SCHEMA_TYPE_MISMATCH：字段类型不符（含非有限数值、布尔冒充整数等）。
    - SCHEMA_UNKNOWN_VERSION：schema_version 不被支持。
    - WINDOW_TOO_LATE：事件时间早于水位线允许的迟到范围（窗口已定型）。
    - WINDOW_OUT_OF_RANGE：事件时间超出允许的未来范围。
    - WINDOW_CAPACITY_EXCEEDED：目标窗口事件数达到上限。
    - GLOBAL_CAPACITY_EXCEEDED：全局事件数达到上限。
    - BATCH_TOO_LARGE：批量提交条数超过上限。
    """

    DUPLICATE = "duplicate"
    SCHEMA_FIELD_MISSING = "schema_field_missing"
    SCHEMA_TYPE_MISMATCH = "schema_type_mismatch"
    SCHEMA_UNKNOWN_VERSION = "schema_unknown_version"
    WINDOW_TOO_LATE = "window_too_late"
    WINDOW_OUT_OF_RANGE = "window_out_of_range"
    WINDOW_CAPACITY_EXCEEDED = "window_capacity_exceeded"
    GLOBAL_CAPACITY_EXCEEDED = "global_capacity_exceeded"
    BATCH_TOO_LARGE = "batch_too_large"


@dataclass(frozen=True)
class Event:
    """规范化后的行情事件。"""

    event_id: str
    source: str
    symbol: str
    price: int
    quantity: int
    event_time_ms: int
    schema_version: int
    extras: dict[str, Any] = field(default_factory=dict)

    def dedup_key(self) -> str:
        """去重键：来源 + 事件标识。"""
        return f"{self.source}\x00{self.event_id}"

    def order_key(self) -> tuple[int, str, str]:
        """窗口内排序键：事件时间优先，其次来源、事件标识，保证确定性。"""
        return (self.event_time_ms, self.source, self.event_id)


@dataclass(frozen=True)
class WindowKey:
    """窗口键：标的 + 窗口起始时间。"""

    symbol: str
    window_start_ms: int

    def composite(self) -> str:
        return f"{self.symbol}\x00{self.window_start_ms}"


@dataclass
class WindowAggregate:
    """单个时间窗的聚合状态。

    全部字段都是可交换归约量，乱序并入结果相同：
    - open/close 通过 (event_time, source, event_id) 的最小/最大确定；
    - high/low 为极值；vwap 保留分子分母，最后才做除法。
    """

    key: WindowKey
    event_count: int = 0
    total_quantity: int = 0
    vwap_numerator: int = 0
    high_price: int | None = None
    low_price: int | None = None
    open_price: int | None = None
    close_price: int | None = None
    open_order_key: tuple[int, str, str] | None = None
    close_order_key: tuple[int, str, str] | None = None
    open_event_id: str | None = None
    close_event_id: str | None = None
    side_buy: int = 0
    side_sell: int = 0
    side_unspecified: int = 0
    closed: bool = False

    def merge(self, event: Event) -> None:
        """将一条事件并入窗口聚合（仅修改可归约量，与并入顺序无关）。"""
        self.event_count += 1
        self.total_quantity += event.quantity
        self.vwap_numerator += event.price * event.quantity

        if self.high_price is None or event.price > self.high_price:
            self.high_price = event.price
        if self.low_price is None or event.price < self.low_price:
            self.low_price = event.price

        order_key = event.order_key()
        if self.open_order_key is None or order_key < self.open_order_key:
            self.open_order_key = order_key
            self.open_price = event.price
            self.open_event_id = event.event_id
        if self.close_order_key is None or order_key > self.close_order_key:
            self.close_order_key = order_key
            self.close_price = event.price
            self.close_event_id = event.event_id

        side = str(event.extras.get("trade_side", "unspecified"))
        if side == "buy":
            self.side_buy += 1
        elif side == "sell":
            self.side_sell += 1
        else:
            self.side_unspecified += 1

    def vwap(self) -> float | None:
        """成交量加权均价，保留 6 位小数；无成交量时为 None。"""
        if self.total_quantity == 0:
            return None
        return round(self.vwap_numerator / self.total_quantity, 6)

    def to_dict(self) -> dict[str, Any]:
        """转为可序列化字典。"""
        return {
            "symbol": self.key.symbol,
            "window_start_ms": self.key.window_start_ms,
            "event_count": self.event_count,
            "total_quantity": self.total_quantity,
            "vwap": self.vwap(),
            "vwap_numerator": self.vwap_numerator,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "open_price": self.open_price,
            "close_price": self.close_price,
            "open_event_id": self.open_event_id,
            "close_event_id": self.close_event_id,
            "side_buy": self.side_buy,
            "side_sell": self.side_sell,
            "side_unspecified": self.side_unspecified,
            "closed": self.closed,
        }


@dataclass(frozen=True)
class Ack:
    """单条事件的处理结论。"""

    event_id: str
    decision: Decision
    reason: RejectReason | None
    window_start_ms: int | None
    detail: str

    def to_dict(self) -> dict[str, Any]:
        """转为可序列化字典。"""
        return {
            "event_id": self.event_id,
            "decision": self.decision.value,
            "reason": None if self.reason is None else self.reason.value,
            "window_start_ms": self.window_start_ms,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class PipelineConfig:
    """管线静态配置。"""

    window_size_ms: int = 10_000
    allowed_lateness_ms: int = 30_000
    max_future_ms: int = 60_000
    max_events_per_window: int = 100
    max_events_global: int = 10_000
    max_batch_size: int = 500
    data_dir: str = "./data"


def reject(event_id: str, reason: RejectReason, detail: str) -> Ack:
    """构造拒绝结论。"""
    return Ack(
        event_id=event_id,
        decision=Decision.REJECTED,
        reason=reason,
        window_start_ms=None,
        detail=detail,
    )


def accept(event_id: str, window_start_ms: int, detail: str) -> Ack:
    """构造接受结论。"""
    return Ack(
        event_id=event_id,
        decision=Decision.ACCEPTED,
        reason=None,
        window_start_ms=window_start_ms,
        detail=detail,
    )
