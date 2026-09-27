"""事件结构（schema）演进与校验。

兼容策略（向后兼容、新增字段缺省语义稳定）：
- v1 字段：event_id, source, symbol, price, quantity, event_time_ms, schema_version。
- v2 在 v1 基础上新增可选字段 trade_side（"buy"|"sell"）；
  v2 事件缺省 trade_side 时稳定取 "unspecified"，不影响既有聚合。
- 未声明字段一律忽略，不进入聚合，避免脏数据污染。
- 字段缺失与类型不符给出可区分的错误码，原始载荷原样返回，可被重新投递。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from market_pipeline.model import Event, RejectReason

SUPPORTED_VERSIONS = (1, 2)
DEFAULT_TRADE_SIDE = "unspecified"
VALID_TRADE_SIDES = ("buy", "sell")

_REQUIRED_FIELDS = (
    "event_id",
    "source",
    "symbol",
    "price",
    "quantity",
    "event_time_ms",
    "schema_version",
)


@dataclass(frozen=True)
class ValidationOK:
    """校验通过：产出规范化事件。"""

    event: Event


@dataclass(frozen=True)
class ValidationError:
    """校验失败：携带可区分原因与可重投原始载荷。"""

    code: str
    detail: str
    raw: dict[str, Any]


def _is_strict_int(value: Any) -> bool:
    """bool 是 int 的子类，行情数值不接受布尔冒充。"""
    return isinstance(value, int) and not isinstance(value, bool)


def validate_event(raw: Any) -> ValidationOK | ValidationError:
    """按 schema 版本校验并规范化原始事件。"""
    if not isinstance(raw, dict):
        return ValidationError(
            code=RejectReason.SCHEMA_TYPE_MISMATCH.value,
            detail="事件载荷必须是 JSON 对象",
            raw={} if not isinstance(raw, dict) else raw,
        )

    # 1) 版本必须先可识别，否则无法解释后续字段。
    if "schema_version" not in raw:
        return ValidationError(
            RejectReason.SCHEMA_FIELD_MISSING.value,
            "缺少必填字段 schema_version",
            raw,
        )
    version = raw["schema_version"]
    if not _is_strict_int(version) or version not in SUPPORTED_VERSIONS:
        return ValidationError(
            RejectReason.SCHEMA_UNKNOWN_VERSION.value,
            f"不支持的 schema_version={version!r}，支持 {SUPPORTED_VERSIONS}",
            raw,
        )

    # 2) 按固定顺序检查必填字段：先缺失，后类型，保证判定稳定可解释。
    for name in _REQUIRED_FIELDS:
        if name not in raw:
            return ValidationError(
                RejectReason.SCHEMA_FIELD_MISSING.value,
                f"缺少必填字段 {name}",
                raw,
            )

    event_id = raw["event_id"]
    source = raw["source"]
    symbol = raw["symbol"]
    for name, value in (
        ("event_id", event_id),
        ("source", source),
        ("symbol", symbol),
    ):
        if not isinstance(value, str) or value == "":
            return ValidationError(
                RejectReason.SCHEMA_TYPE_MISMATCH.value,
                f"字段 {name} 必须是非空字符串，实际为 {value!r}",
                raw,
            )

    price = raw["price"]
    quantity = raw["quantity"]
    event_time_ms = raw["event_time_ms"]
    if not _is_strict_int(price) or price < 0:
        return ValidationError(
            RejectReason.SCHEMA_TYPE_MISMATCH.value,
            f"字段 price 必须是非负整数，实际为 {price!r}",
            raw,
        )
    if not _is_strict_int(quantity) or quantity <= 0:
        return ValidationError(
            RejectReason.SCHEMA_TYPE_MISMATCH.value,
            f"字段 quantity 必须是正整数，实际为 {quantity!r}",
            raw,
        )
    if not _is_strict_int(event_time_ms) or event_time_ms < 0:
        return ValidationError(
            RejectReason.SCHEMA_TYPE_MISMATCH.value,
            f"字段 event_time_ms 必须是非负整数，实际为 {event_time_ms!r}",
            raw,
        )

    # 3) v2 新增可选字段：缺省语义稳定；非法值仍按类型不符拒绝。
    extras: dict[str, Any] = {}
    if version >= 2:
        if "trade_side" in raw:
            trade_side = raw["trade_side"]
            if trade_side not in VALID_TRADE_SIDES:
                return ValidationError(
                    RejectReason.SCHEMA_TYPE_MISMATCH.value,
                    f"v2 字段 trade_side 必须是 {VALID_TRADE_SIDES} 之一，"
                    f"实际为 {trade_side!r}",
                    raw,
                )
            extras["trade_side"] = trade_side
        else:
            extras["trade_side"] = DEFAULT_TRADE_SIDE

    return ValidationOK(
        Event(
            event_id=event_id,
            source=source,
            symbol=symbol,
            price=price,
            quantity=quantity,
            event_time_ms=event_time_ms,
            schema_version=version,
            extras=extras,
        )
    )
