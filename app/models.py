"""事件模型、拒绝原因与聚合结果的类型定义。

事件结构演进策略：
- schema_version=1 的字段为 event_id/source/symbol/price/quantity/event_time_ms；
- schema_version=2 新增可选字段 venue（缺省语义稳定为 "DEFAULT"）；
- 未知的高版本 schema 一律以 UNSUPPORTED_SCHEMA 拒绝，待服务升级后可重新投递。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, localcontext
from enum import Enum

# 金额/数量/均价统一使用 Decimal 高精度十进制运算，杜绝二进制浮点的累加漂移。
# 精度取 40 位：足以容纳大额金额累加（28 位有效数字）后再做均价除法。
MONEY_PRECISION = 40
# 均价对外输出的固定小数位（四舍五入，半数向上，结果与到达顺序无关）
AVG_PRICE_QUANTUM = Decimal("0.0000000001")  # 1e-10


def to_decimal(value: object) -> Decimal:
    """把外部数值（int/float/str/Decimal）转为精确 Decimal。

    float 必须经 str() 转换，直接 Decimal(0.1) 会带入二进制噪声；
    转换结果等价于用户实际提交的十进制字面量。
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    return Decimal(str(value))


def decimal_to_json(value: Decimal | None) -> str | None:
    """Decimal 的 JSON 序列化口径：输出精确十进制字符串（不转回 float）。"""
    if value is None:
        return None
    return format(value, "f")


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
    SERVICE_FULL = "SERVICE_FULL"
    AMBIGUOUS_REDELIVERY = "AMBIGUOUS_REDELIVERY"
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


@dataclass(frozen=True)
class SubmitResult:
    status: EventStatus
    event_id: str
    reason: RejectReason | None = None
    detail: str = ""
    window_start_ms: int | None = None
    source: str = ""


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
    # 来源限定引用 "source/event_id"：多来源下编号可能撞号，
    # 仅凭 event_id 无法定位归属来源，故并列保存规范引用。
    event_refs: list[str] = field(default_factory=list)
    sources: set[str] = field(default_factory=set)
    closed: bool = False
    # first/last 价的确定性决胜键 (事件时间, 来源, 事件ID)
    # 多来源允许 event_id 撞号，故必须把 source 纳入决胜键，保证跨来源也有全序。
    first_key: tuple[int, str, str] | None = None
    last_key: tuple[int, str, str] | None = None

    @property
    def avg_price(self) -> Decimal | None:
        """成交金额加权的平均成交价 = 累计金额 / 累计数量。

        全程 Decimal 精确运算，并量化到固定 1e-10 位（ROUND_HALF_UP），
        保证同一批事件无论到达顺序、重复提交与否，输出完全一致；
        累计数量为 0（如仅有 0 数量报价）时返回 None。
        """
        if self.total_quantity == 0:
            return None
        with localcontext() as ctx:
            ctx.prec = MONEY_PRECISION
            avg = self.notional / self.total_quantity
        return avg.quantize(AVG_PRICE_QUANTUM)
