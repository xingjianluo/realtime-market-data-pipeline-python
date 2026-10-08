"""核心流水线：结构校验、去重、入窗、持久化、位点推进与拒绝记录重投。

确定性保证：
- 去重作用域为"同一来源内部"：唯一键为 (source, event_id)。不同来源即使
  event_id 相同也各自独立受理、互不覆盖；同一来源内重复提交返回 DUPLICATE，
  绝不重复计入，内容不同返回 DUPLICATE_ID_CONFLICT，绝不静默覆盖；
- 乱序事件按 event_time 归入其本来窗口；迟到事件明确拒绝（WINDOW_CLOSED）
  而非静默丢弃，全部拒绝都落死信日志，可通过 /dead-letters/{id}/redeliver 重投；
- 批量超限整批拒绝（BATCH_TOO_LARGE），单窗满员拒绝（WINDOW_FULL）；
  服务级总闸到顶拒绝 SERVICE_FULL（跨来源跨窗口累计，与前两档明确区分）；
- 金额/数量全程 Decimal 精确运算，同一批事件的累计金额与均价与到达顺序、
  重复提交无关；
- 每次接受先写 WAL(fsync)，再更新内存，再原子写快照与位点，重启不丢不重，
  总额度占用随快照恢复，不凭空增减。
"""

from __future__ import annotations

import json
import threading
import time
from typing import Callable

from app.config import Config
from app.models import (
    EventStatus,
    MarketEvent,
    RejectReason,
    SubmitResult,
    WindowAggregate,
    to_decimal,
)
from app.storage import LocalStorage
from app.windows import WindowManager

# 每个 schema 版本必填/类型固定的字段（新增版本只在此处显式登记）
_BASE_FIELDS: dict[str, type | tuple[type, ...]] = {
    "event_id": str,
    "source": str,
    "symbol": str,
    "price": (int, float),
    "quantity": (int, float),
    "event_time_ms": int,
}
# 不同版本新增字段：缺省语义稳定
_NEW_FIELD_DEFAULTS: dict[int, dict[str, object]] = {
    2: {"venue": "DEFAULT"},
}
_SUPPORTED_VERSIONS = {1, 2}
# venue 为 v2 新增"可选"字段：缺失时采用稳定缺省，而非拒绝
_REQUIRED_BY_VERSION: dict[int, set[str]] = {
    1: set(_BASE_FIELDS),
    2: set(_BASE_FIELDS),
}


class ValidationError(Exception):
    """结构校验失败，携带可区分的拒绝原因。"""

    def __init__(self, reason: RejectReason, detail: str,
                 event_id: str | None = None) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail
        self.event_id = event_id


def _is_nonneg_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_finite_number(value: object) -> bool:
    return (isinstance(value, (int, float))
            and not isinstance(value, bool)
            and value == value  # NaN 判定
            and value not in (float("inf"), float("-inf")))


def canonical_payload(event: MarketEvent) -> str:
    """同来源内除 event_id/source 外字段的规范化内容签名，用于冲突检测。

    Decimal 以精确十进制字符串规范化（normalize 去尾零，如 2.0 与 2.00 视为一致），
    保证按数值口径比较而非按浮点表示比较。
    """
    return json.dumps({
        "symbol": event.symbol,
        # 数值按十进制标量值规范化：1.0 与 1.00 视为同一内容
        "price": format(event.price.normalize(), "f"),
        "quantity": format(event.quantity.normalize(), "f"),
        "event_time_ms": event.event_time_ms,
        "schema_version": event.schema_version,
        "venue": event.venue,
    }, sort_keys=True, ensure_ascii=False)


def dedup_key(source: str, event_id: str) -> str:
    """来源限定去重键：不同来源编号撞车互不影响，同来源内才判定同一事件。"""
    return f"{source}/{event_id}"


class Pipeline:
    """行情接入与窗口聚合流水线。"""

    def __init__(self, config: Config | None = None,
                 clock: Callable[[], int] | None = None) -> None:
        self.config = config or Config()
        self._clock = clock or (lambda: int(time.time() * 1000))
        self.storage = LocalStorage(self.config.data_dir)
        self.storage.setup()
        self.wm = WindowManager(
            window_size_ms=self.config.window_size_ms,
            allowed_lateness_ms=self.config.allowed_lateness_ms,
            max_future_skew_ms=self.config.max_future_skew_ms,
            max_events_per_window=self.config.max_events_per_window,
        )
        self._lock = threading.RLock()
        # "source/event_id" -> 已确认事件的规范化内容签名
        # （去重作用域仅限同一来源内部，跨来源编号撞车互不影响）
        self._dedup: dict[str, str] = {}
        # 服务级总闸：已受理事件总数（跨来源、跨窗口累计，含重启恢复的占用）
        self._total_accepted: int = 0
        # source -> 已确认最大 offset（模拟消费位点）
        self._offsets: dict[str, int] = self.storage.load_offsets()
        self._restore()

    # ------------------------------------------------------------------ #
    # 结构解析（纯函数，失败抛 ValidationError）
    # ------------------------------------------------------------------ #
    @staticmethod
    def parse_event(raw: object) -> MarketEvent:
        if not isinstance(raw, dict):
            raise ValidationError(RejectReason.TYPE_MISMATCH,
                                  f"事件必须是 JSON 对象，实际为 {type(raw).__name__}")

        version = raw.get("schema_version", 1)
        if not isinstance(version, int) or isinstance(version, bool):
            raise ValidationError(RejectReason.TYPE_MISMATCH,
                                  f"schema_version 必须为整数，实际为 {type(version).__name__}")
        if version not in _SUPPORTED_VERSIONS:
            raise ValidationError(
                RejectReason.UNSUPPORTED_SCHEMA,
                f"schema_version={version} 不受支持，支持版本 {sorted(_SUPPORTED_VERSIONS)}，"
                f"待服务升级后可重新投递")

        eid = raw.get("event_id")

        # 必填字段缺失（含空白字符串）
        for name in _REQUIRED_BY_VERSION[version]:
            if name not in raw:
                raise ValidationError(RejectReason.MISSING_FIELD,
                                      f"schema v{version} 缺少必填字段 {name}", eid)
            value = raw[name]
            if isinstance(value, str) and value.strip() == "":
                raise ValidationError(RejectReason.MISSING_FIELD,
                                      f"字段 {name} 为空白字符串，视同缺失", eid)

        # 基础字段类型校验
        for name, expected in _BASE_FIELDS.items():
            value = raw[name]
            if not isinstance(value, expected) or isinstance(value, bool):
                raise ValidationError(
                    RejectReason.TYPE_MISMATCH,
                    f"字段 {name} 类型不符：期望 {expected}，实际 {type(value).__name__}",
                    eid)

        if not _is_nonneg_int(raw["event_time_ms"]):
            raise ValidationError(RejectReason.TYPE_MISMATCH,
                                  "event_time_ms 必须为非负整数", eid)
        if not _is_finite_number(raw["price"]) or raw["price"] <= 0:
            raise ValidationError(RejectReason.TYPE_MISMATCH,
                                  "price 必须为有限正数", eid)
        if not _is_finite_number(raw["quantity"]) or raw["quantity"] < 0:
            raise ValidationError(RejectReason.TYPE_MISMATCH,
                                  "quantity 必须为非负有限数", eid)

        # 新版本字段
        venue = "DEFAULT"
        if version >= 2:
            if "venue" in raw:
                venue = raw["venue"]
                if not isinstance(venue, str):
                    raise ValidationError(RejectReason.TYPE_MISMATCH,
                                          f"字段 venue 类型不符：期望 str，"
                                          f"实际 {type(venue).__name__}", eid)
                if venue.strip() == "":
                    venue = "DEFAULT"

        return MarketEvent(
            event_id=raw["event_id"],
            source=raw["source"],
            symbol=raw["symbol"],
            # 经 str() 转精确 Decimal，避免 float 二进制噪声进入金额口径
            price=to_decimal(raw["price"]),
            quantity=to_decimal(raw["quantity"]),
            event_time_ms=raw["event_time_ms"],
            schema_version=version,
            venue=venue,
        )

    # ------------------------------------------------------------------ #
    # 提交
    # ------------------------------------------------------------------ #
    def submit(self, raw: dict) -> SubmitResult:
        with self._lock:
            return self._submit_locked(raw)

    def _submit_locked(self, raw: dict) -> SubmitResult:
        # 1) 结构校验
        try:
            event = self.parse_event(raw)
        except ValidationError as exc:
            return self._reject(raw, exc.event_id, exc.reason, exc.detail)

        key = dedup_key(event.source, event.event_id)

        # 2) 同来源内去重 / 冲突（跨来源同号事件在此之前即已各自独立）
        existing = self._dedup.get(key)
        if existing is not None:
            signature = canonical_payload(event)
            if signature == existing or self._legacy_signature_match(existing, event):
                return SubmitResult(
                    status=EventStatus.DUPLICATE,
                    event_id=event.event_id,
                    source=event.source,
                    detail="同来源内 event_id 已确认，内容一致，按重复提交幂等忽略，"
                           "不重复计入")
            result = self._reject(
                raw, event.event_id, RejectReason.DUPLICATE_ID_CONFLICT,
                "同来源内 event_id 已存在但内容不同，拒绝覆盖以保证已确认聚合不被污染",
                source=event.source)
            return result

        # 3) 服务级总闸：跨来源、跨窗口统一计数。
        #    放在入窗判定之前：总闸到顶即拒，绝不超发。
        limit = self.config.max_accepted_events
        if limit is not None and self._total_accepted >= limit:
            return self._reject(
                raw, event.event_id, RejectReason.SERVICE_FULL,
                f"服务级总容量已达上限 {limit}（已受理 {self._total_accepted} 条，"
                f"跨来源跨窗口累计），事件被总闸拒绝",
                source=event.source)

        # 4) 入窗（迟到/未来/单窗容量判定）
        now_ms = self._clock()
        accepted, reason, detail = self.wm.add_event(event, now_ms)
        if not accepted:
            assert reason is not None
            return self._reject(raw, event.event_id, reason, detail,
                                source=event.source)

        # 5) 持久化：WAL -> 内存去重/总额度 -> 快照 -> 位点
        self.storage.append_wal({
            "event_id": event.event_id,
            "source": event.source,
            "offset": raw.get("offset"),
            "event": self._event_to_dict(event),
        })
        self._dedup[key] = canonical_payload(event)
        self._total_accepted += 1
        self._advance_offset(raw, event)
        self._persist_state()

        window_start = self.wm.window_start_for(event.event_time_ms)
        return SubmitResult(
            status=EventStatus.ACCEPTED,
            event_id=event.event_id,
            source=event.source,
            window_start_ms=window_start,
            detail=detail)

    def submit_batch(self, raw_events: list) -> list[SubmitResult]:
        """批量提交：超过 max_submit_batch 整批拒绝（原因可区分），不做部分处理。"""
        with self._lock:
            if not isinstance(raw_events, list):
                return [self._reject({}, None, RejectReason.TYPE_MISMATCH,
                                     "批量请求体必须是数组")]
            if len(raw_events) > self.config.max_submit_batch:
                msg = (f"批量大小 {len(raw_events)} 超过上限 "
                       f"{self.config.max_submit_batch}，整批拒绝")
                ids = [r.get("event_id") for r in raw_events if isinstance(r, dict)]
                results = [SubmitResult(status=EventStatus.REJECTED,
                                        event_id=eid or "",
                                        reason=RejectReason.BATCH_TOO_LARGE,
                                        detail=msg) for eid in ids]
                return results
            return [self._submit_locked(raw) for raw in raw_events]

    # ------------------------------------------------------------------ #
    # 死信与重投
    # ------------------------------------------------------------------ #
    @staticmethod
    def _legacy_signature_match(stored: str, event: MarketEvent) -> bool:
        """兼容旧版本（float 口径）的内容签名，使升级后重复提交仍判 DUPLICATE。"""
        legacy = json.dumps({
            "source": event.source,
            "symbol": event.symbol,
            "price": float(event.price),
            "quantity": float(event.quantity),
            "event_time_ms": event.event_time_ms,
            "schema_version": event.schema_version,
            "venue": event.venue,
        }, sort_keys=True, ensure_ascii=False)
        return stored == legacy

    def _reject(self, raw: dict, event_id: str | None,
                reason: RejectReason, detail: str,
                source: str = "") -> SubmitResult:
        eid = event_id or (raw.get("event_id") if isinstance(raw, dict) else None) or ""
        src = source or (raw.get("source") if isinstance(raw, dict) else "") or ""
        record = {
            "event_id": eid,
            "source": src,
            "reason": reason.value,
            "detail": detail,
            "raw": raw if isinstance(raw, dict) else {"_non_object": repr(raw)},
            "committed_at_ms": self._clock(),
        }
        self.storage.append_dead_letter(record)
        return SubmitResult(status=EventStatus.REJECTED, event_id=eid,
                            reason=reason, detail=detail, source=src)

    def dead_letters(self) -> list[dict]:
        with self._lock:
            return self.storage.load_dead_letters()

    def redeliver(self, event_id: str, source: str = "") -> SubmitResult:
        """从死信中取出原始记录重新投递。

        多来源下编号可撞号：不指定 source 且该 event_id 在多个来源的死信中
        同时存在时，以 AMBIGUOUS_REDELIVERY 拒绝，要求显式带来源；
        成功或幂等重复后移除对应死信条目。
        """
        with self._lock:
            records = [rec for rec in self.storage.load_dead_letters()
                       if rec.get("event_id") == event_id]
            if source:
                records = [rec for rec in records
                           if rec.get("source", "") == source]
            if len(records) > 1:
                sources = sorted({rec.get("source", "") for rec in records})
                return SubmitResult(
                    status=EventStatus.REJECTED, event_id=event_id,
                    reason=RejectReason.AMBIGUOUS_REDELIVERY,
                    detail=f"event_id={event_id} 在多个来源 {sources} 的死信中均存在，"
                           f"重投必须显式指定 source 以消除歧义")
            if not records:
                return SubmitResult(
                    status=EventStatus.REJECTED, event_id=event_id,
                    reason=RejectReason.MISSING_FIELD,
                    detail="死信中不存在该 event_id（或该来源下不存在），无需重投",
                    source=source)
            rec = records[0]
            result = self._submit_locked(rec["raw"])
            if result.status in (EventStatus.ACCEPTED, EventStatus.DUPLICATE):
                self.storage.remove_dead_letter(
                    event_id, rec.get("source", ""))
            return result

    # ------------------------------------------------------------------ #
    # 查询与窗口关闭
    # ------------------------------------------------------------------ #
    def close_expired(self, now_ms: int | None = None) -> list[WindowAggregate]:
        with self._lock:
            closed = self.wm.close_expired(now_ms if now_ms is not None else self._clock())
            self._persist_state()
            return closed

    def aggregates(self) -> list[WindowAggregate]:
        with self._lock:
            return self.wm.all_aggregates()

    def watermark(self) -> int | None:
        with self._lock:
            return self.wm.watermark()

    def offsets(self) -> dict[str, int]:
        with self._lock:
            return dict(self._offsets)

    def capacity(self) -> dict:
        """服务级总闸的当前占用与上限。"""
        with self._lock:
            return {
                "accepted": self._total_accepted,
                "limit": self.config.max_accepted_events,
                "remaining": (None if self.config.max_accepted_events is None
                              else self.config.max_accepted_events - self._total_accepted),
            }

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #
    @staticmethod
    def _event_to_dict(event: MarketEvent) -> dict:
        return {
            "event_id": event.event_id,
            "source": event.source,
            "symbol": event.symbol,
            # Decimal 精确十进制落盘（不用 float，避免读回时引入二进制噪声）
            "price": format(event.price, "f"),
            "quantity": format(event.quantity, "f"),
            "event_time_ms": event.event_time_ms,
            "schema_version": event.schema_version,
            "venue": event.venue,
        }

    def _advance_offset(self, raw: dict, event: MarketEvent) -> None:
        offset = raw.get("offset")
        if _is_nonneg_int(offset):
            prev = self._offsets.get(event.source, -1)
            if offset > prev:
                self._offsets[event.source] = offset

    def _persist_state(self) -> None:
        self.storage.save_state({
            "version": 2,
            "dedup": self._dedup,
            "total_accepted": self._total_accepted,
            "offsets": self._offsets,
            "windows": self.wm.snapshot(),
        })
        self.storage.save_offsets(self._offsets)

    def _restore(self) -> None:
        state = self.storage.load_state()
        if state is not None:
            windows_state = state["windows"]
            self._restore_dedup(state.get("dedup", {}), windows_state)
            self._offsets = dict(state.get("offsets", {}))
            # 总额度优先取快照计数；旧快照无该字段时由窗口计数兜底
            self._total_accepted = state.get("total_accepted")
            if self._total_accepted is None:
                self._total_accepted = sum(
                    w.get("count", 0) for w in windows_state.get("windows", []))
            self.wm.restore(windows_state)
            return
        # 快照缺失时从 WAL 重放（兜底）
        for rec in self.storage.load_wal():
            event = self._event_from_wal(rec["event"])
            self.wm.restore_fold(event)
            self._dedup[dedup_key(event.source, event.event_id)] = (
                canonical_payload(event))
            self._total_accepted += 1
            offset = rec.get("offset")
            if _is_nonneg_int(offset):
                prev = self._offsets.get(event.source, -1)
                if offset > prev:
                    self._offsets[event.source] = offset
        if self._dedup:
            self._persist_state()

    def _restore_dedup(self, stored: dict, windows_state: dict) -> None:
        """恢复来源限定去重表，兼容旧版仅以 event_id 为键的快照。

        旧条目无来源前缀，且内容签名是 float 口径：借助窗口快照中的
        event_refs/event_ids 反查来源后归位，原样保留旧签名，提交时通过
        _legacy_signature_match 比对，使升级后同来源重复提交仍判 DUPLICATE；
        反查不到来源的条目挂到空来源键，新来源的同号事件不受其影响。
        """
        # event_id -> 来源（多来源撞号时取任一，仍保留各自旧键冲突无法还原
        # 的极少数情况：旧版本本来就不允许跨来源同号，数据里不会存在）
        id_to_source: dict[str, str] = {}
        for w in windows_state.get("windows", []):
            refs = w.get("event_refs") or []
            ids = w.get("event_ids") or []
            if refs:
                for ref in refs:
                    src, _, eid = ref.partition("/")
                    id_to_source.setdefault(eid, src)
            else:
                for eid in ids:
                    id_to_source.setdefault(eid, "")
        for key, signature in stored.items():
            if isinstance(key, str) and "/" in key:
                self._dedup[key] = signature
            else:
                source = id_to_source.get(key, "")
                self._dedup[dedup_key(source, key)] = signature

    @staticmethod
    def _event_from_wal(data: dict) -> MarketEvent:
        """从 WAL 记录还原事件，兼容早期以 JSON 数字落盘的 Decimal 值。"""
        return MarketEvent(
            event_id=data["event_id"],
            source=data["source"],
            symbol=data["symbol"],
            price=to_decimal(data["price"]),
            quantity=to_decimal(data["quantity"]),
            event_time_ms=data["event_time_ms"],
            schema_version=data.get("schema_version", 1),
            venue=data.get("venue", "DEFAULT"),
        )

    def close(self) -> None:
        with self._lock:
            self.storage.close()
