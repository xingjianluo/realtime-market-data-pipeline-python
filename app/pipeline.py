"""核心流水线：结构校验、去重、入窗、持久化、位点推进与拒绝记录重投。

确定性保证：
- 同一 event_id 的重复提交返回 DUPLICATE，绝不重复计入；
- event_id 相同但内容不同返回 DUPLICATE_ID_CONFLICT，绝不静默覆盖；
- 乱序事件按 event_time 归入其本来窗口；迟到事件明确拒绝（WINDOW_CLOSED）
  而非静默丢弃，全部拒绝都落死信日志，可通过 /dead-letters/{id}/redeliver 重投；
- 批量超限整批拒绝（BATCH_TOO_LARGE），单窗满员拒绝（WINDOW_FULL）；
- 每次接受先写 WAL(fsync)，再更新内存，再原子写快照与位点，重启不丢不重。
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
    """除 event_id 外字段的规范化内容签名，用于冲突检测。"""
    return json.dumps({
        "source": event.source,
        "symbol": event.symbol,
        "price": event.price,
        "quantity": event.quantity,
        "event_time_ms": event.event_time_ms,
        "schema_version": event.schema_version,
        "venue": event.venue,
    }, sort_keys=True, ensure_ascii=False)


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
        # event_id -> 已确认事件的规范化内容签名
        self._dedup: dict[str, str] = {}
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
            price=float(raw["price"]),
            quantity=float(raw["quantity"]),
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

        # 2) 去重 / 冲突
        existing = self._dedup.get(event.event_id)
        if existing is not None:
            signature = canonical_payload(event)
            if signature == existing:
                return SubmitResult(
                    status=EventStatus.DUPLICATE,
                    event_id=event.event_id,
                    detail="event_id 已确认，内容一致，按重复提交幂等忽略，不重复计入")
            result = self._reject(
                raw, event.event_id, RejectReason.DUPLICATE_ID_CONFLICT,
                "event_id 已存在但内容不同，拒绝覆盖以保证已确认聚合不被污染")
            return result

        # 3) 入窗（迟到/未来/容量判定）
        now_ms = self._clock()
        accepted, reason, detail = self.wm.add_event(event, now_ms)
        if not accepted:
            assert reason is not None
            return self._reject(raw, event.event_id, reason, detail)

        # 4) 持久化：WAL -> 内存去重 -> 快照 -> 位点
        self.storage.append_wal({
            "event_id": event.event_id,
            "source": event.source,
            "offset": raw.get("offset"),
            "event": self._event_to_dict(event),
        })
        self._dedup[event.event_id] = canonical_payload(event)
        self._advance_offset(raw, event)
        self._persist_state()

        window_start = self.wm.window_start_for(event.event_time_ms)
        return SubmitResult(
            status=EventStatus.ACCEPTED,
            event_id=event.event_id,
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
    def _reject(self, raw: dict, event_id: str | None,
                reason: RejectReason, detail: str) -> SubmitResult:
        eid = event_id or (raw.get("event_id") if isinstance(raw, dict) else None) or ""
        record = {
            "event_id": eid,
            "reason": reason.value,
            "detail": detail,
            "raw": raw if isinstance(raw, dict) else {"_non_object": repr(raw)},
            "committed_at_ms": self._clock(),
        }
        self.storage.append_dead_letter(record)
        return SubmitResult(status=EventStatus.REJECTED, event_id=eid,
                            reason=reason, detail=detail)

    def dead_letters(self) -> list[dict]:
        with self._lock:
            return self.storage.load_dead_letters()

    def redeliver(self, event_id: str) -> SubmitResult:
        """从死信中取出原始记录重新投递；成功或幂等重复后移除死信条目。"""
        with self._lock:
            for rec in self.storage.load_dead_letters():
                if rec.get("event_id") == event_id:
                    result = self._submit_locked(rec["raw"])
                    if result.status in (EventStatus.ACCEPTED, EventStatus.DUPLICATE):
                        self.storage.remove_dead_letter(event_id)
                    return result
            return SubmitResult(
                status=EventStatus.REJECTED, event_id=event_id,
                reason=RejectReason.MISSING_FIELD,
                detail="死信中不存在该 event_id，无需重投")

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

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #
    @staticmethod
    def _event_to_dict(event: MarketEvent) -> dict:
        return {
            "event_id": event.event_id,
            "source": event.source,
            "symbol": event.symbol,
            "price": event.price,
            "quantity": event.quantity,
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
            "dedup": self._dedup,
            "offsets": self._offsets,
            "windows": self.wm.snapshot(),
        })
        self.storage.save_offsets(self._offsets)

    def _restore(self) -> None:
        state = self.storage.load_state()
        if state is not None:
            self._dedup = dict(state.get("dedup", {}))
            self._offsets = dict(state.get("offsets", {}))
            self.wm.restore(state["windows"])
            return
        # 快照缺失时从 WAL 重放（兜底）
        for rec in self.storage.load_wal():
            event = MarketEvent(**rec["event"])
            self.wm.restore_fold(event)
            self._dedup[event.event_id] = canonical_payload(event)
            offset = rec.get("offset")
            if _is_nonneg_int(offset):
                prev = self._offsets.get(event.source, -1)
                if offset > prev:
                    self._offsets[event.source] = offset
        if self._dedup:
            self._persist_state()

    def close(self) -> None:
        with self._lock:
            self.storage.close()
