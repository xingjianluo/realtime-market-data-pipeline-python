"""管线引擎：校验、去重、窗口归位、容量控制与持久化。

处理顺序（固定，任何输入都落到确定分支，不静默丢弃）：
1. 结构校验：版本 -> 必填缺失 -> 类型不符（原因各自可区分，原始载荷可重投）。
2. 重复判定：(source, event_id) 已确认接受过 -> duplicate，幂等返回原窗口。
3. 窗口归位：按事件时间取窗口；早于「水位线 - 允许迟到」-> window_too_late；
   晚于「处理时间 + 允许未来」-> window_out_of_range；目标窗口已关闭 -> window_too_late。
4. 容量判定：先全局后窗口，拒绝原因可区分。
5. 接受并入聚合并持久化（事件日志 + 快照 + 位点，全部 fsync/原子替换）。

水位线单调不减，只随显式的关窗调用（处理时间）推进；重启后从快照恢复。
"""
from __future__ import annotations

import threading
from typing import Any

from market_pipeline.model import (
    Ack,
    Decision,
    PipelineConfig,
    RejectReason,
    accept,
    reject,
)
from market_pipeline.schema import ValidationError, validate_event
from market_pipeline.storage import LocalStorage
from market_pipeline.windows import WindowManager, assign_window


class PipelineEngine:
    """串行化写入、保证确定性与重启一致性。"""

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._lock = threading.RLock()
        self._storage = LocalStorage(config.data_dir)
        self._windows = WindowManager(config.window_size_ms)
        self._accepted_keys: set[str] = set()
        self._key_windows: dict[str, int] = {}
        self._accepted_windows: dict[str, int] = {}
        self._total_accepted = 0
        self._watermark_ms = 0
        self._offsets: dict[str, int] = {}
        self._restore()

    # ---- 恢复 -----------------------------------------------------------

    def _restore(self) -> None:
        """重启恢复：快照为准，日志兜底重放已接受事件。"""
        snapshot = self._storage.load_snapshot()
        if snapshot is not None:
            self._windows.restore(snapshot)
            self._accepted_keys = set(snapshot.get("accepted_keys", []))
            self._key_windows = dict(snapshot.get("key_windows", {}))
            self._total_accepted = int(snapshot.get("total_accepted", 0))
            self._watermark_ms = int(snapshot.get("watermark_ms", 0))
            for row in self._windows.list_windows():
                if row.event_count > 0:
                    self._accepted_windows[row.key.composite()] = row.event_count
        else:
            for record in self._storage.load_log():
                if record.get("decision") != Decision.ACCEPTED.value:
                    continue
                event = self._event_from_record(record)
                ws = int(record["window_start_ms"])
                self._windows.add(event, ws)
                self._accepted_keys.add(record["dedup_key"])
                self._key_windows[record["dedup_key"]] = ws
                self._accepted_windows[
                    f"{event.symbol}\x00{ws}"
                ] = self._accepted_windows.get(f"{event.symbol}\x00{ws}", 0) + 1
                self._total_accepted += 1
        self._offsets = self._storage.load_offsets()

    @staticmethod
    def _event_from_record(record: dict[str, Any]):
        from market_pipeline.model import Event

        ev = record["event"]
        return Event(
            event_id=ev["event_id"],
            source=ev["source"],
            symbol=ev["symbol"],
            price=int(ev["price"]),
            quantity=int(ev["quantity"]),
            event_time_ms=int(ev["event_time_ms"]),
            schema_version=int(ev["schema_version"]),
            extras=dict(ev.get("extras", {})),
        )

    # ---- 持久化 ---------------------------------------------------------

    def _persist_snapshot(self) -> None:
        snapshot = self._windows.snapshot()
        snapshot["accepted_keys"] = sorted(self._accepted_keys)
        snapshot["key_windows"] = dict(sorted(self._key_windows.items()))
        snapshot["total_accepted"] = self._total_accepted
        snapshot["watermark_ms"] = self._watermark_ms
        self._storage.save_snapshot(snapshot)
        self._storage.save_offsets(self._offsets)

    def _next_offset(self, source: str) -> int:
        offset = self._offsets.get(source, 0) + 1
        self._offsets[source] = offset
        return offset

    # ---- 接入 -----------------------------------------------------------

    def ingest(self, raw: dict[str, Any], process_time_ms: int | None = None) -> Ack:
        """接入单条原始事件并给出确定结论。"""
        with self._lock:
            # 1) 结构校验
            result = validate_event(raw)
            if isinstance(result, ValidationError):
                event_id = (
                    str(raw.get("event_id", "")) if isinstance(raw, dict) else ""
                )
                return self._reject_persist(
                    event_id or "<unknown>",
                    RejectReason(result.code),
                    result.detail,
                    result.raw,
                )

            event = result.event

            # 2) 重复判定（幂等：不前进位点、不重复计数、返回原归属窗口）
            if event.dedup_key() in self._accepted_keys:
                ws = self._key_windows[event.dedup_key()]
                detail = (
                    f"事件 {event.dedup_key()} 已确认，归属窗口 {ws}，幂等返回"
                )
                # 审计留痕（seq 前进、来源位点不前进），不改动任何聚合。
                self._storage.append_log(
                    {
                        "offset": None,
                        "source": event.source,
                        "dedup_key": event.dedup_key(),
                        "decision": Decision.REJECTED.value,
                        "reason": RejectReason.DUPLICATE.value,
                        "window_start_ms": ws,
                        "event": self._event_to_dict(event),
                        "raw": raw,
                    }
                )
                return reject(event.event_id, RejectReason.DUPLICATE, detail)

            # 3) 窗口归位
            cfg = self._config
            ws = assign_window(event.event_time_ms, cfg.window_size_ms)
            window_end = ws + cfg.window_size_ms

            if self._windows.is_closed(event.symbol, ws):
                return self._reject_persist(
                    event.event_id,
                    RejectReason.WINDOW_TOO_LATE,
                    f"窗口 [{ws},{window_end}) 已关闭定型，拒绝迟到事件",
                    raw,
                    event=event,
                    window_start_ms=ws,
                )
            if self._watermark_ms > 0 and event.event_time_ms < (
                self._watermark_ms - cfg.allowed_lateness_ms
            ):
                return self._reject_persist(
                    event.event_id,
                    RejectReason.WINDOW_TOO_LATE,
                    f"事件时间 {event.event_time_ms} 早于水位线 "
                    f"{self._watermark_ms} 允许迟到 {cfg.allowed_lateness_ms}ms 的下界",
                    raw,
                    event=event,
                    window_start_ms=ws,
                )
            if process_time_ms is not None and event.event_time_ms > (
                process_time_ms + cfg.max_future_ms
            ):
                return self._reject_persist(
                    event.event_id,
                    RejectReason.WINDOW_OUT_OF_RANGE,
                    f"事件时间 {event.event_time_ms} 超出处理时间 "
                    f"{process_time_ms} 允许未来 {cfg.max_future_ms}ms 的上界",
                    raw,
                    event=event,
                    window_start_ms=ws,
                )

            # 4) 容量判定（先全局，后窗口；原因可区分）
            if self._total_accepted >= cfg.max_events_global:
                return self._reject_persist(
                    event.event_id,
                    RejectReason.GLOBAL_CAPACITY_EXCEEDED,
                    f"全局已接受事件数达上限 {cfg.max_events_global}",
                    raw,
                    event=event,
                    window_start_ms=ws,
                )
            window_count = self._accepted_windows.get(
                f"{event.symbol}\x00{ws}", 0
            )
            if window_count >= cfg.max_events_per_window:
                return self._reject_persist(
                    event.event_id,
                    RejectReason.WINDOW_CAPACITY_EXCEEDED,
                    f"窗口 {event.symbol}@{ws} 已达 {cfg.max_events_per_window} 条上限",
                    raw,
                    event=event,
                    window_start_ms=ws,
                )

            # 5) 接受：位点 -> 日志 -> 内存聚合 -> 快照
            offset = self._next_offset(event.source)
            self._storage.append_log(
                {
                    "offset": offset,
                    "source": event.source,
                    "dedup_key": event.dedup_key(),
                    "decision": Decision.ACCEPTED.value,
                    "reason": None,
                    "window_start_ms": ws,
                    "event": self._event_to_dict(event),
                    "raw": raw,
                }
            )
            self._windows.add(event, ws)
            self._accepted_keys.add(event.dedup_key())
            self._key_windows[event.dedup_key()] = ws
            self._accepted_windows[f"{event.symbol}\x00{ws}"] = window_count + 1
            self._total_accepted += 1
            self._persist_snapshot()
            return accept(
                event.event_id,
                ws,
                f"接受并入窗口 [{ws},{window_end})，来源位点 {offset}",
            )

    def _reject_persist(
        self,
        event_id: str,
        reason: RejectReason,
        detail: str,
        raw: Any,
        *,
        event: Any = None,
        window_start_ms: int | None = None,
    ) -> Ack:
        """记录拒绝（重复除外）：前进位点、落日志，保证不静默丢弃且可重投。"""
        source = event.source if event is not None else (
            raw.get("source", "unknown") if isinstance(raw, dict) else "unknown"
        )
        source = source if isinstance(source, str) and source else "unknown"
        offset = self._next_offset(source)
        self._storage.append_log(
            {
                "offset": offset,
                "source": source,
                "dedup_key": event.dedup_key() if event is not None else None,
                "decision": Decision.REJECTED.value,
                "reason": reason.value,
                "window_start_ms": window_start_ms,
                "event": None if event is None else self._event_to_dict(event),
                "raw": raw if isinstance(raw, dict) else {"_non_object": repr(raw)},
            }
        )
        self._persist_snapshot()
        return reject(event_id, reason, f"{detail}（来源位点 {offset}，可重投）")

    @staticmethod
    def _event_to_dict(event: Any) -> dict[str, Any]:
        return {
            "event_id": event.event_id,
            "source": event.source,
            "symbol": event.symbol,
            "price": event.price,
            "quantity": event.quantity,
            "event_time_ms": event.event_time_ms,
            "schema_version": event.schema_version,
            "extras": dict(event.extras),
        }

    def ingest_batch(
        self,
        raws: list[dict[str, Any]],
        process_time_ms: int | None = None,
    ) -> list[Ack]:
        """接入一批事件：超批量整体拒绝；否则按提交顺序逐条处理（确定）。"""
        with self._lock:
            if len(raws) > self._config.max_batch_size:
                return [
                    reject(
                        str(raw.get("event_id", i)) if isinstance(raw, dict) else str(i),
                        RejectReason.BATCH_TOO_LARGE,
                        f"批量 {len(raws)} 条超过上限 "
                        f"{self._config.max_batch_size}，整批拒绝（未处理任何一条）",
                    )
                    for i, raw in enumerate(raws)
                ]
            return [self.ingest(raw, process_time_ms) for raw in raws]

    # ---- 查询 / 关窗 ----------------------------------------------------

    def query_window(self, symbol: str, window_start_ms: int) -> dict[str, Any] | None:
        """查询窗口聚合。"""
        with self._lock:
            agg = self._windows.get(symbol, window_start_ms)
            return None if agg is None else agg.to_dict()

    def list_windows(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """列出窗口聚合（按窗口起点、标的确定排序）。"""
        with self._lock:
            return [a.to_dict() for a in self._windows.list_windows(symbol)]

    def close_windows(self, now_ms: int) -> list[dict[str, Any]]:
        """按处理时间推进水位线（单调不减）并关闭到期窗口。"""
        with self._lock:
            if now_ms > self._watermark_ms:
                self._watermark_ms = now_ms
            closed = self._windows.close_before(self._watermark_ms)
            self._persist_snapshot()
            return [
                {"symbol": k.symbol, "window_start_ms": k.window_start_ms}
                for k in closed
            ]

    def get_offsets(self) -> dict[str, int]:
        """返回各来源已确认位点。"""
        with self._lock:
            return dict(sorted(self._offsets.items()))

    def get_watermark(self) -> int:
        with self._lock:
            return self._watermark_ms

    def close(self) -> None:
        with self._lock:
            self._storage.close()
