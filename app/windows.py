"""窗口分配、乱序归位、迟到判定与确定性聚合。

规则（全部确定、可解释）：
- 固定窗口：window_start = event_time_ms // size * size；
- 水位线：watermark = 已观测最大事件时间 - allowed_lateness_ms（单调不减）；
- 窗口结束时刻 <= watermark 即视为关闭，再来事件明确拒绝（WINDOW_CLOSED），
  落在允许迟到范围内的乱序事件仍归入其本来的窗口，不静默丢弃；
- 超过当前时间 max_future_skew_ms 的"未来事件"以 FUTURE_EVENT 拒绝；
- 单窗事件数达到上限以 WINDOW_FULL 拒绝；
- 聚合字段的 first/last 按 (事件时间, 来源, 事件ID) 决胜：多来源允许
  event_id 撞号，加入 source 后跨来源仍为全序，结果与到达顺序无关；
- 数量/金额全程 Decimal 精确运算，杜绝二进制浮点的累加漂移。
"""

from __future__ import annotations

from decimal import Decimal

from app.models import (
    MarketEvent,
    RejectReason,
    WindowAggregate,
    decimal_to_json,
    to_decimal,
)


def _event_key(event: MarketEvent) -> tuple[int, str, str]:
    return (event.event_time_ms, event.source, event.event_id)


def _event_ref(event: MarketEvent) -> str:
    return f"{event.source}/{event.event_id}"


class WindowManager:
    """按标的维护固定时间窗与聚合状态。"""

    def __init__(self, window_size_ms: int, allowed_lateness_ms: int,
                 max_future_skew_ms: int, max_events_per_window: int) -> None:
        self.window_size_ms = window_size_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self.max_future_skew_ms = max_future_skew_ms
        self.max_events_per_window = max_events_per_window
        # key: (symbol, window_start_ms)
        self._windows: dict[tuple[str, int], WindowAggregate] = {}
        self._max_seen_event_time: int | None = None

    def window_start_for(self, event_time_ms: int) -> int:
        return (event_time_ms // self.window_size_ms) * self.window_size_ms

    def watermark(self) -> int | None:
        if self._max_seen_event_time is None:
            return None
        return self._max_seen_event_time - self.allowed_lateness_ms

    def add_event(self, event: MarketEvent,
                  now_ms: int) -> tuple[bool, RejectReason | None, str]:
        """尝试将事件归入窗口。返回 (是否接受, 拒绝原因, 说明)。"""
        start = self.window_start_for(event.event_time_ms)
        end = start + self.window_size_ms

        # 1) 未来事件（越界）：相对"已观测最大事件时间"判定；
        #    首批事件无历史时以当前系统时间为基准。
        reference = (self._max_seen_event_time
                     if self._max_seen_event_time is not None else now_ms)
        if event.event_time_ms > reference + self.max_future_skew_ms:
            return (False, RejectReason.FUTURE_EVENT,
                    f"event_time={event.event_time_ms} 超前基准时间 {reference} "
                    f"超过允许偏移 {self.max_future_skew_ms}ms，窗口起点 {start}")

        # 2) 窗口已关闭：watermark 已越过窗口结束时刻
        wm = self.watermark()
        if wm is not None and end <= wm:
            return (False, RejectReason.WINDOW_CLOSED,
                    f"事件归属窗口 [{start},{end}) 已关闭，watermark={wm}，"
                    f"超出允许迟到 {self.allowed_lateness_ms}ms")

        # 3) 容量受限
        agg = self._windows.get((event.symbol, start))
        if agg is not None and agg.count >= self.max_events_per_window:
            return (False, RejectReason.WINDOW_FULL,
                    f"窗口 [{start},{end}) 标的 {event.symbol} 已达容量上限 "
                    f"{self.max_events_per_window}")

        # 4) 接受并更新聚合
        if agg is None:
            agg = WindowAggregate(symbol=event.symbol,
                                  window_start_ms=start,
                                  window_end_ms=end)
            self._windows[(event.symbol, start)] = agg
        self._fold(agg, event)

        # 5) 推进水位线（单调不减）
        if (self._max_seen_event_time is None
                or event.event_time_ms > self._max_seen_event_time):
            self._max_seen_event_time = event.event_time_ms

        return (True, None, f"归入窗口 [{start},{end})")

    @staticmethod
    def _fold(agg: WindowAggregate, event: MarketEvent) -> None:
        agg.count += 1
        agg.total_quantity += event.quantity
        # Decimal 精确乘法：金额不产生二进制舍入，累加顺序不影响最终值
        agg.notional += event.price * event.quantity
        agg.event_ids.append(event.event_id)
        agg.event_refs.append(_event_ref(event))
        agg.sources.add(event.source)

        if agg.min_price is None or event.price < agg.min_price:
            agg.min_price = event.price
        if agg.max_price is None or event.price > agg.max_price:
            agg.max_price = event.price
        # first/last 以 (事件时间, 来源, 事件ID) 确定性决胜，与到达顺序无关
        key = _event_key(event)
        if agg.first_key is None or key < agg.first_key:
            agg.first_price = event.price
            agg.first_key = key
        if agg.last_key is None or key > agg.last_key:
            agg.last_price = event.price
            agg.last_key = key

    def close_expired(self, now_ms: int) -> list[WindowAggregate]:
        """关闭所有已越过水位线的窗口，返回被关闭的聚合（确定性排序）。"""
        wm = self.watermark()
        if wm is None:
            return []
        closed: list[WindowAggregate] = []
        for agg in self._windows.values():
            if not agg.closed and agg.window_end_ms <= wm:
                agg.closed = True
                closed.append(agg)
        closed.sort(key=lambda a: (a.symbol, a.window_start_ms))
        return closed

    def get_aggregate(self, symbol: str, window_start_ms: int) -> WindowAggregate | None:
        return self._windows.get((symbol, window_start_ms))

    def all_aggregates(self) -> list[WindowAggregate]:
        return [self._windows[k] for k in sorted(self._windows.keys())]

    # ---- 快照与恢复 ------------------------------------------------------
    def restore_fold(self, event: MarketEvent) -> None:
        """重启重放 WAL 时使用：只做归窗与聚合，不做迟到/容量校验。"""
        start = self.window_start_for(event.event_time_ms)
        end = start + self.window_size_ms
        agg = self._windows.get((event.symbol, start))
        if agg is None:
            agg = WindowAggregate(symbol=event.symbol,
                                  window_start_ms=start,
                                  window_end_ms=end)
            self._windows[(event.symbol, start)] = agg
        self._fold(agg, event)
        if (self._max_seen_event_time is None
                or event.event_time_ms > self._max_seen_event_time):
            self._max_seen_event_time = event.event_time_ms

    def snapshot(self) -> dict:
        windows = []
        for (symbol, start), agg in self._windows.items():
            windows.append({
                "symbol": agg.symbol,
                "window_start_ms": agg.window_start_ms,
                "window_end_ms": agg.window_end_ms,
                "count": agg.count,
                # Decimal 以精确十进制字符串落盘，重启读回仍是同一个数
                "total_quantity": decimal_to_json(agg.total_quantity),
                "notional": decimal_to_json(agg.notional),
                "min_price": decimal_to_json(agg.min_price),
                "max_price": decimal_to_json(agg.max_price),
                "first_price": decimal_to_json(agg.first_price),
                "last_price": decimal_to_json(agg.last_price),
                "event_ids": list(agg.event_ids),
                "event_refs": list(agg.event_refs),
                "sources": sorted(agg.sources),
                "closed": agg.closed,
                "first_key": list(agg.first_key) if agg.first_key else None,
                "last_key": list(agg.last_key) if agg.last_key else None,
                "_key": [symbol, start],
            })
        return {"windows": windows, "max_seen_event_time": self._max_seen_event_time}

    @staticmethod
    def _load_decimal(value: object) -> Decimal:
        """快照/旧 WAL 读回：字符串/数字统一转精确 Decimal。"""
        return to_decimal(value)

    def restore(self, state: dict) -> None:
        self._windows = {}
        self._max_seen_event_time = state.get("max_seen_event_time")
        for w in state.get("windows", []):
            agg = WindowAggregate(
                symbol=w["symbol"],
                window_start_ms=w["window_start_ms"],
                window_end_ms=w["window_end_ms"],
                count=w["count"],
                total_quantity=self._load_decimal(w["total_quantity"]),
                notional=self._load_decimal(w["notional"]),
                min_price=(self._load_decimal(w["min_price"])
                           if w.get("min_price") is not None else None),
                max_price=(self._load_decimal(w["max_price"])
                           if w.get("max_price") is not None else None),
                first_price=(self._load_decimal(w["first_price"])
                             if w.get("first_price") is not None else None),
                last_price=(self._load_decimal(w["last_price"])
                            if w.get("last_price") is not None else None),
                event_ids=list(w["event_ids"]),
                event_refs=list(w.get("event_refs", [])),
                sources=set(w["sources"]),
                closed=w["closed"],
            )
            fk = w.get("first_key")
            lk = w.get("last_key")
            agg.first_key = self._restore_key(fk, agg.event_ids, agg.event_refs)
            agg.last_key = self._restore_key(lk, agg.event_ids, agg.event_refs)
            symbol, start = w["_key"]
            self._windows[(symbol, start)] = agg

    @staticmethod
    def _restore_key(key: object, event_ids: list[str],
                     event_refs: list[str]) -> tuple[int, str, str] | None:
        """决胜键读回；兼容旧版二元键 (time, event_id)，来源补空串。"""
        if key is None:
            return None
        if len(key) == 3:
            return (int(key[0]), str(key[1]), str(key[2]))
        # 旧快照：尝试按 event_id 反查来源，反查不到则用空串占位
        eid = str(key[1])
        source = ""
        for ref in event_refs:
            sid, _, rid = ref.partition("/")
            if rid == eid:
                source = sid
                break
        return (int(key[0]), source, eid)
