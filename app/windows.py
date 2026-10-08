"""窗口分配、乱序归位、迟到判定与确定性聚合。

规则（全部确定、可解释）：
- 固定窗口：window_start = event_time_ms // size * size；
- 水位线：watermark = 已观测最大事件时间 - allowed_lateness_ms（单调不减）；
- 窗口结束时刻 <= watermark 即视为关闭，再来事件明确拒绝（WINDOW_CLOSED），
  落在允许迟到范围内的乱序事件仍归入其本来的窗口，不静默丢弃；
- 超过当前时间 max_future_skew_ms 的"未来事件"以 FUTURE_EVENT 拒绝；
- 单窗事件数达到上限以 WINDOW_FULL 拒绝；
- 聚合字段的 first/last 按 (事件时间, 来源, 事件ID) 决胜（多来源 event_id
  可能撞车，故加入 source 保证全局可比较），保证乱序下结果一致；
- 成交量/成交金额/均价全部使用 Decimal 精确十进制运算，与到达顺序无关。
"""

from __future__ import annotations

from decimal import Decimal

from app.models import MarketEvent, RejectReason, WindowAggregate


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
        agg.notional += event.price * event.quantity
        agg.event_ids.append(event.event_id)
        agg.sources.add(event.source)

        if agg.min_price is None or event.price < agg.min_price:
            agg.min_price = event.price
        if agg.max_price is None or event.price > agg.max_price:
            agg.max_price = event.price
        # first/last 以 (事件时间, 来源, 事件ID) 确定性决胜，与到达顺序无关
        key = (event.event_time_ms, event.source, event.event_id)
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
                "total_quantity": agg.total_quantity,
                "notional": agg.notional,
                "min_price": agg.min_price,
                "max_price": agg.max_price,
                "first_price": agg.first_price,
                "last_price": agg.last_price,
                "event_ids": list(agg.event_ids),
                "sources": sorted(agg.sources),
                "closed": agg.closed,
                "first_key": list(agg.first_key) if agg.first_key else None,
                "last_key": list(agg.last_key) if agg.last_key else None,
                "_key": [symbol, start],
            })
        return {"windows": windows, "max_seen_event_time": self._max_seen_event_time}

    def restore(self, state: dict) -> None:
        self._windows = {}
        self._max_seen_event_time = state.get("max_seen_event_time")
        for w in state.get("windows", []):
            agg = WindowAggregate(
                symbol=w["symbol"],
                window_start_ms=w["window_start_ms"],
                window_end_ms=w["window_end_ms"],
                count=w["count"],
                total_quantity=_D(w["total_quantity"]),
                notional=_D(w["notional"]),
                min_price=_D(w["min_price"]),
                max_price=_D(w["max_price"]),
                first_price=_D(w["first_price"]),
                last_price=_D(w["last_price"]),
                event_ids=list(w["event_ids"]),
                sources=set(w["sources"]),
                closed=w["closed"],
            )
            agg.first_key = _key(w.get("first_key"))
            agg.last_key = _key(w.get("last_key"))
            symbol, start = w["_key"]
            self._windows[(symbol, start)] = agg


def _D(value: object) -> Decimal | None:
    """把快照/WAL 中的金额字段归一化为 Decimal；兼容旧版 float/int/字符串。"""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    # 经 str 走十进制字面量，绝不从二进制 float 直接构造
    return Decimal(str(value))


def _key(value: object) -> tuple[int, str, str] | None:
    """恢复 first/last 决胜键：兼容旧版 (time, event_id) 二元组。"""
    if not value:
        return None
    if len(value) == 3:
        return (int(value[0]), str(value[1]), str(value[2]))
    # 旧快照没有 source 分量，统一归入空来源命名空间
    return (int(value[0]), "", str(value[1]))
