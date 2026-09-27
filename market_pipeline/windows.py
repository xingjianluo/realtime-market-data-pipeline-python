"""时间窗分配与窗口聚合。

窗口规则：
- 固定大小滚动窗口，window_start = event_time - (event_time % window_size)，
  窗口区间为 [start, start + window_size)，归属完全由「事件时间」决定，
  与到达顺序无关，因此乱序到达归位结果确定。
- 聚合只使用可交换归约量；open/close 用窗口内确定性顺序键取极值。
"""
from __future__ import annotations

from typing import Any

from market_pipeline.model import Event, WindowAggregate, WindowKey


def assign_window(event_time_ms: int, window_size_ms: int) -> int:
    """按事件时间向下取整到窗口起点。"""
    return (event_time_ms // window_size_ms) * window_size_ms


class WindowManager:
    """管理各标的各窗口的聚合状态。"""

    def __init__(self, window_size_ms: int) -> None:
        self._window_size_ms = window_size_ms
        self._windows: dict[str, WindowAggregate] = {}

    def _key(self, symbol: str, window_start_ms: int) -> WindowKey:
        return WindowKey(symbol=symbol, window_start_ms=window_start_ms)

    def add(self, event: Event, window_start_ms: int) -> WindowAggregate:
        """把已接受事件并入指定窗口（窗口不存在则创建）。"""
        wk = self._key(event.symbol, window_start_ms)
        agg = self._windows.get(wk.composite())
        if agg is None:
            agg = WindowAggregate(key=wk)
            self._windows[wk.composite()] = agg
        agg.merge(event)
        return agg

    def get(self, symbol: str, window_start_ms: int) -> WindowAggregate | None:
        """查询某窗口聚合。"""
        return self._windows.get(self._key(symbol, window_start_ms).composite())

    def is_closed(self, symbol: str, window_start_ms: int) -> bool:
        """窗口是否已关闭定型。"""
        agg = self.get(symbol, window_start_ms)
        return bool(agg is not None and agg.closed)

    def close_before(self, watermark_ms: int) -> list[WindowKey]:
        """关闭「窗口结束时刻 <= 水位线」的窗口，返回本次关闭的窗口键。

        已关闭窗口保持关闭；其聚合结果继续可查询但不再接受事件。
        """
        closed_keys: list[WindowKey] = []
        for agg in self._windows.values():
            if agg.closed:
                continue
            if agg.key.window_start_ms + self._window_size_ms <= watermark_ms:
                agg.closed = True
                closed_keys.append(agg.key)
        return closed_keys

    def list_windows(self, symbol: str | None = None) -> list[WindowAggregate]:
        """按 (window_start, symbol) 确定序列出窗口聚合。"""
        items = list(self._windows.values())
        if symbol is not None:
            items = [a for a in items if a.key.symbol == symbol]
        items.sort(key=lambda a: (a.key.window_start_ms, a.key.symbol))
        return items

    def snapshot(self) -> dict[str, Any]:
        """导出可持久化快照（顺序键转为列表以兼容 JSON）。"""
        return {
            "window_size_ms": self._window_size_ms,
            "windows": [
                {
                    **agg.to_dict(),
                    "open_order_key": (
                        None
                        if agg.open_order_key is None
                        else list(agg.open_order_key)
                    ),
                    "close_order_key": (
                        None
                        if agg.close_order_key is None
                        else list(agg.close_order_key)
                    ),
                }
                for agg in self._windows.values()
            ],
        }

    def restore(self, snapshot: dict[str, Any]) -> None:
        """从快照恢复窗口聚合状态。"""
        self._window_size_ms = int(snapshot["window_size_ms"])
        self._windows = {}
        for row in snapshot.get("windows", []):
            wk = self._key(row["symbol"], int(row["window_start_ms"]))
            agg = WindowAggregate(
                key=wk,
                event_count=int(row["event_count"]),
                total_quantity=int(row["total_quantity"]),
                vwap_numerator=int(row["vwap_numerator"]),
                high_price=row["high_price"],
                low_price=row["low_price"],
                open_price=row["open_price"],
                close_price=row["close_price"],
                open_order_key=(
                    None
                    if row.get("open_order_key") is None
                    else tuple(row["open_order_key"])
                ),
                close_order_key=(
                    None
                    if row.get("close_order_key") is None
                    else tuple(row["close_order_key"])
                ),
                open_event_id=row.get("open_event_id"),
                close_event_id=row.get("close_event_id"),
                side_buy=int(row.get("side_buy", 0)),
                side_sell=int(row.get("side_sell", 0)),
                side_unspecified=int(row.get("side_unspecified", 0)),
                closed=bool(row.get("closed", False)),
            )
            self._windows[wk.composite()] = agg
