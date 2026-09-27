"""乱序到达、迟到、越界窗口场景：归位确定，拒绝原因可区分，不静默丢弃。"""
from __future__ import annotations

from market_pipeline.model import RejectReason
from tests.conftest import log_decision, make_event


def test_out_of_order_events_aggregate_deterministically(engine):
    # 同一窗口 [10000,20000)，到达顺序与事件时间相反
    late_arrival = make_event("later-time", 12_000, price=110, quantity=1)
    early_arrival = make_event("earlier-time", 10_500, price=100, quantity=3)
    r1 = engine.ingest(late_arrival, process_time_ms=15_000)
    r2 = engine.ingest(early_arrival, process_time_ms=15_000)
    log_decision("先到（时间较晚）", late_arrival, r1)
    log_decision("后到（时间较早，乱序）", early_arrival, r2)
    assert r1.window_start_ms == r2.window_start_ms == 10_000

    agg = engine.query_window("AAA", 10_000)
    assert agg["event_count"] == 2
    assert agg["total_quantity"] == 4
    # vwap=(110*1+100*3)/4=102.5；open/close 由事件时间决定而非到达顺序
    assert agg["vwap"] == 102.5
    assert agg["open_price"] == 100
    assert agg["open_event_id"] == "earlier-time"
    assert agg["close_price"] == 110
    assert agg["close_event_id"] == "later-time"


def test_order_independence_same_aggregate(engine):
    # 两批相同事件、相反提交顺序，聚合结果必须一致
    events = [
        make_event(f"e{i}", 10_000 + i * 100, price=100 + i, quantity=i + 1)
        for i in range(5)
    ]
    for e in events:
        engine.ingest(e, process_time_ms=20_000)
    forward = engine.query_window("AAA", 10_000)

    engine2 = type(engine)(engine._config)  # noqa: SLF001
    try:
        for e in reversed(events):
            engine2.ingest(e, process_time_ms=20_000)
        backward = engine2.query_window("AAA", 10_000)
        assert forward == backward
    finally:
        engine2.close()


def test_late_within_lateness_is_accepted(engine):
    # 先推进水位线到 30000；allowed_lateness=20000，事件时间 15000 仍在容忍内
    engine.close_windows(30_000)
    raw = make_event("late-ok", 15_000, price=101)
    r = engine.ingest(raw, process_time_ms=30_000)
    log_decision("容忍范围内迟到", raw, r, note=f"watermark={engine.get_watermark()}")
    assert r.decision.value == "accepted"
    assert r.window_start_ms == 10_000


def test_too_late_event_is_rejected_with_distinct_reason(engine):
    engine.close_windows(40_000)
    raw = make_event("too-late", 5_000, price=1)
    r = engine.ingest(raw, process_time_ms=40_000)
    log_decision("超出迟到容忍", raw, r, note=f"watermark={engine.get_watermark()}")
    assert r.decision.value == "rejected"
    assert r.reason == RejectReason.WINDOW_TOO_LATE
    # 拒绝不污染任何既有聚合
    assert engine.query_window("AAA", 0) is None
    # 拒绝有位点记录，可重投（重投到更宽容配置可被接受）
    assert engine.get_offsets()["src-a"] == 1


def test_closed_window_rejects_late_event(engine):
    engine.ingest(make_event("in-win", 10_500), process_time_ms=15_000)
    engine.close_windows(40_000)  # [10000,20000) 结束时刻 20000 <= 40000
    raw = make_event("after-close", 10_800)
    r = engine.ingest(raw, process_time_ms=40_000)
    log_decision("窗口关闭后到达", raw, r)
    assert r.reason == RejectReason.WINDOW_TOO_LATE
    assert engine.query_window("AAA", 10_000)["event_count"] == 1


def test_future_event_out_of_range_rejected(engine):
    # max_future=30000，处理时间 10000，事件时间 45000 越界
    raw = make_event("future", 45_000)
    r = engine.ingest(raw, process_time_ms=10_000)
    log_decision("超出未来上界", raw, r)
    assert r.reason == RejectReason.WINDOW_OUT_OF_RANGE
