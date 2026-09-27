"""容量受限：窗口容量、全局容量、批量超限原因各自可区分，策略确定。"""
from __future__ import annotations

from market_pipeline.model import PipelineConfig, RejectReason
from market_pipeline.engine import PipelineEngine
from tests.conftest import log_decision, make_event


def build(tmp_path, **overrides):
    cfg = PipelineConfig(
        data_dir=str(tmp_path / "data"),
        window_size_ms=10_000,
        allowed_lateness_ms=0,
        max_future_ms=10**9,
        max_events_per_window=overrides.pop("max_events_per_window", 2),
        max_events_global=overrides.pop("max_events_global", 10),
        max_batch_size=3,
    )
    return PipelineEngine(cfg)


def test_window_capacity_rejected_with_distinct_reason(tmp_path):
    eng = build(tmp_path)
    try:
        for i in range(2):
            raw = make_event(f"w{i}", 10_500 + i, price=100 + i)
            r = eng.ingest(raw, process_time_ms=11_000)
            log_decision("窗口容量内", raw, r)
            assert r.decision.value == "accepted"
        overflow = make_event("overflow", 10_700)
        ro = eng.ingest(overflow, process_time_ms=11_000)
        log_decision("超出窗口容量", overflow, ro)
        assert ro.reason == RejectReason.WINDOW_CAPACITY_EXCEEDED
        agg = eng.query_window("AAA", 10_000)
        assert agg["event_count"] == 2
    finally:
        eng.close()


def test_other_window_unaffected_by_window_capacity(tmp_path):
    # 一个窗口超容不影响同标的其他窗口/其他标的
    eng = build(tmp_path)
    try:
        eng.ingest(make_event("a1", 10_100), process_time_ms=30_000)
        eng.ingest(make_event("a2", 10_200), process_time_ms=30_000)
        blocked = eng.ingest(make_event("a3", 10_300), process_time_ms=30_000)
        other_win = eng.ingest(make_event("b1", 20_300), process_time_ms=30_000)
        other_sym = eng.ingest(
            make_event("c1", 10_300, symbol="BBB"), process_time_ms=30_000
        )
        log_decision("满窗", make_event("a3", 10_300), blocked)
        log_decision("另一窗口", make_event("b1", 20_300), other_win)
        assert blocked.reason == RejectReason.WINDOW_CAPACITY_EXCEEDED
        assert other_win.decision.value == "accepted"
        assert other_sym.decision.value == "accepted"
    finally:
        eng.close()


def test_global_capacity_rejected_with_distinct_reason(tmp_path):
    eng = build(tmp_path, max_events_per_window=100, max_events_global=3)
    try:
        raws = [
            make_event("g1", 10_100, symbol="AAA"),
            make_event("g2", 20_100, symbol="BBB"),
            make_event("g3", 30_100, symbol="CCC"),
        ]
        for raw in raws:
            r = eng.ingest(raw, process_time_ms=40_000)
            log_decision("全局容量内", raw, r)
            assert r.decision.value == "accepted"
        overflow = make_event("g4", 10_200, symbol="AAA")
        ro = eng.ingest(overflow, process_time_ms=40_000)
        log_decision("超出全局容量", overflow, ro)
        assert ro.reason == RejectReason.GLOBAL_CAPACITY_EXCEEDED
    finally:
        eng.close()


def test_batch_too_large_rejected_entirely(tmp_path):
    eng = build(tmp_path)
    try:
        batch = [make_event(f"b{i}", 10_100 + i) for i in range(4)]
        acks = eng.ingest_batch(batch, process_time_ms=11_000)
        for raw, ack in zip(batch, acks):
            log_decision("超量批量提交", raw, ack)
            assert ack.reason == RejectReason.BATCH_TOO_LARGE
        # 整批拒绝：没有任何事件进入聚合或窗口计数
        assert eng.list_windows() == []
    finally:
        eng.close()


def test_batch_partial_rejection_reasons_distinguishable(tmp_path):
    eng = build(tmp_path)
    try:
        batch = [
            make_event("ok1", 10_100),
            make_event("dup", 10_200),
            {"event_id": "bad", "source": "src-a", "symbol": "AAA",
             "price": "x", "quantity": 1, "event_time_ms": 10_300,
             "schema_version": 1},
        ]
        acks = eng.ingest_batch(batch, process_time_ms=11_000)
        reasons = [a.reason for a in acks]
        for raw, ack in zip(batch, acks):
            log_decision("混合批次", raw, ack)
        assert reasons[0] is None
        assert reasons[1] is None
        assert reasons[2] == RejectReason.SCHEMA_TYPE_MISMATCH
        assert eng.query_window("AAA", 10_000)["event_count"] == 2

        # 单条重复提交，与结构错误原因可区分
        dup_again = eng.ingest(make_event("dup", 10_200), process_time_ms=11_000)
        log_decision("批次后重复", make_event("dup", 10_200), dup_again)
        assert dup_again.reason == RejectReason.DUPLICATE
        assert eng.query_window("AAA", 10_000)["event_count"] == 2
    finally:
        eng.close()
