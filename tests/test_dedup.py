"""重复提交场景：同一 (source, event_id) 不得重复计入，且幂等可解释。"""
from __future__ import annotations

from market_pipeline.model import RejectReason
from tests.conftest import log_decision, make_event


def test_duplicate_submission_not_counted_twice(engine):
    raw = make_event("evt-1", 10_500, price=100, quantity=2)

    first = engine.ingest(raw, process_time_ms=11_000)
    log_decision("首次提交", raw, first)
    assert first.decision.value == "accepted"
    assert first.window_start_ms == 10_000

    duplicate = engine.ingest(raw, process_time_ms=11_000)
    log_decision("相同载荷重复提交", raw, duplicate)
    assert duplicate.decision.value == "rejected"
    assert duplicate.reason == RejectReason.DUPLICATE

    agg = engine.query_window("AAA", 10_000)
    assert agg["event_count"] == 1
    assert agg["total_quantity"] == 2
    assert agg["vwap"] == 100.0

    # 重复提交不前进消费位点
    assert engine.get_offsets() == {"src-a": 1}


def test_same_event_id_different_sources_are_distinct(engine):
    # 跨来源相同 event_id 不算重复
    a = make_event("evt-x", 10_500, source="src-a")
    b = make_event("evt-x", 10_600, source="src-b")
    ra = engine.ingest(a, process_time_ms=11_000)
    rb = engine.ingest(b, process_time_ms=11_000)
    log_decision("来源A", a, ra)
    log_decision("来源B同event_id", b, rb)
    assert ra.decision.value == "accepted"
    assert rb.decision.value == "accepted"
    assert engine.query_window("AAA", 10_000)["event_count"] == 2


def test_duplicate_is_idempotent_after_payload_tamper(engine):
    # 第二次改了价格也仍是重复：以首次确认为准，不污染聚合
    raw = make_event("evt-2", 10_500, price=100, quantity=1)
    tampered = make_event("evt-2", 10_500, price=999, quantity=1)
    r1 = engine.ingest(raw, process_time_ms=11_000)
    r2 = engine.ingest(tampered, process_time_ms=11_000)
    log_decision("原始事件", raw, r1)
    log_decision("同键改价重复提交", tampered, r2)
    assert r2.reason == RejectReason.DUPLICATE
    agg = engine.query_window("AAA", 10_000)
    assert agg["event_count"] == 1
    assert agg["vwap"] == 100.0
