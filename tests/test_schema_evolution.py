"""结构演进：新增字段缺省语义稳定；缺失/类型不符原因可区分且可重投。"""
from __future__ import annotations

from market_pipeline.model import PipelineConfig, RejectReason
from market_pipeline.engine import PipelineEngine
from tests.conftest import log_decision, make_event


def test_v1_events_aggregate_unchanged(engine):
    raw = make_event("v1", 10_500, price=100, quantity=2, schema_version=1)
    r = engine.ingest(raw, process_time_ms=11_000)
    log_decision("v1 事件", raw, r)
    assert r.decision.value == "accepted"
    agg = engine.query_window("AAA", 10_000)
    assert agg["event_count"] == 1
    # v1 不感知 trade_side，归类为 unspecified 且不影响核心聚合
    assert agg["side_unspecified"] == 1


def test_v2_optional_field_default_is_stable(engine):
    # v2 缺省 trade_side 稳定为 unspecified
    raw = make_event("v2-default", 10_500, schema_version=2)
    r = engine.ingest(raw, process_time_ms=11_000)
    log_decision("v2 缺省新字段", raw, r)
    assert r.decision.value == "accepted"
    agg = engine.query_window("AAA", 10_000)
    assert agg["side_unspecified"] == 1


def test_v2_new_field_is_aggregated(engine):
    buy = make_event("v2-buy", 10_500, schema_version=2, trade_side="buy")
    sell = make_event("v2-sell", 10_600, schema_version=2, trade_side="sell")
    rb = engine.ingest(buy, process_time_ms=11_000)
    rs = engine.ingest(sell, process_time_ms=11_000)
    log_decision("v2 买方", buy, rb)
    log_decision("v2 卖方", sell, rs)
    agg = engine.query_window("AAA", 10_000)
    assert agg["side_buy"] == 1
    assert agg["side_sell"] == 1
    assert agg["side_unspecified"] == 0


def test_missing_field_distinct_from_type_mismatch(engine):
    missing = {
        "event_id": "m1", "source": "s", "symbol": "AAA",
        "price": 100, "quantity": 1, "schema_version": 1,  # 缺 event_time_ms
    }
    mistype = {
        "event_id": "m2", "source": "s", "symbol": "AAA",
        "price": "100", "quantity": 1, "event_time_ms": 10_500, "schema_version": 1,
    }
    rm = engine.ingest(missing, process_time_ms=11_000)
    rt = engine.ingest(mistype, process_time_ms=11_000)
    log_decision("缺字段", missing, rm)
    log_decision("类型不符", mistype, rt)
    assert rm.reason == RejectReason.SCHEMA_FIELD_MISSING
    assert rt.reason == RejectReason.SCHEMA_TYPE_MISMATCH
    assert rm.reason != rt.reason


def test_unknown_version_rejected(engine):
    raw = make_event("v9", 10_500, schema_version=99)
    r = engine.ingest(raw, process_time_ms=11_000)
    log_decision("未知版本", raw, r)
    assert r.reason == RejectReason.SCHEMA_UNKNOWN_VERSION


def test_rejected_event_can_be_redelivered_after_fix(engine, tmp_path):
    bad = make_event("fix-me", 10_500)
    bad["price"] = "not-int"
    first = engine.ingest(bad, process_time_ms=11_000)
    log_decision("首次类型错误", bad, first)
    assert first.reason == RejectReason.SCHEMA_TYPE_MISMATCH

    # 同一 event_id 修正载荷后重新投递：之前是拒绝（未确认），现在应被接受
    fixed = make_event("fix-me", 10_500, price=105, quantity=2)
    second = engine.ingest(fixed, process_time_ms=11_000)
    log_decision("修正后重投", fixed, second, note="同 event_id 此前被拒绝")
    assert second.decision.value == "accepted"
    agg = engine.query_window("AAA", 10_000)
    assert agg["event_count"] == 1
    assert agg["vwap"] == 105.0

    # 位点：拒绝一次 + 接受一次，共两次确认
    assert engine.get_offsets()["src-a"] == 2


def test_unknown_fields_ignored_not_poisoning(engine):
    raw = make_event("extra", 10_500, future_weird_field={"nested": [1, 2]})
    r = engine.ingest(raw, process_time_ms=11_000)
    log_decision("带未声明字段", raw, r)
    assert r.decision.value == "accepted"
    assert engine.query_window("AAA", 10_000)["event_count"] == 1
