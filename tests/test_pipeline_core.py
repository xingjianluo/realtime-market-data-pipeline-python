"""核心场景测试：去重、乱序归位、水位线与迟到拒绝。

每个用例都打印输入事件、判定结果与依据，便于人工核对。
"""

from __future__ import annotations

import logging

import pytest

from app.config import Config
from app.models import EventStatus, RejectReason
from app.pipeline import Pipeline

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("pipeline-test")


class FixedClock:
    """可手动设定当前时间的时钟，保证测试确定性。"""

    def __init__(self, start_ms: int = 1_000_000) -> None:
        self.now = start_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, delta_ms: int) -> None:
        self.now += delta_ms


def make_pipeline(tmp_path: str, clock: FixedClock, **overrides) -> Pipeline:
    cfg = Config(data_dir=str(tmp_path), **overrides)
    return Pipeline(cfg, clock=clock)


def evt(eid: str, t: int, price: float, qty: float = 1.0,
        source: str = "SRC-A", symbol: str = "600000", **extra) -> dict:
    data = {
        "event_id": eid, "source": source, "symbol": symbol,
        "price": price, "quantity": qty, "event_time_ms": t,
        "offset": int(eid[1:]) if eid[1:].isdigit() else None,
    }
    data.update(extra)
    return data


def log_result(raw: dict, result) -> None:
    log.info("输入事件: %s", raw)
    log.info("  -> 判定: %s | 原因: %s | 窗口: %s | 依据: %s",
             result.status.value,
             result.reason.value if result.reason else "-",
             result.window_start_ms, result.detail)


def test_duplicate_submit_counts_once(tmp_path):
    clock = FixedClock()
    pipe = make_pipeline(tmp_path, clock)
    raw = evt("E1", 10_000, price=10.0, qty=2.0)

    r1 = pipe.submit(raw)
    log_result(raw, r1)
    assert r1.status == EventStatus.ACCEPTED

    r2 = pipe.submit(raw)
    log_result(raw, r2)
    assert r2.status == EventStatus.DUPLICATE  # 依据：event_id 已确认且内容一致

    # 第三次重复同样不计数
    r3 = pipe.submit(dict(raw))
    log_result(raw, r3)
    assert r3.status == EventStatus.DUPLICATE

    agg = pipe.aggregates()
    assert len(agg) == 1
    assert agg[0].count == 1
    assert agg[0].total_quantity == 2.0
    assert agg[0].event_ids == ["E1"]
    log.info("聚合结果: count=%s quantity=%s notional=%s",
             agg[0].count, agg[0].total_quantity, agg[0].notional)


def test_duplicate_id_with_different_payload_conflicts(tmp_path):
    clock = FixedClock()
    pipe = make_pipeline(tmp_path, clock)
    r1 = pipe.submit(evt("E1", 10_000, price=10.0))
    log_result(evt("E1", 10_000, price=10.0), r1)
    assert r1.status == EventStatus.ACCEPTED

    conflict = evt("E1", 10_000, price=99.0)  # 同 id 不同价
    r2 = pipe.submit(conflict)
    log_result(conflict, r2)
    assert r2.status == EventStatus.REJECTED
    assert r2.reason == RejectReason.DUPLICATE_ID_CONFLICT

    # 既有聚合不被污染
    agg = pipe.aggregates()[0]
    assert agg.max_price == 10.0
    assert agg.count == 1
    assert len(pipe.dead_letters()) == 1


def test_out_of_order_events_land_in_correct_windows(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock, window_size_ms=1000,
                         allowed_lateness_ms=1000)

    # 先到一条 t=2500 的事件，再到更早的 t=2100、t=1800
    # （乱序，但分别迟到 400ms/700ms，均在允许迟到 1000ms 内）
    cases = [
        evt("E3", 2500, price=13.0),
        evt("E2", 2100, price=11.0),
        evt("E1", 1800, price=10.0),
    ]
    for raw in cases:
        r = pipe.submit(raw)
        log_result(raw, r)
        assert r.status == EventStatus.ACCEPTED

    aggs = {(a.symbol, a.window_start_ms): a for a in pipe.aggregates()}
    key = ("600000", 1000)
    assert aggs[("600000", 1000)].event_ids == ["E1"]
    # 窗口 [2000,3000) 内两条乱序事件
    w2 = aggs[("600000", 2000)]
    assert w2.event_ids == ["E3", "E2"]
    # first/last 价由 (事件时间, 事件ID) 决胜，与到达顺序无关：
    # 先到的 E3(2500,13) 后到的 E2(2100,11)，first 仍是 11、last 仍是 13
    assert w2.first_price == 11.0
    assert w2.last_price == 13.0
    assert w2.min_price == 11.0 and w2.max_price == 13.0
    assert w2.count == 2

    # 水位线 = 最大事件时间 2500 - 1000 = 1500
    assert pipe.watermark() == 1500
    log.info("窗口归位: %s", sorted((k, v.count) for k, v in aggs.items()))


def test_late_event_after_window_closed_is_rejected_not_dropped(tmp_path):
    clock = FixedClock(start_ms=20_000)
    pipe = make_pipeline(tmp_path, clock, window_size_ms=1000,
                         allowed_lateness_ms=500)

    r1 = pipe.submit(evt("E10", 10_000, price=10.0))
    log_result(evt("E10", 10_000, price=10.0), r1)
    assert r1.status == EventStatus.ACCEPTED

    clock.advance(1000)
    # 高水位事件把 watermark 推到 11500-500=11000，窗口 [10000,11000) 关闭
    r2 = pipe.submit(evt("E11", 11_500, price=11.0))
    log_result(evt("E11", 11_500, price=11.0), r2)
    assert r2.status == EventStatus.ACCEPTED

    late = evt("E12", 10_100, price=10.5)
    r3 = pipe.submit(late)
    log_result(late, r3)
    assert r3.status == EventStatus.REJECTED
    assert r3.reason == RejectReason.WINDOW_CLOSED  # 明确拒绝而非静默丢弃

    # 死信可查，包含可区分原因
    dl = pipe.dead_letters()
    assert len(dl) == 1 and dl[0]["reason"] == "WINDOW_CLOSED"

    # 重投依然关闭 => 保持 REJECTED
    rr = pipe.redeliver("E12")
    assert rr.status == EventStatus.REJECTED
    log.info("重投结果: %s %s", rr.reason.value, rr.detail)
