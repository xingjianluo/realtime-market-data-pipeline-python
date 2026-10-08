"""新场景测试：多来源独立编号、Decimal 金额顺序无关、服务级总闸。

判定依据全部打印到 INFO 日志，跑测时可直接看到覆盖的业务与拒绝分层。
"""

from __future__ import annotations

import logging
import threading
from decimal import Decimal

from app.config import Config
from app.models import EventStatus, RejectReason
from app.pipeline import Pipeline
from tests.test_pipeline_core import FixedClock, evt, log_result, make_pipeline

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("new-scenario-test")


# --------------------------------------------------------------------- #
# 场景一：多来源编号语义
# --------------------------------------------------------------------- #
def test_same_event_id_from_different_sources_are_independent(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock)

    a = evt("DUP1", 500, price=10.0, source="SRC-A")
    b = evt("DUP1", 600, price=20.0, source="SRC-B")
    r1 = pipe.submit(a)
    log_result(a, r1)
    r2 = pipe.submit(b)
    log_result(b, r2)
    # 依据：去重作用域为 (source, event_id)，跨来源同号各自独立受理
    assert r1.status == EventStatus.ACCEPTED
    assert r2.status == EventStatus.ACCEPTED

    agg = pipe.aggregates()[0]
    assert agg.count == 2
    assert set(agg.sources) == {"SRC-A", "SRC-B"}
    # 来源限定引用同时保留，避免撞号后无法区分归属
    assert agg.event_refs == ["SRC-A/DUP1", "SRC-B/DUP1"]

    # 各自重复提交：互不干扰，均幂等
    ra2 = pipe.submit(a)
    rb2 = pipe.submit(b)
    log_result(a, ra2)
    log_result(b, rb2)
    assert ra2.status == EventStatus.DUPLICATE
    assert rb2.status == EventStatus.DUPLICATE
    assert agg.count == 2
    log.info("跨来源同号：两条独立受理，重复各自幂等，窗口 count=%s", agg.count)


def test_same_source_same_id_content_conflict_still_rejected(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock)

    first = evt("C1", 500, price=10.0, source="SRC-A")
    r1 = pipe.submit(first)
    log_result(first, r1)
    assert r1.status == EventStatus.ACCEPTED

    # 同来源同 id 但价格对不上 -> 冲突拒绝，与跨来源独立明确区分
    conflict = evt("C1", 500, price=11.5, source="SRC-A")
    r2 = pipe.submit(conflict)
    log_result(conflict, r2)
    assert r2.status == EventStatus.REJECTED
    assert r2.reason == RejectReason.DUPLICATE_ID_CONFLICT

    # 另一来源用同号同价也不受 SRC-A 冲突记录影响
    other = evt("C1", 500, price=11.5, source="SRC-B")
    r3 = pipe.submit(other)
    log_result(other, r3)
    assert r3.status == EventStatus.ACCEPTED

    agg = pipe.aggregates()[0]
    assert agg.count == 2
    log.info("同来源冲突被拒且不污染聚合；跨来源同号正常受理，count=%s", agg.count)


# --------------------------------------------------------------------- #
# 场景二：小数金额精度，正反两种到达顺序结果一致
# --------------------------------------------------------------------- #
DECIMAL_EVENTS = [
    ("E1", 10.10, 3.0),
    ("E2", 0.30, 7.0),
    ("E3", 99.99, 0.01),
    ("E4", 1234567.89, 1.5),
    ("E5", 0.01, 999.99),
]


def _run_order(tmp_path, clock, order):
    pipe = make_pipeline(tmp_path, clock)
    for eid, price, qty in order:
        raw = evt(eid, 500, price=price, qty=qty)
        r = pipe.submit(raw)
        log_result(raw, r)
        assert r.status == EventStatus.ACCEPTED
    return pipe.aggregates()[0]


def test_decimal_notional_and_avg_identical_in_both_arrival_orders(tmp_path):
    forward = list(DECIMAL_EVENTS)
    reverse = list(reversed(DECIMAL_EVENTS))

    agg_f = _run_order(tmp_path / "f", FixedClock(0), forward)
    agg_r = _run_order(tmp_path / "r", FixedClock(0), reverse)

    log.info("正序 notional=%s avg=%s qty=%s",
             agg_f.notional, agg_f.avg_price, agg_f.total_quantity)
    log.info("逆序 notional=%s avg=%s qty=%s",
             agg_r.notional, agg_r.avg_price, agg_r.total_quantity)

    # 判定依据：Decimal 精确累加，两种到达顺序的累计金额必须逐位相等
    assert agg_f.notional == agg_r.notional
    assert agg_f.avg_price == agg_r.avg_price
    assert agg_f.total_quantity == agg_r.total_quantity

    # 与手工精确口径核对（不允许用 float 结果当基准）
    expected_notional = sum(
        (Decimal(str(p)) * Decimal(str(q)) for p, q in
         [(10.10, 3.0), (0.30, 7.0), (99.99, 0.01),
          (1234567.89, 1.5), (0.01, 999.99)]),
        Decimal(0))
    assert agg_f.notional == expected_notional
    expected_avg = (expected_notional / Decimal("1011.5")).quantize(
        Decimal("0.0000000001"))
    assert agg_f.avg_price == expected_avg
    log.info("金额口径与手工 Decimal 基准一致：notional=%s avg=%s",
             expected_notional, expected_avg)


def test_repeated_resubmission_does_not_drift_decimal_totals(tmp_path):
    clock = FixedClock(0)
    pipe = make_pipeline(tmp_path, clock)
    # 同一批小数事件，夹杂乱序与重复提交，结果必须只算一次且无漂移
    sequence = [
        evt("E2", 500, price=0.30, qty=7.0),
        evt("E1", 500, price=10.10, qty=3.0),
        evt("E2", 500, price=0.30, qty=7.0),  # 重复
        evt("E3", 500, price=99.99, qty=0.01),
        evt("E1", 500, price=10.10, qty=3.0),  # 重复
    ]
    for raw in sequence:
        r = pipe.submit(raw)
        log_result(raw, r)
    agg = pipe.aggregates()[0]
    assert agg.count == 3
    assert agg.notional == Decimal("30.30") + Decimal("2.10") + Decimal("0.9999")
    assert agg.total_quantity == Decimal("10.01")
    log.info("乱序+重复提交后 notional=%s avg=%s（无漂移、不重计）",
             agg.notional, agg.avg_price)


def test_large_volume_decimal_accumulation_stays_exact(tmp_path):
    clock = FixedClock(0)
    # 1000 笔相同小额报价：float 下 0.1 累加会漂移，Decimal 必须精确。
    # 时间分散到多个窗口，避免触发单窗上限（本用例只验证金额精度）。
    n = 1000
    pipe = make_pipeline(tmp_path, clock, allowed_lateness_ms=10_000)
    for i in range(n):
        assert pipe.submit(
            evt(f"L{i}", 500 + (i % 10) * 1000,
                price=1000.01, qty=0.1)).status == EventStatus.ACCEPTED
    # 事件分散在 10 个窗口，跨窗口汇总后再核对精确口径
    aggs = pipe.aggregates()
    total_qty = sum((a.total_quantity for a in aggs), Decimal(0))
    total_notional = sum((a.notional for a in aggs), Decimal(0))
    assert sum(a.count for a in aggs) == n
    assert total_qty == Decimal("100.0")           # 1000 * 0.1 精确
    assert total_notional == Decimal("100001.000")  # 1000 * 1000.01 * 0.1 精确
    avg = (total_notional / total_qty).quantize(Decimal("0.0000000001"))
    assert avg == Decimal("1000.0100000000")
    # 每个窗口自身均价同样精确
    assert all(a.avg_price == Decimal("1000.0100000000") for a in aggs)
    log.info("大额累计 %d 笔（跨 %d 窗）：qty=%s notional=%s avg=%s",
             n, len(aggs), total_qty, total_notional, avg)


# --------------------------------------------------------------------- #
# 场景三：服务级总闸（跨来源、跨窗口）
# --------------------------------------------------------------------- #
def test_service_cap_rejects_with_distinct_reason_and_is_cross_source(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock, max_accepted_events=5)

    # 3 个来源各占额度，跨来源累计到顶
    for i in range(2):
        r = pipe.submit(evt(f"A{i}", 500, price=1.0, source="SRC-A"))
        assert r.status == EventStatus.ACCEPTED
    for i in range(2):
        r = pipe.submit(evt(f"B{i}", 1500, price=1.0, source="SRC-B"))
        assert r.status == EventStatus.ACCEPTED
    r = pipe.submit(evt("C0", 1500, price=1.0, source="SRC-C", symbol="000001"))
    assert r.status == EventStatus.ACCEPTED

    cap = pipe.capacity()
    log.info("总闸占用: %s", cap)
    assert cap == {"accepted": 5, "limit": 5, "remaining": 0}

    # 到顶：新事件被总闸拒，原因与 WINDOW_FULL/BATCH_TOO_LARGE 一眼可区分
    overflow = evt("A9", 500, price=1.0, source="SRC-A")
    ro = pipe.submit(overflow)
    log_result(overflow, ro)
    assert ro.status == EventStatus.REJECTED
    assert ro.reason == RejectReason.SERVICE_FULL

    # 死信记录同样标明 SERVICE_FULL
    dl = [d for d in pipe.dead_letters() if d["event_id"] == "A9"]
    assert dl and dl[0]["reason"] == "SERVICE_FULL"

    # 已确认事件的重复提交不占用新额度，仍幂等成功
    dup = evt("A0", 500, price=1.0, source="SRC-A")
    rd = pipe.submit(dup)
    log_result(dup, rd)
    assert rd.status == EventStatus.DUPLICATE
    assert pipe.capacity()["accepted"] == 5
    log.info("总闸到顶后重复提交幂等放行，占用不增加")


def test_service_cap_concurrent_deterministic_no_over_issuance(tmp_path):
    clock = FixedClock(start_ms=0)
    limit = 100
    pipe = make_pipeline(tmp_path, clock, max_accepted_events=limit)

    threads_n, per_thread = 8, 25  # 共 200 次提交，恰好争抢 100 个额度
    barrier = threading.Barrier(threads_n)
    accepted: list[str] = []
    rejected: list[str] = []
    lock = threading.Lock()

    def worker(tid: int) -> None:
        barrier.wait()  # 最大化竞争：所有线程同时开抢
        for i in range(per_thread):
            eid = f"T{tid}-{i}"
            r = pipe.submit(evt(eid, 500, price=1.0, source=f"SRC-{tid % 3}"))
            with lock:
                if r.status == EventStatus.ACCEPTED:
                    accepted.append(eid)
                elif r.status == EventStatus.REJECTED:
                    assert r.reason == RejectReason.SERVICE_FULL
                    rejected.append(eid)
                else:
                    raise AssertionError(f"意外状态 {r.status}")

    threads = [threading.Thread(target=worker, args=(t,))
               for t in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 判定依据：总额度绝不超发；谁进谁出确定（接受集合无重复、数量精确等于上限）
    assert len(accepted) == limit
    assert len(set(accepted)) == limit
    assert len(rejected) == threads_n * per_thread - limit
    assert pipe.capacity()["accepted"] == limit
    wal_ids = [r["event_id"] for r in pipe.storage.load_wal()]
    assert len(wal_ids) == len(set(wal_ids)) == limit
    log.info("并发 %d 提交争抢 %d 额度：接受=%d 拒绝=%d，无超发、无重复",
             threads_n * per_thread, limit, len(accepted), len(rejected))


def test_service_cap_survives_restart(tmp_path):
    data_dir = str(tmp_path)
    # 首次运行：用掉 4 个额度（跨两个来源）
    pipe = make_pipeline(data_dir, FixedClock(0), max_accepted_events=5)
    for raw in [evt("K1", 500, price=1.0, source="SRC-A"),
                evt("K2", 1500, price=1.0, source="SRC-A"),
                evt("K3", 1500, price=1.0, source="SRC-B", symbol="000001"),
                evt("K4", 2500, price=1.0, source="SRC-B")]:
        assert pipe.submit(raw).status == EventStatus.ACCEPTED
    assert pipe.capacity()["accepted"] == 4
    pipe.close()
    log.info("重启前已占用额度 4/5，关闭服务")

    # 重启：已占用额度必须对得上，不凭空多也不凭空少
    pipe2 = make_pipeline(data_dir, FixedClock(0), max_accepted_events=5)
    cap = pipe2.capacity()
    log.info("重启后总闸占用: %s", cap)
    assert cap == {"accepted": 4, "limit": 5, "remaining": 1}

    ok = evt("K5", 3500, price=1.0, source="SRC-C", symbol="600519")
    r1 = pipe2.submit(ok)
    log_result(ok, r1)
    assert r1.status == EventStatus.ACCEPTED

    full = evt("K6", 3600, price=1.0, source="SRC-C", symbol="600519")
    r2 = pipe2.submit(full)
    log_result(full, r2)
    assert r2.status == EventStatus.REJECTED
    assert r2.reason == RejectReason.SERVICE_FULL
    log.info("重启后第 5 条接受、第 6 条 SERVICE_FULL，额度恢复一致")


def test_service_cap_wal_replay_also_recovers_quota(tmp_path):
    data_dir = str(tmp_path)
    pipe = make_pipeline(data_dir, FixedClock(0), max_accepted_events=3)
    for i in range(3):
        assert pipe.submit(
            evt(f"Q{i}", 500, price=1.0, source="SRC-A")).status == EventStatus.ACCEPTED
    pipe.close()
    # 删掉快照，强制走 WAL 重放兜底路径
    (tmp_path / "state.json").unlink()

    pipe2 = make_pipeline(data_dir, FixedClock(0), max_accepted_events=3)
    assert pipe2.capacity()["accepted"] == 3
    overflow = evt("Q9", 500, price=1.0, source="SRC-A")
    r = pipe2.submit(overflow)
    log_result(overflow, r)
    assert r.reason == RejectReason.SERVICE_FULL
    log.info("WAL 重放兜底恢复后额度仍为 3/3，新事件被总闸拒绝")


# --------------------------------------------------------------------- #
# 补充：跨来源死信重投消歧
# --------------------------------------------------------------------- #
def test_redelivery_requires_source_when_id_clashes_across_sources(tmp_path):
    clock = FixedClock(start_ms=10_000)
    pipe = make_pipeline(tmp_path, clock, max_future_skew_ms=10_000)

    # 两个来源都用 event_id=Z1 且都是未来事件，各自落死信
    far_a = evt("Z1", 90_000, price=1.0, source="SRC-A")
    far_b = evt("Z1", 90_000, price=2.0, source="SRC-B")
    assert pipe.submit(far_a).reason == RejectReason.FUTURE_EVENT
    assert pipe.submit(far_b).reason == RejectReason.FUTURE_EVENT

    # 不带 source 重投：撞号歧义必须明确拒绝，不能随机挑一条
    r0 = pipe.redeliver("Z1")
    log.info("无来源重投撞号死信 -> %s %s", r0.reason, r0.detail)
    assert r0.status == EventStatus.REJECTED
    assert r0.reason == RejectReason.AMBIGUOUS_REDELIVERY

    # 带 source 后两条都能在基准推进后重投（先桥接时间基准）
    for t in (20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000):
        assert pipe.submit(
            evt(f"G{t}", t, price=1.0, source="SRC-A")).status == EventStatus.ACCEPTED
    ra = pipe.redeliver("Z1", source="SRC-A")
    rb = pipe.redeliver("Z1", source="SRC-B")
    log_result(far_a, ra)
    log_result(far_b, rb)
    assert ra.status == EventStatus.ACCEPTED and ra.source == "SRC-A"
    assert rb.status == EventStatus.ACCEPTED and rb.source == "SRC-B"
    assert pipe.dead_letters() == []
    log.info("带来源重投后两条撞号事件分别受理，死信清空")


# --------------------------------------------------------------------- #
# 补充：Decimal 金额经快照重启后完全一致
# --------------------------------------------------------------------- #
def test_decimal_totals_identical_after_restart(tmp_path):
    data_dir = str(tmp_path)
    cases = [evt("D1", 500, price=0.10, qty=0.30),
             evt("D2", 500, price=19.99, qty=1.25),
             evt("D3", 500, price=0.07, qty=88.88)]
    pipe = make_pipeline(data_dir, FixedClock(0))
    for raw in cases:
        assert pipe.submit(raw).status == EventStatus.ACCEPTED
    before = pipe.aggregates()[0]
    before_notional, before_avg = before.notional, before.avg_price
    pipe.close()

    pipe2 = make_pipeline(data_dir, FixedClock(0))
    after = pipe2.aggregates()[0]
    log.info("重启前 notional=%s avg=%s；重启后 notional=%s avg=%s",
             before_notional, before_avg, after.notional, after.avg_price)
    assert after.notional == before_notional
    assert after.avg_price == before_avg
    # 快照中金额以精确十进制字符串落盘（不是二进制 float）
    import json
    state = json.loads((tmp_path / "state.json").read_text())
    w = state["windows"]["windows"][0]
    assert w["notional"] == format(before_notional, "f")


# --------------------------------------------------------------------- #
# 补充：HTTP 层总闸与金额输出口径
# --------------------------------------------------------------------- #
def test_http_capacity_gate_and_decimal_payload(tmp_path):
    from fastapi.testclient import TestClient

    from app.api import create_app

    cfg = Config(data_dir=str(tmp_path), max_accepted_events=2)
    pipe = Pipeline(cfg, clock=FixedClock(0))
    client = TestClient(create_app(pipe))

    def http_event(eid, **kw):
        return {"event_id": eid, "source": kw.pop("source", "SRC-A"),
                "symbol": "600000", "price": 10.10, "quantity": 0.30,
                "event_time_ms": 500, **kw}

    r1 = client.post("/events", json=http_event("H1"))
    r2 = client.post("/events", json=http_event("H2", source="SRC-B"))
    assert r1.status_code == 200 and r2.status_code == 200

    cap = client.get("/capacity").json()
    log.info("GET /capacity -> %s", cap)
    assert cap == {"accepted": 2, "limit": 2, "remaining": 0}

    r3 = client.post("/events", json=http_event("H3"))
    log.info("总闸到顶 POST /events -> %s", r3.json())
    assert r3.status_code == 422
    assert r3.json()["reason"] == "SERVICE_FULL"

    # 金额字段为精确十进制字符串（数值口径，尾零规范化），均价 = 10.10
    agg = client.get("/aggregates").json()["aggregates"][0]
    log.info("GET /aggregates -> %s", agg)
    assert Decimal(agg["notional"]) == Decimal("6.0600")   # 2*10.10*0.30
    assert Decimal(agg["total_quantity"]) == Decimal("0.60")
    assert agg["avg_price"] == "10.1000000000"
    assert agg["event_refs"] == ["SRC-A/H1", "SRC-B/H2"]

    health = client.get("/health").json()
    assert health["capacity"]["accepted"] == 2
