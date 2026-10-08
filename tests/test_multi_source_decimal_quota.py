"""多来源独立编号、Decimal 精确金额、服务级总闸的专项测试。

覆盖业务（日志中逐条打印输入、判定与依据）：
1. 多来源编号语义：不同来源 event_id 撞车各自独立受理、互不覆盖、互不干扰；
   同一来源内重复提交仍只计一次，内容冲突以 DUPLICATE_ID_CONFLICT 区分。
2. 小数金额口径：price/quantity/累计金额/均价全程 Decimal，同一批事件在
   正序与反序（含中途重复提交）下累计金额与均价**完全一致**（== 精确相等）。
3. 服务级总容量：跨来源、跨窗口合计；到顶以 SERVICE_FULL 拒绝，与
   WINDOW_FULL/BATCH_TOO_LARGE 明确区分；并发下总额度不超发、结果确定；
   重启后已占用额度精确一致。
"""

from __future__ import annotations

import logging
import threading
from decimal import Decimal

from app.models import EventStatus, RejectReason
from tests.test_pipeline_core import FixedClock, evt, log_result, make_pipeline

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("multi-src-dec-quota-test")


# --------------------------------------------------------------------------- #
# 场景一：多来源编号相互独立
# --------------------------------------------------------------------------- #
def test_same_event_id_from_different_sources_are_independent(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock, allowed_lateness_ms=10_000)

    # 两个来源各自使用编号 "E1"（撞车是真实常态）
    a = evt("E1", 1000, price=10.0, qty=1.0, source="SRC-A")
    b = evt("E1", 1000, price=20.0, qty=3.0, source="SRC-B")
    r1 = pipe.submit(a)
    log.info("[多来源] SRC-A 的 E1:")
    log_result(a, r1)
    r2 = pipe.submit(b)
    log.info("[多来源] SRC-B 的 E1（同编号不同来源）:")
    log_result(b, r2)

    # 依据：去重主键是 (source, event_id)，跨来源互不覆盖
    assert r1.status == EventStatus.ACCEPTED
    assert r2.status == EventStatus.ACCEPTED
    agg = pipe.aggregates()
    assert len(agg) == 1  # 同一标的同一窗口聚到一起
    assert agg[0].count == 2
    assert agg[0].total_quantity == Decimal("4.0")
    assert agg[0].notional == Decimal("70.0")  # 10*1 + 20*3
    assert agg[0].sources == {"SRC-A", "SRC-B"}
    log.info("[多来源] 同编号两来源均受理: count=%s 金额=%s 来源=%s",
             agg[0].count, format(agg[0].notional, "f"), agg[0].sources)

    # 同一来源内重复提交仍幂等；他来源不受影响
    ra = pipe.submit(dict(a))
    log_result(a, ra)
    assert ra.status == EventStatus.DUPLICATE
    rb = pipe.submit(dict(b))
    assert rb.status == EventStatus.DUPLICATE
    assert agg[0].count == 2

    # 同一来源内同 id 内容对不上：明确、可区分地拒绝
    conflict = evt("E1", 1000, price=999.0, source="SRC-A")
    rc = pipe.submit(conflict)
    log.info("[多来源] SRC-A 内同编号不同内容:")
    log_result(conflict, rc)
    assert rc.status == EventStatus.REJECTED
    assert rc.reason == RejectReason.DUPLICATE_ID_CONFLICT

    # 冲突只影响 SRC-A 命名空间：SRC-B 同编号仍可继续重复提交（幂等）
    rb2 = pipe.submit(dict(b))
    assert rb2.status == EventStatus.DUPLICATE
    assert agg[0].count == 2
    assert agg[0].notional == Decimal("70.0")  # 聚合未被污染
    log.info("[多来源] 冲突隔离成立，聚合金额仍为 %s", format(agg[0].notional, "f"))


def test_redeliver_disambiguates_same_id_across_sources(tmp_path):
    clock = FixedClock(start_ms=20_000)
    pipe = make_pipeline(tmp_path, clock)

    # 先放一条高水位事件：watermark=11500-500=11000，窗口 [10000,11000) 关闭
    assert pipe.submit(evt("HI", 11_500, price=11.0)).status == \
        EventStatus.ACCEPTED

    # 两个来源的同编号迟到事件都因窗口关闭落死信
    for src in ("SRC-A", "SRC-B"):
        r = pipe.submit(evt("LATE9", 10_100, price=10.0, source=src))
        assert r.status == EventStatus.REJECTED
        assert r.reason == RejectReason.WINDOW_CLOSED

    # 不传 source 且命中多条：明确报歧义，不随意重投
    log.info("[多来源] 不带 source 重投撞车死信 LATE9:")
    ambiguous = pipe.redeliver("LATE9")
    log.info("  -> %s | %s", ambiguous.reason.value, ambiguous.detail)
    assert ambiguous.status == EventStatus.REJECTED
    assert ambiguous.reason == RejectReason.DUPLICATE_ID_CONFLICT

    # 指定来源后精确命中；重投仍失败（窗口保持关闭），死信条目不被误删
    rr = pipe.redeliver("LATE9", source="SRC-A")
    assert rr.reason == RejectReason.WINDOW_CLOSED
    dead = {(d.get("source"), d["event_id"]) for d in pipe.dead_letters()}
    assert dead == {("SRC-A", "LATE9"), ("SRC-B", "LATE9")}
    log.info("[多来源] 指定 source=SRC-A 重投，失败后两条死信均保留: %s", dead)


# --------------------------------------------------------------------------- #
# 场景二：小数金额精确、与到达顺序无关
# --------------------------------------------------------------------------- #
PRICE_QTY = [
    ("N1", "10.10", "3.00"),
    ("N2", "19.99", "0.30"),    # 经典 19.99/0.3 二进制浮点漂移样本
    ("N3", "100.005", "7.000"),
    ("N4", "0.07", "100.00"),
    ("N5", "1234567.89", "0.001"),
]
# 手算精确期望：
# 10.10*3.00        = 30.300
# 19.99*0.30        = 5.997
# 100.005*7.000     = 700.035
# 0.07*100.00       = 7.0000
# 1234567.89*0.001  = 1234.56789
EXPECTED_NOTIONAL = Decimal("30.300") + Decimal("5.997") + Decimal("700.035") \
    + Decimal("7.0000") + Decimal("1234.56789")
EXPECTED_QTY = Decimal("110.301")


def _submit_orders(tmp_path, order, source: str):
    """按给定顺序提交同一批小数事件，返回 (notional, avg_price, total_qty)。"""
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path / source, clock, allowed_lateness_ms=10_000)
    for eid, price, qty in order:
        raw = evt(eid, 1000, price=Decimal(price), qty=Decimal(qty),
                  source=source)
        r = pipe.submit(raw)
        assert r.status == EventStatus.ACCEPTED, (eid, r.reason)
    return pipe


def test_decimal_totals_identical_under_forward_and_reverse_order(tmp_path):
    forward = PRICE_QTY
    reverse = list(reversed(PRICE_QTY))

    p_fwd = _submit_orders(tmp_path, forward, "SRC-F")
    p_rev = _submit_orders(tmp_path, reverse, "SRC-R")
    a_fwd = p_fwd.aggregates()[0]
    a_rev = p_rev.aggregates()[0]

    log.info("[小数金额] 正序累计金额=%s 均价=%s",
             format(a_fwd.notional, "f"),
             format(a_fwd.avg_price, "f"))
    log.info("[小数金额] 反序累计金额=%s 均价=%s",
             format(a_rev.notional, "f"),
             format(a_rev.avg_price, "f"))

    # 与手算精确期望一致（若用 float，19.99*0.3 等已产生漂移）
    assert a_fwd.notional == EXPECTED_NOTIONAL
    assert a_rev.notional == EXPECTED_NOTIONAL
    assert a_fwd.total_quantity == EXPECTED_QTY
    assert a_rev.total_quantity == EXPECTED_QTY
    # 正反两种到达顺序结果**完全一致**（Decimal 精确相等，非近似）
    assert a_fwd.notional == a_rev.notional
    assert a_fwd.avg_price == a_rev.avg_price
    assert a_fwd.total_quantity == a_rev.total_quantity

    # 均价 = 累计金额 / 累计数量（Decimal 高精度除法；循环小数按 40 位量化比较）
    assert a_fwd.avg_price == EXPECTED_NOTIONAL / EXPECTED_QTY
    assert (a_fwd.avg_price * EXPECTED_QTY).quantize(Decimal("1e-40")) == \
        EXPECTED_NOTIONAL.quantize(Decimal("1e-40"))
    log.info("[小数金额] 期望金额=%s 实际=%s，正反序一致断言通过",
             format(EXPECTED_NOTIONAL, "f"), format(a_fwd.notional, "f"))


def test_decimal_totals_stable_with_interleaved_duplicates(tmp_path):
    """乱序 + 中途重复提交：结果与干净提交完全一致。"""
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock, allowed_lateness_ms=10_000)

    interleaved = [
        evt("N3", 1000, price=Decimal("100.005"), qty=Decimal("7.000")),
        evt("N1", 1000, price=Decimal("10.10"), qty=Decimal("3.00")),
        evt("N1", 1000, price=Decimal("10.10"), qty=Decimal("3.00")),  # 重复
        evt("N5", 1000, price=Decimal("1234567.89"), qty=Decimal("0.001")),
        evt("N2", 1000, price=Decimal("19.99"), qty=Decimal("0.30")),
        evt("N3", 1000, price=Decimal("100.005"), qty=Decimal("7.000")),  # 重复
        evt("N4", 1000, price=Decimal("0.07"), qty=Decimal("100.00")),
    ]
    accepted = duplicate = 0
    for raw in interleaved:
        r = pipe.submit(raw)
        if r.status == EventStatus.ACCEPTED:
            accepted += 1
        elif r.status == EventStatus.DUPLICATE:
            duplicate += 1
    assert accepted == 5 and duplicate == 2
    agg = pipe.aggregates()[0]
    assert agg.count == 5
    assert agg.notional == EXPECTED_NOTIONAL
    assert agg.avg_price == EXPECTED_NOTIONAL / EXPECTED_QTY
    log.info("[小数金额] 乱序+2 次重复提交后金额=%s 均价=%s（与干净提交一致）",
             format(agg.notional, "f"), format(agg.avg_price, "f"))


# --------------------------------------------------------------------------- #
# 场景三：服务级总容量
# --------------------------------------------------------------------------- #
def test_service_full_is_distinct_from_window_and_batch_levels(tmp_path):
    clock = FixedClock(start_ms=0)
    # 服务总闸=3，比单窗上限(默认1000)和批量上限(默认500)都先到顶
    pipe = make_pipeline(tmp_path, clock, max_total_events=3,
                         allowed_lateness_ms=10_000)

    for i in range(3):
        r = pipe.submit(evt(f"Q{i}", 1000 + (i % 3) * 100, price=1.0,
                            symbol="SYM"))
        log_result(evt(f"Q{i}", 1000, price=1.0), r)
        assert r.status == EventStatus.ACCEPTED

    assert pipe.quota() == {"accepted_total": 3, "max_total_events": 3}

    # 第 4 条：跨来源、跨窗口都算总闸，到顶以独立原因 SERVICE_FULL 拒绝
    overflow_a = evt("Q9", 1200, price=1.0, source="SRC-A", symbol="SYM")
    ra = pipe.submit(overflow_a)
    log_result(overflow_a, ra)
    assert ra.status == EventStatus.REJECTED
    assert ra.reason == RejectReason.SERVICE_FULL

    overflow_b = evt("Q9", 1200, price=1.0, source="SRC-B", symbol="OTHER")
    rb = pipe.submit(overflow_b)
    log_result(overflow_b, rb)
    assert rb.reason == RejectReason.SERVICE_FULL  # 他来源、他标的同样被总闸挡住

    # 重复提交不占用额度，也不报错（在到顶之后仍幂等返回 DUPLICATE）
    dup = pipe.submit(evt("Q0", 1000, price=1.0, symbol="SYM"))
    assert dup.status == EventStatus.DUPLICATE
    assert pipe.quota()["accepted_total"] == 3

    # 拒绝原因集合与 WINDOW_FULL / BATCH_TOO_LARGE 明确可区分
    reasons = {d["reason"] for d in pipe.dead_letters()}
    assert reasons == {"SERVICE_FULL"}
    log.info("[总闸] 到顶后拒绝原因=%s，额度=%s", reasons, pipe.quota())


def test_window_full_still_distinguishable_when_service_has_room(tmp_path):
    """单窗档先于总闸到顶时，仍报 WINDOW_FULL 而非 SERVICE_FULL。"""
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock, max_events_per_window=2,
                         max_total_events=100, allowed_lateness_ms=10_000)
    for i in range(2):
        assert pipe.submit(evt(f"W{i}", 1000, price=1.0)).status == \
            EventStatus.ACCEPTED
    r = pipe.submit(evt("W2", 1000, price=1.0))
    assert r.reason == RejectReason.WINDOW_FULL
    log.info("[总闸] 单窗先到顶 -> %s（总闸仍有余量 %s）",
             r.reason.value, pipe.quota())


def test_concurrent_submissions_never_oversell_quota(tmp_path):
    """并发打满总闸：已受理数严格等于上限，谁进谁出确定、无超发。"""
    clock = FixedClock(start_ms=0)
    cap = 50
    pipe = make_pipeline(tmp_path, clock, max_total_events=cap,
                         allowed_lateness_ms=10_000)

    results: list[EventStatus] = []
    barrier = threading.Barrier(8)

    def worker(src: int) -> None:
        barrier.wait()  # 尽量让 8 个线程同时抢额度
        for i in range(20):  # 共 160 次提交，远超 50
            r = pipe.submit(evt(f"C{i}", 1000, price=1.0,
                                source=f"SRC-{src}", symbol="CONC"))
            results.append(r.status)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = sum(1 for s in results if s == EventStatus.ACCEPTED)
    rejected = sum(1 for s in results
                   if s == EventStatus.REJECTED)
    log.info("[总闸并发] 提交=%s 受理=%s 拒绝=%s 上限=%s",
             len(results), accepted, rejected, cap)

    # 核心确定性断言：绝不超发，且额度被恰好占满
    assert accepted == cap
    assert pipe.quota()["accepted_total"] == cap
    assert accepted + rejected == len(results)
    # 所有拒绝都必须是 SERVICE_FULL（本用例不触发其他档位）
    dl = pipe.dead_letters()
    assert len(dl) == rejected
    assert {d["reason"] for d in dl} == {"SERVICE_FULL"}

    # 持久化侧同样不重不超：WAL 条数 == 已受理数，复合键唯一
    wal = pipe.storage.load_wal()
    keys = [(r["source"], r["event_id"]) for r in wal]
    assert len(keys) == len(set(keys)) == cap
    log.info("[总闸并发] WAL 唯一复合键=%s，无超发、无重复", len(keys))


def test_quota_consistent_after_restart(tmp_path):
    """重启后已占用总额度与持久化数据完全对得上，不凭空增减。"""
    clock = FixedClock(start_ms=0)
    data_dir = str(tmp_path)
    cap = 100
    pipe = make_pipeline(data_dir, clock, max_total_events=cap,
                         allowed_lateness_ms=10_000)
    # 跨来源、跨窗口写入 7 条（含一条重复提交）
    for src in ("SRC-A", "SRC-B", "SRC-C"):
        for i in range(2):
            assert pipe.submit(evt(f"R{i}", 1000 + i * 100, price=1.0,
                                   source=src)).status == EventStatus.ACCEPTED
    assert pipe.submit(evt("R0", 1000, price=1.0, source="SRC-A")).status \
        == EventStatus.DUPLICATE
    assert pipe.quota()["accepted_total"] == 6
    pipe.close()
    log.info("[总闸重启] 关闭前已受理=6")

    # 模拟重启：同配置重建
    pipe2 = make_pipeline(data_dir, clock, max_total_events=cap,
                          allowed_lateness_ms=10_000)
    quota = pipe2.quota()
    log.info("[总闸重启] 重启后额度=%s，窗口事件总数=%s",
             quota, sum(a.count for a in pipe2.aggregates()))
    assert quota["accepted_total"] == 6
    assert quota["max_total_events"] == cap
    assert sum(a.count for a in pipe2.aggregates()) == 6

    # 重启后只剩 cap-6 个名额：第 7~ 条按总闸规则被拒，额度不凭空多
    for i in range(6, cap):
        r = pipe2.submit(evt(f"FILL{i}", 1500, price=1.0, source="FILLER"))
        assert r.status == EventStatus.ACCEPTED
    assert pipe2.quota()["accepted_total"] == cap
    over = pipe2.submit(evt("ONE-MORE", 1500, price=1.0, source="FILLER"))
    assert over.reason == RejectReason.SERVICE_FULL
    log.info("[总闸重启] 补齐到 %s 后再提交 -> %s，额度一致",
             cap, over.reason.value)


def test_quota_recovered_from_wal_when_snapshot_missing(tmp_path):
    """快照缺失走 WAL 重放兜底时，总额度同样精确恢复。"""
    clock = FixedClock(start_ms=0)
    data_dir = str(tmp_path)
    pipe = make_pipeline(data_dir, clock, max_total_events=10,
                         allowed_lateness_ms=10_000)
    for i in range(4):
        pipe.submit(evt(f"K{i}", 1000 + i, price=Decimal("1.1"),
                        qty=Decimal("0.2")))
    pipe.close()
    (tmp_path / "state.json").unlink()

    pipe2 = make_pipeline(data_dir, clock, max_total_events=10,
                          allowed_lateness_ms=10_000)
    assert pipe2.quota()["accepted_total"] == 4
    # 重放的小数金额无漂移
    assert sum(a.notional for a in pipe2.aggregates()) == \
        Decimal("1.1") * Decimal("0.2") * 4
    log.info("[总闸重启] WAL 兜底重放额度=4，金额=%s",
             format(sum(a.notional for a in pipe2.aggregates()), "f"))


def test_legacy_float_snapshot_restores_into_decimal_model(tmp_path):
    """旧版本落盘的 float 快照可直接读入：金额归一为 Decimal，额度/命名空间不丢。"""
    import json

    legacy_state = {
        # 旧版：event_id 全局命名空间，金额为二进制 float，决胜键为二元组
        "dedup": {"OLD1": "sig"},
        "offsets": {"SRC-A": 1},
        "total_accepted": 1,
        "windows": {"windows": [{
            "symbol": "600000", "window_start_ms": 1000, "window_end_ms": 2000,
            "count": 1, "total_quantity": 2.0, "notional": 21.0,
            "min_price": 10.5, "max_price": 10.5, "first_price": 10.5,
            "last_price": 10.5, "event_ids": ["OLD1"], "sources": ["SRC-A"],
            "closed": False, "first_key": [1000, "OLD1"],
            "last_key": [1000, "OLD1"], "_key": ["600000", 1000]}],
            "max_seen_event_time": 1000},
    }
    (tmp_path / "state.json").write_text(json.dumps(legacy_state),
                                         encoding="utf-8")

    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock, allowed_lateness_ms=10_000)
    agg = pipe.aggregates()[0]
    assert isinstance(agg.notional, Decimal)
    assert agg.notional == Decimal("21.0")
    assert agg.total_quantity == Decimal("2.0")
    assert agg.first_key == (1000, "", "OLD1")  # 旧二元键归一到保留来源
    assert pipe.quota()["accepted_total"] == 1
    log.info("[兼容] 旧 float 快照恢复: notional=%s 类型=Decimal 额度=1",
             format(agg.notional, "f"))
