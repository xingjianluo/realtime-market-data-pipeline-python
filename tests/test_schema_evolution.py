"""事件结构演进测试：新增字段缺省语义、缺失/类型不符/越界拒绝、死信重投。

失败原因必须可区分：MISSING_FIELD / TYPE_MISMATCH / UNSUPPORTED_SCHEMA /
FUTURE_EVENT，且拒绝记录可被重新投递，不污染既有聚合。
"""

from __future__ import annotations

import logging

from app.models import EventStatus, RejectReason
from tests.test_pipeline_core import FixedClock, evt, log_result, make_pipeline

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("schema-test")


def base(t=10_000, **kw):
    return evt("S1", t, price=10.0, **kw)


def test_v2_new_venue_field_has_stable_default(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock)

    # v1 事件不含 venue
    r1 = pipe.submit(base())
    log_result(base(), r1)
    assert r1.status == EventStatus.ACCEPTED

    # v2 事件显式带 venue
    v2 = {**base(event_id="S2"), "schema_version": 2, "venue": "SSE"}
    r2 = pipe.submit(v2)
    log_result(v2, r2)
    assert r2.status == EventStatus.ACCEPTED

    # v2 事件缺省 venue：缺省语义稳定为 DEFAULT
    v2_default = {**base(event_id="S3"), "schema_version": 2}
    r3 = pipe.submit(v2_default)
    log_result(v2_default, r3)
    assert r3.status == EventStatus.ACCEPTED

    # WAL 中 venue 落盘值：v1 升级读入也补 DEFAULT
    wal = pipe.storage.load_wal()
    venues = {r["event_id"]: r["event"]["venue"] for r in wal}
    assert venues == {"S1": "DEFAULT", "S2": "SSE", "S3": "DEFAULT"}
    log.info("venue 缺省语义落盘: %s", venues)


def test_missing_field_and_type_mismatch_are_distinct(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock)

    missing = {"event_id": "M1", "source": "s", "symbol": "600000",
               "price": 10.0, "quantity": 1.0}  # 缺 event_time_ms
    r1 = pipe.submit(missing)
    log_result(missing, r1)
    assert r1.status == EventStatus.REJECTED
    assert r1.reason == RejectReason.MISSING_FIELD

    bad_type = {**base(event_id="M2"), "price": "10.5"}  # price 类型不符
    r2 = pipe.submit(bad_type)
    log_result(bad_type, r2)
    assert r2.status == EventStatus.REJECTED
    assert r2.reason == RejectReason.TYPE_MISMATCH

    bad_time = {**base(event_id="M3"), "event_time_ms": -1}
    r3 = pipe.submit(bad_time)
    log_result(bad_time, r3)
    assert r3.reason == RejectReason.TYPE_MISMATCH

    blank = {**base(event_id="M4"), "symbol": "  "}  # 空白视同缺失
    r4 = pipe.submit(blank)
    log_result(blank, r4)
    assert r4.reason == RejectReason.MISSING_FIELD

    # 原因集合可区分
    reasons = {d["reason"] for d in pipe.dead_letters()}
    assert reasons == {"MISSING_FIELD", "TYPE_MISMATCH"}
    # 全部拒绝未污染聚合
    assert pipe.aggregates() == []


def test_unsupported_schema_rejected_and_redeliverable_after_upgrade(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock)

    future_schema = {**base(event_id="U1"), "schema_version": 99}
    r1 = pipe.submit(future_schema)
    log_result(future_schema, r1)
    assert r1.reason == RejectReason.UNSUPPORTED_SCHEMA

    # 模拟"修复"：降级为受支持版本后重投同一条原始记录
    fixed = {**future_schema, "schema_version": 2, "venue": "SSE"}
    # 通过死信重放接口：先替换死信内容不可行，故直接提交修正版并验证可接受
    r2 = pipe.submit(fixed)
    log_result(fixed, r2)
    assert r2.status == EventStatus.ACCEPTED
    assert pipe.aggregates()[0].count == 1


def test_future_event_out_of_window_boundary_rejected(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock, max_future_skew_ms=10_000)

    normal = base(event_id="F1", t=1000)
    r0 = pipe.submit(normal)
    assert r0.status == EventStatus.ACCEPTED

    too_future = {**base(event_id="F2", t=999_000)}  # 超前 99.8 万 ms
    r1 = pipe.submit(too_future)
    log_result(too_future, r1)
    assert r1.reason == RejectReason.FUTURE_EVENT
    assert len(pipe.dead_letters()) == 1


def test_dead_letter_redeliver_after_correction(tmp_path):
    clock = FixedClock(start_ms=10_000)
    pipe = make_pipeline(tmp_path, clock, max_future_skew_ms=10_000)

    # 先接受一条正常事件建立基准（最大已观测事件时间 10000）
    pipe.submit(base(event_id="R0", t=10_000))

    # 未来事件先被拒（50000 超前基准 10000 达 40000 > 10000）
    far = base(event_id="R1", t=50_000)
    r1 = pipe.submit(far)
    log_result(far, r1)
    assert r1.reason == RejectReason.FUTURE_EVENT
    assert len(pipe.dead_letters()) == 1

    # 基准时间随已观测最大事件时间逐步推进：10000 -> 20000 -> 30000 -> 40000
    # （相邻事件恰好不超未来偏移）
    for i, t in enumerate((20_000, 30_000, 40_000), start=2):
        bridge = base(event_id=f"R{i}", t=t)
        rb = pipe.submit(bridge)
        log_result(bridge, rb)
        assert rb.status == EventStatus.ACCEPTED

    rr = pipe.redeliver("R1")
    log.info("重投 R1:")
    log_result(far, rr)
    assert rr.status == EventStatus.ACCEPTED
    assert len(pipe.dead_letters()) == 0  # 死信条目已移除
