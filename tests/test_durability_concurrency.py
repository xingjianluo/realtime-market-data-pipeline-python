"""重启一致性与并发写入：已确认事件/聚合不丢不重，容量判定确定。"""
from __future__ import annotations

import concurrent.futures

from market_pipeline.engine import PipelineEngine
from market_pipeline.model import RejectReason
from tests.conftest import log_decision, make_event


def _fresh(config):
    return PipelineEngine(config)


def test_accepted_events_survive_restart(config):
    eng = _fresh(config)
    raws = [make_event(f"d{i}", 10_000 + i * 100, price=100 + i, quantity=i + 1)
            for i in range(4)]
    for raw in raws:
        r = eng.ingest(raw, process_time_ms=20_000)
        log_decision("重启前提交", raw, r)
    eng.close_windows(40_000)
    before = eng.query_window("AAA", 10_000)
    offsets_before = eng.get_offsets()
    eng.close()

    reborn = _fresh(config)
    try:
        after = reborn.query_window("AAA", 10_000)
        import logging
        logging.info(
            "用例=重启恢复 | 输入窗口=%s | 重启后聚合=%s", 10_000, after
        )
        assert after == before
        assert after["closed"] is True
        assert reborn.get_watermark() == 40_000
        assert reborn.get_offsets() == offsets_before

        # 重启后重复提交仍被识别，不重复计数
        dup = reborn.ingest(raws[0], process_time_ms=40_000)
        log_decision("重启后重复", raws[0], dup)
        assert dup.reason == RejectReason.DUPLICATE
        assert reborn.query_window("AAA", 10_000)["event_count"] == 4
    finally:
        reborn.close()


class _Dec:
    value = "accepted"


def test_recover_by_replaying_event_log(config):
    # 删除快照后，引擎须能从追加日志重放得到相同聚合
    import os

    eng = _fresh(config)
    for i in range(3):
        eng.ingest(make_event(f"r{i}", 10_000 + i, price=10 + i),
                   process_time_ms=20_000)
    expected = eng.query_window("AAA", 10_000)
    eng.close()
    os.remove(os.path.join(config.data_dir, "snapshot.json"))

    reborn = _fresh(config)
    try:
        assert reborn.query_window("AAA", 10_000) == expected
    finally:
        reborn.close()


def test_concurrent_writes_no_loss_no_double_count(config):
    eng = _fresh(config)
    total = 200
    # 事件时间横跨两个窗口，避免撞上默认每窗 100 条上限
    raws = [make_event(f"c{i:04d}", 10_000 + i * 100, price=100, quantity=1)
            for i in range(total)]

    def submit(raw):
        return eng.ingest(raw, process_time_ms=40_000)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        acks = list(pool.map(submit, raws))

    accepted = [a for a in acks if a.decision.value == "accepted"]
    assert len(accepted) == total
    total_count = sum(
        w["event_count"] for w in eng.list_windows("AAA")
    )
    assert total_count == total
    assert sum(w["total_quantity"] for w in eng.list_windows("AAA")) == total
    assert eng.get_offsets()["src-a"] == total

    # 并发重复提交：每个事件再投一次，全部 duplicate，聚合不变
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        dup_acks = list(pool.map(submit, raws))
    assert all(a.reason == RejectReason.DUPLICATE for a in dup_acks)
    assert sum(w["event_count"] for w in eng.list_windows("AAA")) == total
    sample = acks[0]
    log_decision("并发写入(抽样)", raws[0], sample,
                 note=f"接受 {len(accepted)}/{total}")
    eng.close()


def test_concurrent_writes_respect_window_capacity(config):
    config_kwargs = dict(
        data_dir=config.data_dir,
        window_size_ms=10_000,
        allowed_lateness_ms=0,
        max_future_ms=10**9,
        max_events_per_window=50,
        max_events_global=10_000,
        max_batch_size=500,
    )
    from market_pipeline.model import PipelineConfig

    eng = PipelineEngine(PipelineConfig(**config_kwargs))
    raws = [make_event(f"k{i:04d}", 10_000 + i) for i in range(120)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        acks = list(pool.map(lambda r: eng.ingest(r, process_time_ms=20_000), raws))

    accepted_n = sum(a.decision.value == "accepted" for a in acks)
    rejected_n = sum(
        a.reason == RejectReason.WINDOW_CAPACITY_EXCEEDED for a in acks
    )
    assert accepted_n == 50
    assert rejected_n == 70
    assert eng.query_window("AAA", 10_000)["event_count"] == 50
    log_decision("并发容量(抽样)", raws[0], acks[0],
                 note=f"接受 {accepted_n}, 容量拒绝 {rejected_n}")
    eng.close()
