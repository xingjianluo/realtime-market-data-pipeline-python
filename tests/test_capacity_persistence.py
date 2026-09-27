"""容量受限、批量超限、重启恢复与并发写入的确定性测试。"""

from __future__ import annotations

import logging
import threading

from app.models import EventStatus, RejectReason
from tests.test_pipeline_core import FixedClock, evt, log_result, make_pipeline

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("cap-test")


def test_window_full_rejected_with_distinct_reason(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock, max_events_per_window=3)

    for i in range(3):
        r = pipe.submit(evt(f"W{i}", 500, price=10.0 + i))
        log_result(evt(f"W{i}", 500, price=10.0 + i), r)
        assert r.status == EventStatus.ACCEPTED

    overflow = evt("W3", 600, price=10.0)
    r = pipe.submit(overflow)
    log_result(overflow, r)
    assert r.status == EventStatus.REJECTED
    assert r.reason == RejectReason.WINDOW_FULL

    # 不同标的的窗口不受影响
    other = evt("W4", 600, price=1.0, symbol="000001")
    r2 = pipe.submit(other)
    log_result(other, r2)
    assert r2.status == EventStatus.ACCEPTED

    agg = pipe.aggregates()
    by_sym = {a.symbol: a for a in agg}
    assert by_sym["600000"].count == 3
    assert by_sym["000001"].count == 1
    log.info("容量隔离: 600000=%s 000001=%s",
             by_sym["600000"].count, by_sym["000001"].count)


def test_batch_too_large_rejected_as_whole(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock, max_submit_batch=5)

    batch = [evt(f"B{i}", 1000, price=1.0) for i in range(6)]
    results = pipe.submit_batch(batch)
    log.info("批量输入 %d 条（上限 5），判定：", len(batch))
    assert len(results) == 6
    assert all(r.reason == RejectReason.BATCH_TOO_LARGE for r in results)
    # 整批拒绝，未部分写入
    assert pipe.aggregates() == []

    # 边界：恰好 5 条应全部接受
    ok = [evt(f"C{i}", 1000, price=1.0) for i in range(5)]
    results2 = pipe.submit_batch(ok)
    assert all(r.status == EventStatus.ACCEPTED for r in results2)
    assert pipe.aggregates()[0].count == 5
    log.info("边界批量 5 条全部接受，窗口计数=%s", pipe.aggregates()[0].count)


def test_restart_recovers_events_and_aggregates_no_loss_no_dup(tmp_path):
    clock = FixedClock(start_ms=0)
    data_dir = str(tmp_path)

    # 第一次运行：写入 3 条（含乱序，迟到均在 1000ms 内）
    pipe = make_pipeline(data_dir, clock, allowed_lateness_ms=1000)
    for raw in [evt("P3", 2500, price=13.0),
                evt("P1", 1800, price=10.0),
                evt("P2", 2200, price=11.0)]:
        assert pipe.submit(raw).status == EventStatus.ACCEPTED
    pipe.close()
    log.info("首次运行写入 P1/P2/P3 后关闭")

    # 模拟重启：新建 Pipeline，从 state.json + WAL 恢复
    pipe2 = make_pipeline(data_dir, clock, allowed_lateness_ms=1000)
    aggs = pipe2.aggregates()
    total = sum(a.count for a in aggs)
    assert total == 3  # 不丢失

    # 重复提交 P1：重启后仍幂等，不重复计入
    again = evt("P1", 1800, price=10.0)
    r = pipe2.submit(again)
    log_result(again, r)
    assert r.status == EventStatus.DUPLICATE
    assert sum(a.count for a in pipe2.aggregates()) == 3

    # 位点恢复
    assert pipe2.offsets().get("SRC-A") == 3
    # 聚合内容确定性恢复
    w2 = pipe2.wm.get_aggregate("600000", 2000)
    assert w2 is not None
    assert sorted(w2.event_ids) == ["P2", "P3"]
    assert w2.first_price == 11.0 and w2.last_price == 13.0
    log.info("重启恢复: 总计数=%s 位点=%s 窗口[2000] first=%s last=%s",
             total, pipe2.offsets(), w2.first_price, w2.last_price)


def test_wal_replay_when_snapshot_missing(tmp_path):
    clock = FixedClock(start_ms=0)
    data_dir = str(tmp_path)
    pipe = make_pipeline(data_dir, clock)
    for i in range(3):
        pipe.submit(evt(f"X{i}", 1000 + i, price=1.0))
    pipe.close()

    # 删除快照，仅保留 WAL，验证兜底重放路径
    (tmp_path / "state.json").unlink()
    pipe2 = make_pipeline(data_dir, clock)
    assert sum(a.count for a in pipe2.aggregates()) == 3
    log.info("仅 WAL 重放恢复成功，计数=%s",
             sum(a.count for a in pipe2.aggregates()))


def test_concurrent_writers_are_deterministic_and_dedup_works(tmp_path):
    clock = FixedClock(start_ms=0)
    pipe = make_pipeline(tmp_path, clock)

    errors: list[Exception] = []

    def worker(base_id: int) -> None:
        try:
            # 每个线程写自己的 20 条 + 与其他线程重复的公共 10 条
            for i in range(20):
                pipe.submit(evt(f"T{base_id}_{i}", 10_000 + (i % 5) * 100,
                                price=1.0 + (i % 5), symbol="CONC"))
            for i in range(10):
                pipe.submit(evt(f"COMMON{i}", 10_000, price=5.0, symbol="CONC"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    # 唯一事件 = 4 线程 * 20 私有 + 10 公共 = 90
    accepted = pipe.storage.load_wal()
    ids = [r["event_id"] for r in accepted]
    assert len(ids) == len(set(ids)) == 90
    total = sum(a.count for a in pipe.aggregates() if a.symbol == "CONC")
    assert total == 90
    log.info("4 线程并发：去重后唯一事件=%s，无异常", total)
