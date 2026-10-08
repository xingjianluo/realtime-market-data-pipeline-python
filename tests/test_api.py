"""HTTP 端到端测试：提交、去重、拒绝原因、聚合、死信重投、位点。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Config
from app.pipeline import Pipeline

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("api-test")


class FixedClock:
    def __init__(self, n: int = 0) -> None:
        self.n = n

    def __call__(self) -> int:
        return self.n


@pytest.fixture()
def client(tmp_path):
    cfg = Config(data_dir=str(tmp_path), allowed_lateness_ms=1000)
    pipe = Pipeline(cfg, clock=FixedClock(0))
    app = create_app(pipe)
    return TestClient(app)


def event(eid: str, t: int, price: float = 10.0, **kw) -> dict:
    return {"event_id": eid, "source": "SRC-A", "symbol": "600000",
            "price": price, "quantity": 1.0, "event_time_ms": t,
            "offset": int(eid[1:]), **kw}


def test_submit_duplicate_and_aggregate_flow(client):
    r1 = client.post("/events", json=event("A1", 1500))
    log.info("POST /events %s -> %s", event("A1", 1500), r1.json())
    assert r1.status_code == 200 and r1.json()["status"] == "ACCEPTED"

    r2 = client.post("/events", json=event("A1", 1500))
    log.info("POST /events(重复) -> %s", r2.json())
    assert r2.json()["status"] == "DUPLICATE"

    r3 = client.post("/events", json={**event("A2", 2200), "price": "bad"})
    log.info("POST /events(坏类型) -> %s", r3.json())
    assert r3.status_code == 422
    assert r3.json()["reason"] == "TYPE_MISMATCH"

    aggs = client.get("/aggregates").json()["aggregates"]
    assert len(aggs) == 1 and aggs[0]["count"] == 1
    log.info("GET /aggregates -> %s", aggs)

    dls = client.get("/dead-letters").json()["dead_letters"]
    assert len(dls) == 1 and dls[0]["reason"] == "TYPE_MISMATCH"
    assert client.get("/health").json()["watermark_ms"] == 1500 - 1000
    assert client.get("/offsets").json()["offsets"] == {"SRC-A": 1}
    # 服务级总闸视图：已受理 1 条，额度信息完整（新能力不影响既有流程）
    cap = client.get("/capacity").json()
    assert cap["accepted"] == 1 and cap["limit"] is not None
    assert cap["remaining"] == cap["limit"] - 1
    # 提交结果携带来源字段（多来源语义在 HTTP 层可见）
    assert r1.json()["source"] == "SRC-A"


def test_batch_and_window_close_and_redeliver(client):
    # 批量超限
    big = [event(f"B{i}", 1000) for i in range(999)]
    r = client.post("/events/batch", json=big)
    assert r.status_code == 422
    assert all(x["reason"] == "BATCH_TOO_LARGE"
               for x in r.json()["results"])

    # 正常批量
    ok = [event(f"C{i}", 1000 + i * 100, price=10.0 + i) for i in range(3)]
    r2 = client.post("/events/batch", json=ok)
    assert r2.status_code == 200

    # 高水位事件使早窗口关闭，再投迟到事件被拒
    client.post("/events", json=event("C9", 5000, price=20.0))
    late = event("L1", 1050)
    r3 = client.post("/events", json=late)
    log.info("迟到事件 %s -> %s", late, r3.json())
    assert r3.json()["reason"] == "WINDOW_CLOSED"

    # 重投仍关闭 => 保留死信
    rr = client.post("/dead-letters/L1/redeliver")
    assert rr.json()["reason"] == "WINDOW_CLOSED"

    # 显式关闭到期窗口
    closed = client.post("/windows/close").json()["closed"]
    starts = {c["window_start_ms"] for c in closed}
    assert 1000 in starts
    log.info("关闭窗口: %s", starts)
