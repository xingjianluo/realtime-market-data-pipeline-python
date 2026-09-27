"""HTTP 端到端：接入、查询、关窗、位点与结构化拒绝原因。"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from market_pipeline.api import create_app
from market_pipeline.model import PipelineConfig


@pytest.fixture()
def client(tmp_path):
    cfg = PipelineConfig(
        data_dir=str(tmp_path / "data"),
        window_size_ms=10_000,
        allowed_lateness_ms=20_000,
        max_future_ms=30_000,
        max_events_per_window=2,
        max_events_global=100,
        max_batch_size=3,
    )
    with TestClient(create_app(cfg)) as c:
        yield c


def test_post_and_query_window(client):
    event = {"event_id": "h1", "source": "s", "symbol": "AAA",
             "price": 100, "quantity": 2, "event_time_ms": 10500,
             "schema_version": 1}
    r = client.post("/events?process_time_ms=11000", json=event)
    logging.info("API 用例=接入 输入=%s 输出=%s", event, r.json())
    assert r.status_code == 200
    assert r.json()["window_start_ms"] == 10_000

    r = client.get("/windows/AAA/10000")
    assert r.status_code == 200
    assert r.json()["event_count"] == 1
    assert r.json()["vwap"] == 100.0


def test_duplicate_returns_422_with_reason(client):
    event = {"event_id": "h2", "source": "s", "symbol": "AAA",
             "price": 100, "quantity": 1, "event_time_ms": 10500,
             "schema_version": 1}
    client.post("/events?process_time_ms=11000", json=event)
    r = client.post("/events?process_time_ms=11000", json=event)
    logging.info("API 用例=重复 输入=%s 状态=%s 输出=%s", event,
                 r.status_code, r.json())
    assert r.status_code == 422
    assert r.json()["reason"] == "duplicate"


def test_batch_too_large_entire_rejected(client):
    batch = [
        {"event_id": f"x{i}", "source": "s", "symbol": "AAA",
         "price": 1, "quantity": 1, "event_time_ms": 10000 + i,
         "schema_version": 1}
        for i in range(4)
    ]
    r = client.post("/events/batch", json=batch)
    logging.info("API 用例=超量批量 状态=%s 首条原因=%s",
                 r.status_code, r.json()["results"][0]["reason"])
    assert r.status_code == 207
    assert all(x["reason"] == "batch_too_large" for x in r.json()["results"])
    assert client.get("/windows").json()["windows"] == []


def test_close_and_offsets(client):
    event = {"event_id": "h3", "source": "s", "symbol": "AAA",
             "price": 100, "quantity": 1, "event_time_ms": 10500,
             "schema_version": 1}
    client.post("/events?process_time_ms=11000", json=event)
    r = client.post("/windows/close", json={"now_ms": 40000})
    assert r.status_code == 200
    assert {"symbol": "AAA", "window_start_ms": 10000} in r.json()["closed"]

    late = {"event_id": "h4", "source": "s", "symbol": "AAA",
            "price": 1, "quantity": 1, "event_time_ms": 10800,
            "schema_version": 1}
    rl = client.post("/events?process_time_ms=40000", json=late)
    assert rl.status_code == 422
    assert rl.json()["reason"] == "window_too_late"
    assert client.get("/offsets").json()["offsets"] == {"s": 2}
