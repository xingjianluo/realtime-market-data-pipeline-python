"""pytest 公共夹具与日志配置。

每个用例通过 log_decision 打印：输入事件、判定结论、拒绝/接受依据，
满足「单测日志中需要打印输入事件与判定依据」。
"""
from __future__ import annotations

import logging
import sys

import pytest

from market_pipeline.engine import PipelineEngine
from market_pipeline.model import PipelineConfig

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    force=True,
)


def make_event(
    event_id: str,
    event_time_ms: int,
    *,
    source: str = "src-a",
    symbol: str = "AAA",
    price: int = 100,
    quantity: int = 1,
    schema_version: int = 1,
    **extra,
) -> dict:
    """构造测试用原始事件。"""
    payload = {
        "event_id": event_id,
        "source": source,
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
        "event_time_ms": event_time_ms,
        "schema_version": schema_version,
    }
    payload.update(extra)
    return payload


def log_decision(case: str, raw: object, ack, *, note: str = "") -> None:
    """打印输入事件与判定依据（单测日志核心输出）。"""
    logging.info(
        "用例=%s | 输入=%s | 判定=%s | 原因=%s | 窗口=%s | 依据=%s %s",
        case,
        raw,
        ack.decision.value,
        ack.reason.value if ack.reason is not None else "-",
        ack.window_start_ms,
        ack.detail,
        f"| 备注={note}" if note else "",
    )


@pytest.fixture()
def config(tmp_path):
    return PipelineConfig(
        data_dir=str(tmp_path / "data"),
        window_size_ms=10_000,
        allowed_lateness_ms=20_000,
        max_future_ms=30_000,
        max_events_per_window=100,
        max_events_global=10_000,
        max_batch_size=50,
    )


@pytest.fixture()
def engine(config):
    eng = PipelineEngine(config)
    yield eng
    eng.close()
