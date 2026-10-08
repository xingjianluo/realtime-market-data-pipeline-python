"""FastAPI HTTP 接入层。

路由：
- GET  /health                         健康检查与当前水位线；
- POST /events                         提交单条行情（原始 JSON 透传，便于暴露结构错误）；
- POST /events/batch                   批量提交；
- GET  /aggregates                     查询窗口聚合（可按 symbol 过滤）；
- GET  /dead-letters                   查询被拒绝事件及可区分原因；
- POST /dead-letters/{event_id}/redeliver  重新投递死信；
- POST /windows/close                  显式关闭已到期窗口；
- GET  /offsets                        查询各来源消费位点。
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse

from app.config import Config
from app.models import EventStatus
from app.pipeline import Pipeline
from app.storage import dumps as json_dumps


class DecimalJSONResponse(JSONResponse):
    """金额字段（Decimal）按精确十进制数字输出，不经过 float。"""

    def render(self, content: Any) -> bytes:
        return json_dumps(content).encode("utf-8")


def _result_to_dict(result: Any) -> dict:
    return {
        "status": result.status.value,
        "event_id": result.event_id,
        "reason": result.reason.value if result.reason else None,
        "detail": result.detail,
        "window_start_ms": result.window_start_ms,
    }


def _num(value: Decimal | None) -> str | None:
    """聚合金额按**精确十进制字面量**输出（如 0.30、19.99），不做 float 舍入。"""
    return None if value is None else format(value, "f")


def _aggregate_to_dict(agg: Any) -> dict:
    return {
        "symbol": agg.symbol,
        "window_start_ms": agg.window_start_ms,
        "window_end_ms": agg.window_end_ms,
        "closed": agg.closed,
        "count": agg.count,
        "total_quantity": _num(agg.total_quantity),
        "notional": _num(agg.notional),
        "avg_price": _num(agg.avg_price),
        "min_price": _num(agg.min_price),
        "max_price": _num(agg.max_price),
        "first_price": _num(agg.first_price),
        "last_price": _num(agg.last_price),
        "sources": sorted(agg.sources),
        "event_ids": list(agg.event_ids),
    }


def create_app(pipeline: Pipeline | None = None) -> FastAPI:
    config = Config(data_dir=os.environ.get("PIPELINE_DATA_DIR", "data"))
    pipe = pipeline or Pipeline(config)
    app = FastAPI(title="realtime-market-data-pipeline")
    app.state.pipeline = pipe

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "watermark_ms": pipe.watermark(),
                "windows": len(pipe.aggregates()), **pipe.quota()}

    @app.post("/events")
    def submit_event(payload: dict) -> dict:
        result = pipe.submit(payload)
        status_code = 200 if result.status != EventStatus.REJECTED else 422
        return DecimalJSONResponse(status_code=status_code,
                                   content=_result_to_dict(result))

    @app.post("/events/batch")
    def submit_batch(payload: list = Body(...)):
        results = pipe.submit_batch(payload)
        rejected = sum(1 for r in results if r.status == EventStatus.REJECTED)
        status_code = 200 if rejected == 0 else 422
        return DecimalJSONResponse(
            status_code=status_code,
            content={"results": [_result_to_dict(r) for r in results]})

    @app.get("/aggregates")
    def get_aggregates(symbol: str | None = None):
        items = pipe.aggregates()
        if symbol is not None:
            items = [a for a in items if a.symbol == symbol]
        return DecimalJSONResponse(
            content={"aggregates": [_aggregate_to_dict(a) for a in items]})

    @app.get("/dead-letters")
    def get_dead_letters():
        return DecimalJSONResponse(content={"dead_letters": pipe.dead_letters()})

    @app.post("/dead-letters/{event_id}/redeliver")
    def redeliver(event_id: str, source: str | None = None):
        result = pipe.redeliver(event_id, source=source)
        status_code = 200 if result.status != EventStatus.REJECTED else 422
        return DecimalJSONResponse(status_code=status_code,
                                   content=_result_to_dict(result))

    @app.post("/windows/close")
    def close_windows():
        closed = pipe.close_expired()
        return DecimalJSONResponse(
            content={"closed": [_aggregate_to_dict(a) for a in closed]})

    @app.get("/offsets")
    def get_offsets() -> dict:
        return {"offsets": pipe.offsets()}

    @app.get("/quota")
    def get_quota() -> dict:
        """服务级总闸用量：跨来源、跨窗口已受理数与上限。"""
        return pipe.quota()

    return app
