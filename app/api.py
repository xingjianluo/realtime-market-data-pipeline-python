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
from typing import Any

from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse

from app.config import Config
from app.models import EventStatus
from app.pipeline import Pipeline


def _result_to_dict(result: Any) -> dict:
    return {
        "status": result.status.value,
        "event_id": result.event_id,
        "reason": result.reason.value if result.reason else None,
        "detail": result.detail,
        "window_start_ms": result.window_start_ms,
    }


def _aggregate_to_dict(agg: Any) -> dict:
    return {
        "symbol": agg.symbol,
        "window_start_ms": agg.window_start_ms,
        "window_end_ms": agg.window_end_ms,
        "closed": agg.closed,
        "count": agg.count,
        "total_quantity": agg.total_quantity,
        "notional": round(agg.notional, 12),
        "min_price": agg.min_price,
        "max_price": agg.max_price,
        "first_price": agg.first_price,
        "last_price": agg.last_price,
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
                "windows": len(pipe.aggregates())}

    @app.post("/events")
    def submit_event(payload: dict) -> dict:
        result = pipe.submit(payload)
        status_code = 200 if result.status != EventStatus.REJECTED else 422
        return JSONResponse(status_code=status_code, content=_result_to_dict(result))

    @app.post("/events/batch")
    def submit_batch(payload: list = Body(...)) -> dict:
        results = pipe.submit_batch(payload)
        rejected = sum(1 for r in results if r.status == EventStatus.REJECTED)
        status_code = 200 if rejected == 0 else 422
        return JSONResponse(status_code=status_code,
                            content={"results": [_result_to_dict(r) for r in results]})

    @app.get("/aggregates")
    def get_aggregates(symbol: str | None = None) -> dict:
        items = pipe.aggregates()
        if symbol is not None:
            items = [a for a in items if a.symbol == symbol]
        return {"aggregates": [_aggregate_to_dict(a) for a in items]}

    @app.get("/dead-letters")
    def get_dead_letters() -> dict:
        return {"dead_letters": pipe.dead_letters()}

    @app.post("/dead-letters/{event_id}/redeliver")
    def redeliver(event_id: str) -> dict:
        result = pipe.redeliver(event_id)
        status_code = 200 if result.status != EventStatus.REJECTED else 422
        return JSONResponse(status_code=status_code, content=_result_to_dict(result))

    @app.post("/windows/close")
    def close_windows() -> dict:
        closed = pipe.close_expired()
        return {"closed": [_aggregate_to_dict(a) for a in closed]}

    @app.get("/offsets")
    def get_offsets() -> dict:
        return {"offsets": pipe.offsets()}

    return app
