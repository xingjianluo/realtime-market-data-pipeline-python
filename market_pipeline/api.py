"""FastAPI HTTP 接入层。

端点：
- POST /events            接入单条事件（可带 process_time_ms 查询参数）
- POST /events/batch      批量接入（超量整体拒绝，原因 batch_too_large）
- GET  /windows           列出聚合（?symbol= 过滤）
- GET  /windows/{symbol}/{window_start_ms}  查询单窗
- POST /windows/close     以处理时间推进水位线并关窗（body: {"now_ms": ...}）
- GET  /offsets           各来源消费位点
- GET  /health            存活与水位线
"""
from __future__ import annotations

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from market_pipeline.engine import PipelineEngine
from market_pipeline.model import PipelineConfig


def create_app(config: PipelineConfig | None = None) -> FastAPI:
    """构建 FastAPI 应用。"""
    cfg = config or PipelineConfig()
    engine = PipelineEngine(cfg)
    app = FastAPI(
        title="本地实时行情接入与窗口聚合",
        version="0.1.0",
    )
    app.state.engine = engine

    @app.post("/events")
    def post_event(
        payload: dict = Body(...),
        process_time_ms: int | None = Query(default=None),
    ):
        ack = engine.ingest(payload, process_time_ms=process_time_ms)
        return _ack_response(ack)

    @app.post("/events/batch")
    def post_batch(
        payload: list[dict] = Body(...),
        process_time_ms: int | None = Query(default=None),
    ):
        acks = engine.ingest_batch(payload, process_time_ms=process_time_ms)
        status = 207 if any(a.decision.value == "rejected" for a in acks) else 200
        return JSONResponse(
            status_code=status,
            content={"results": [a.to_dict() for a in acks]},
        )

    @app.get("/windows")
    def get_windows(symbol: str | None = Query(default=None)):
        return {"windows": engine.list_windows(symbol)}

    @app.get("/windows/{symbol}/{window_start_ms}")
    def get_window(symbol: str, window_start_ms: int):
        agg = engine.query_window(symbol, window_start_ms)
        if agg is None:
            raise HTTPException(status_code=404, detail="window not found")
        return agg

    @app.post("/windows/close")
    def post_close(body: dict = Body(...)):
        now_ms = body.get("now_ms")
        if not isinstance(now_ms, int) or isinstance(now_ms, bool) or now_ms < 0:
            raise HTTPException(status_code=422, detail="now_ms 必须是非负整数")
        return {"closed": engine.close_windows(int(now_ms)), "watermark_ms": engine.get_watermark()}

    @app.get("/offsets")
    def get_offsets():
        return {"offsets": engine.get_offsets()}

    @app.get("/health")
    def health():
        return {"status": "ok", "watermark_ms": engine.get_watermark()}

    return app


def _ack_response(ack) -> JSONResponse:
    """单条结论：接受 200，拒绝 422（结构化原因，调用方据 reason 重投）。"""
    status = 200 if ack.decision.value == "accepted" else 422
    return JSONResponse(status_code=status, content=ack.to_dict())
