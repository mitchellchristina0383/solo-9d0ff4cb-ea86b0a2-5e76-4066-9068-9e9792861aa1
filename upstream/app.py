"""模拟上游服务：可产生慢请求、可被手动置为故障模式。

管理接口（仅用于测试/演练，不经过网关）：
  GET  /control/state            查看当前模式与在途请求数
  POST /control/mode?mode=...    切换模式: healthy | slow | error | hang

业务接口:
  GET  /slow?seconds=N            延迟 N 秒后返回（慢请求）
  GET  /echo                      回显请求信息
  GET  /flaky                     按当前模式返回 200 或 500
  GET  /                          健康信息
"""
from __future__ import annotations

import asyncio
import time

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

# 节点运行模式（演示用的全局状态）
_mode = "healthy"
_inflight = 0
_started = time.time()
_node_id = "unknown"


@asynccontextmanager
async def lifespan(app: FastAPI):
    import os

    global _node_id
    _node_id = os.environ.get("NODE_ID", "unknown")
    yield


app = FastAPI(title="mock-upstream", version="1.0.0", lifespan=lifespan)


@app.get("/")
async def root() -> JSONResponse:
    """健康端点：如实反映节点模式。

    error -> 503（探测据此判定节点不健康）；hang -> 延迟（探测超时判定）；
    其余 -> 200。注意控制面状态请查 /control/state，该接口不受模式影响。
    """
    if _mode == "error":
        return JSONResponse(
            status_code=503,
            content={"node": _node_id, "mode": _mode, "inflight": _inflight,
                     "error": "upstream in error mode"},
        )
    if _mode == "hang":
        await asyncio.sleep(120)
    return JSONResponse(
        status_code=200,
        content={
            "node": _node_id,
            "mode": _mode,
            "inflight": _inflight,
            "uptime": round(time.time() - _started, 2),
        },
    )


@app.get("/echo")
async def echo(request: Request) -> dict:
    return {
        "node": _node_id,
        "path": "/echo",
        "query_params": dict(request.query_params),
        "headers": {k: v for k, v in request.headers.items()},
    }


@app.get("/slow")
async def slow(seconds: float = Query(2.0, ge=0, le=60)) -> JSONResponse:
    """慢请求：用于验证排空期间在途请求不被截断。"""
    global _inflight
    _inflight += 1
    started = time.time()
    try:
        await asyncio.sleep(seconds)
        if _mode == "error":
            return JSONResponse(
                status_code=500,
                content={"node": _node_id, "error": "upstream in error mode"},
            )
        return JSONResponse(
            status_code=200,
            content={
                "node": _node_id,
                "path": "/slow",
                "slept": round(time.time() - started, 3),
                "requested": seconds,
            },
        )
    finally:
        _inflight -= 1


@app.get("/flaky")
async def flaky() -> JSONResponse:
    """按模式返回：error -> 500；hang -> 挂起（由网关超时兜底）；其余 -> 200。"""
    if _mode == "error":
        return JSONResponse(
            status_code=500,
            content={"node": _node_id, "error": "upstream in error mode"},
        )
    if _mode == "hang":
        # 挂足够久，让网关的请求超时先触发
        await asyncio.sleep(120)
    return JSONResponse(status_code=200, content={"node": _node_id, "ok": True})


@app.get("/control/state")
async def control_state() -> dict:
    return {"node": _node_id, "mode": _mode, "inflight": _inflight}


@app.post("/control/mode")
async def set_mode(mode: str = Query(..., pattern="^(healthy|slow|error|hang)$")) -> dict:
    global _mode
    old = _mode
    _mode = mode
    return {"node": _node_id, "previous_mode": old, "mode": _mode}
