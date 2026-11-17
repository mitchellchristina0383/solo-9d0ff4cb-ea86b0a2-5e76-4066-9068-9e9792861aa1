"""模拟上游服务：进程内会话 + 慢请求 + 故障模式。

进程身份：
  * 启动时生成随机 instance_id（uuid4），重启即变；
  * 网关用 (节点名, instance_id) 区分“同名的另一个新进程”，旧绑定凭据因此明确失效。

会话（仅存在于本进程内存，重启即全部丢失）：
  GET  /session/open            开立会话，返回 session_id / instance_id / 节点名
  GET  /session/query?sid=...   查询会话；sid 不存在 -> 404
  GET  /instance                进程身份与 uptime（供网关发现/核对实例）

管理接口（仅用于测试/演练，不经过网关）：
  GET  /control/state            查看当前模式、在途请求数、会话数与 instance_id
  POST /control/mode?mode=...    切换模式: healthy | slow | error | hang

业务接口:
  GET  /slow?seconds=N            延迟 N 秒后返回（慢请求）
  GET  /echo                      回显请求信息
  GET  /flaky                     按当前模式返回 200 或 500
  GET  /                          健康信息
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

# 节点运行模式（演示用的全局状态）
_mode = "healthy"
_inflight = 0
_started = time.time()
_node_id = "unknown"
# 进程实例身份：每次进程启动重新生成，天然不可跨进程复用
_instance_id = str(uuid.uuid4())

# 进程内会话存储：sid -> {"opened_at": ..., "queries": ...}
_sessions: dict[str, dict] = {}
_sessions_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _node_id
    _node_id = os.environ.get("NODE_ID", "unknown")
    yield


app = FastAPI(title="mock-upstream", version="2.0.0", lifespan=lifespan)


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


@app.get("/instance")
async def instance() -> dict:
    """进程身份端点：网关据此发现/核对上游实例。

    不受故障模式影响（控制面性质），保证网关在 error 模式下也能取得
    新进程的 instance_id 并据此让旧凭据明确失效。
    """
    return {
        "node": _node_id,
        "instance_id": _instance_id,
        "uptime": round(time.time() - _started, 3),
        "sessions": len(_sessions),
    }


@app.get("/session/open")
async def session_open() -> JSONResponse:
    """在进程内存中开立一个会话。会话生命周期与本进程一致。"""
    global _inflight
    sid = uuid.uuid4().hex
    async with _sessions_lock:
        _sessions[sid] = {"opened_at": time.time(), "queries": 0}
        session_count = len(_sessions)
    _inflight += 1
    try:
        return JSONResponse(
            status_code=201,
            content={
                "node": _node_id,
                "instance_id": _instance_id,
                "session_id": sid,
                "session_count": session_count,
            },
        )
    finally:
        _inflight -= 1


@app.get("/session/query")
async def session_query(sid: str = Query(..., min_length=1)) -> JSONResponse:
    """查询本进程内的会话；sid 不属于本进程（含进程重启后的旧 sid）-> 404。"""
    global _inflight
    _inflight += 1
    try:
        async with _sessions_lock:
            sess = _sessions.get(sid)
            if sess is None:
                return JSONResponse(
                    status_code=404,
                    content={
                        "node": _node_id,
                        "instance_id": _instance_id,
                        "session_id": sid,
                        "error": "session_not_found",
                        "detail": "本进程内不存在该会话（可能从未开立，或进程已重启导致内存会话丢失）",
                    },
                )
            sess["queries"] += 1
            queries = sess["queries"]
        return JSONResponse(
            status_code=200,
            content={
                "node": _node_id,
                "instance_id": _instance_id,
                "session_id": sid,
                "queries": queries,
                "opened_at": sess["opened_at"],
                "age": round(time.time() - sess["opened_at"], 3),
            },
        )
    finally:
        _inflight -= 1


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
    return {
        "node": _node_id,
        "instance_id": _instance_id,
        "mode": _mode,
        "inflight": _inflight,
        "sessions": len(_sessions),
        "uptime": round(time.time() - _started, 2),
    }


@app.post("/control/mode")
async def set_mode(mode: str = Query(..., pattern="^(healthy|slow|error|hang)$")) -> dict:
    global _mode
    old = _mode
    _mode = mode
    return {"node": _node_id, "previous_mode": old, "mode": _mode}
