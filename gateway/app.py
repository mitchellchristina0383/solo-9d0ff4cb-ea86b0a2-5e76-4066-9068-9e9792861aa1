"""HTTP 入口网关（FastAPI + httpx），支持轮询转发与可选会话绑定。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康且未排空」的节点；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，已在途的请求继续等待原响应返回；
  * 排空 API 阻塞到在途请求全部结束后才报告 drained=true；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态，在途计数靠 try/finally 保证准确。

可选会话绑定（GET 流程）：
  * GET /proxy/session/open 在「可接流」节点上开立上游进程内会话，
    返回不可伪造的绑定凭据（HMAC-SHA256 签名，载荷含节点名 + 上游进程 instance_id）；
  * 之后携带凭据的查询只能进入凭据指定的原节点原进程，普通 GET 继续轮询；
  * 凭据无状态、只存在客户端：网关重启（上游未重启）后凭据仍可验证、仍可使用；
  * 上游进程重启 -> instance_id 变化，旧凭据明确失效（410 Gone），
    绝不会误接到同名新实例；
  * 绑定节点排空或探测故障期间返回 503 暂不可用并保留绑定，恢复后可继续查询，
    绝不暗中改投另一节点；新会话只选可接流节点。
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))
# 会话凭据 HMAC 密钥：compose 固定注入以保证网关滚动重启后旧凭据仍可用；
# 未注入时随机生成（仅本进程有效，重启即作废）。
SESSION_SECRET = os.environ.get("SESSION_SECRET", "").encode("utf-8") or secrets.token_bytes(32)
SESSION_HEADER = "X-Session-Token"
SESSION_QUERY_PARAM = "session_token"

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


# ----------------------------- 绑定凭据（无状态 HMAC 令牌） -----------------------------

def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def mint_session_token(node: str, instance_id: str, session_id: str) -> str:
    """签发绑定凭据：base64url(payload).base64url(HMAC-SHA256(secret, payload))。"""
    payload = {
        "v": 1,
        "node": node,
        "instance": instance_id,
        "sid": session_id,
        "iat": int(time.time()),
    }
    payload_b = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = hmac.new(SESSION_SECRET, payload_b, hashlib.sha256).digest()
    return f"{_b64url(payload_b)}.{_b64url(sig)}"


def verify_session_token(token: str) -> Optional[dict]:
    """验证签名与结构；任何异常都返回 None（按无效凭据处理）。"""
    try:
        payload_part, sig_part = token.split(".", 1)
        payload_b = _b64url_decode(payload_part)
        sig = _b64url_decode(sig_part)
    except (ValueError, TypeError):
        return None
    expected = hmac.new(SESSION_SECRET, payload_b, hashlib.sha256).digest()
    # 常量时间比较，杜绝时序侧信道
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(payload_b.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    required = ("v", "node", "instance", "sid", "iat")
    if not all(k in payload for k in required):
        return None
    return payload


# ----------------------------- 节点状态 -----------------------------

@dataclass
class NodeState:
    name: str
    base_url: str
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    inflight: int = 0
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    # 上游进程实例身份缓存：来自 /instance；None 表示尚未发现
    instance_id: Optional[str] = None
    instance_uptime: Optional[float] = None
    instance_discovered_at: Optional[float] = None
    # 会话绑定统计
    session_opened: int = 0
    session_bound_hits: int = 0
    session_rejected: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)

    def can_serve(self) -> bool:
        return self.healthy and not self.draining

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "instance_id": self.instance_id,
            "instance_uptime": self.instance_uptime,
            "instance_discovered_at": self.instance_discovered_at,
            "session_opened": self.session_opened,
            "session_bound_hits": self.session_bound_hits,
            "session_rejected": self.session_rejected,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
        }


class Gateway:
    def __init__(self, nodes: list[NodeState]) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        # 一把条件变量保护所有节点状态；请求结束 / 排空等待都在其上通知
        self.cond = asyncio.Condition()
        self._rr = 0
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def pick_node(self) -> Optional[NodeState]:
        """轮询选取健康且未排空的节点，并立即登记在途计数。

        新会话开立与普通 GET 都走这里，因此新会话只会落到可接流节点。
        """
        async with self.cond:
            n = len(self.nodes)
            for offset in range(n):
                idx = (self._rr + offset) % n
                node = self.nodes[idx]
                if node.can_serve():
                    self._rr = (idx + 1) % n
                    node.inflight += 1
                    node.rr_hits += 1
                    return node
            return None

    async def bind_inflight(self, node: NodeState) -> None:
        """绑定请求确定可转发后登记在途计数（排空语义与轮询请求一致）。"""
        async with self.cond:
            node.inflight += 1
            node.session_bound_hits += 1

    async def release_node(self, node: NodeState, ok: bool) -> None:
        async with self.cond:
            node.inflight -= 1
            if ok:
                node.success_count += 1
            else:
                node.failure_count += 1
            # 在途归零可能让 drain 等待者满足条件
            self.cond.notify_all()

    async def count_rejected(self, node: NodeState) -> None:
        async with self.cond:
            node.session_rejected += 1

    async def mark_unhealthy(self, node: NodeState, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining。"""
        async with self.cond:
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = time.time()
            node.last_change_at = time.time()

    async def discover_instance(self, node: NodeState) -> Optional[dict]:
        """拉取上游 /instance 身份信息并刷新缓存。失败返回 None，不抛异常。"""
        assert self.client is not None
        try:
            resp = await self.client.get(f"{node.base_url}/instance", timeout=PROBE_TIMEOUT)
            if resp.status_code >= 500:
                return None
            data = resp.json()
        except (httpx.HTTPError, ValueError):
            return None
        instance_id = data.get("instance_id")
        if not isinstance(instance_id, str) or not instance_id:
            return None
        async with self.cond:
            node.instance_id = instance_id
            node.instance_uptime = data.get("uptime")
            node.instance_discovered_at = time.time()
        return data

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。成功只置 healthy=True，绝不解除 draining。

        探测同时刷新上游进程 instance_id 缓存；/instance 是控制面端点，
        即使业务面处于 error 模式也可取回身份，从而及时识别上游重启。
        """
        assert self.client is not None
        try:
            resp = await self.client.get(
                f"{node.base_url}/", timeout=PROBE_TIMEOUT
            )
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        async with self.cond:
            node.healthy = ok
            # 注意：这里刻意不修改 node.draining
            node.last_probe_at = time.time()
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = time.time()

        # 身份发现独立于健康结论：只要进程可达就刷新实例身份
        await self.discover_instance(node)

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def drain(
        self, node: NodeState, timeout: Optional[float]
    ) -> tuple[bool, int]:
        """置排空标志并等待在途清零。返回 (是否排空完成, 剩余在途数)。"""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            node.draining = True
            node.last_change_at = time.time()
            while node.inflight > 0:
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False, node.inflight
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新计算剩余时间 / 在途数
            return True, 0

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测，由探测决定能否接流。"""
        async with self.cond:
            node.draining = False
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
        await self.probe_one(node)


# ----------------------------- 应用生命周期 -----------------------------

gateway = Gateway([NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass


app = FastAPI(
    title="http-entry-gateway",
    version="2.0.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康且未排空的上游，支持排空/恢复/状态管理，"
        "无可接流节点时返回 503。可选会话绑定：GET /proxy/session/open 开立会话并"
        "取得 HMAC 绑定凭据，携带凭据的查询固定路由到原节点原进程；上游进程重启后"
        "旧凭据明确失效（410），绑定节点排空/故障期间返回 503 且不改投。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发辅助 -----------------------------

def _extract_token(request: Request) -> Optional[str]:
    # 头优先，查询参数兜底（便于浏览器 / /docs 直接演示）
    token = request.headers.get(SESSION_HEADER)
    if token:
        return token.strip()
    token = request.query_params.get(SESSION_QUERY_PARAM)
    return token.strip() if token else None


async def _forward(node: NodeState, path: str, request: Request) -> Response:
    """把 GET 请求转发到指定节点并透传响应；调用方负责在途计数的登记与归还。"""
    target_url = f"{node.base_url}/{path}"
    ok = False
    try:
        try:
            upstream_resp = await gateway.client.request(
                "GET",
                target_url,
                params=request.query_params,
                timeout=REQUEST_TIMEOUT,
            )
        except httpx.TimeoutException as exc:
            await gateway.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
            return JSONResponse(
                status_code=504,
                content={"error": "upstream_timeout", "node": node.name},
            )
        except httpx.RequestError as exc:
            await gateway.mark_unhealthy(
                node, f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            return JSONResponse(
                status_code=502,
                content={"error": "upstream_unreachable", "node": node.name},
            )

        # 5xx 视为上游故障：如实透传状态码并降级，绝不虚报成功
        if upstream_resp.status_code >= 500:
            await gateway.mark_unhealthy(
                node, f"上游返回 HTTP {upstream_resp.status_code}"
            )

        ok = upstream_resp.status_code < 500
        headers = {
            k: v
            for k, v in upstream_resp.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        headers["X-Gateway"] = "http-entry-gateway"
        headers["X-Upstream-Node"] = node.name
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=headers,
            media_type=upstream_resp.headers.get("content-type"),
        )
    finally:
        # 无论成功、失败还是取消，在途计数都必须准确归还
        await gateway.release_node(node, ok)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET",
        "session_binding": [
            "GET /proxy/session/open            # 开立会话，返回 token 与绑定信息",
            f"GET /proxy/...  带头 {SESSION_HEADER} 或查询参数 {SESSION_QUERY_PARAM}=...  # 绑定查询",
        ],
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
        ],
    }


@app.get("/proxy/session/open", tags=["session"])
async def session_open(request: Request) -> Response:
    """在可接流节点上开立上游进程内会话，并签发不可伪造的绑定凭据。

    只选择 healthy 且未排空的节点（与轮询同一条选取路径）；
    无可用节点时返回 503，不会在不可接流节点上建会话。
    """
    node = await gateway.pick_node()
    if node is None:
        return JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可开立会话",
            },
        )
    try:
        try:
            resp = await gateway.client.get(
                f"{node.base_url}/session/open", timeout=REQUEST_TIMEOUT
            )
        except httpx.TimeoutException:
            await gateway.mark_unhealthy(node, "会话开立超时")
            return JSONResponse(
                status_code=504,
                content={"error": "upstream_timeout", "node": node.name, "stage": "session_open"},
            )
        except httpx.RequestError as exc:
            await gateway.mark_unhealthy(node, f"会话开立连接错误: {type(exc).__name__}")
            return JSONResponse(
                status_code=502,
                content={"error": "upstream_unreachable", "node": node.name, "stage": "session_open"},
            )

        if resp.status_code >= 500:
            await gateway.mark_unhealthy(node, f"会话开立 HTTP {resp.status_code}")
            return JSONResponse(
                status_code=resp.status_code,
                content={"error": "upstream_error", "node": node.name, "stage": "session_open"},
            )
        data = resp.json()
        instance_id = data["instance_id"]
        session_id = data["session_id"]
        token = mint_session_token(node.name, instance_id, session_id)
        async with gateway.cond:
            node.instance_id = instance_id
            node.session_opened += 1
        body = {
            "token": token,
            "node": node.name,
            "instance_id": instance_id,
            "session_id": session_id,
            "usage": (
                f"后续查询携带头 {SESSION_HEADER}: <token>，"
                f"或查询参数 ?{SESSION_QUERY_PARAM}=<token>"
            ),
        }
        headers = {
            SESSION_HEADER: token,
            "X-Gateway": "http-entry-gateway",
            "X-Upstream-Node": node.name,
        }
        return JSONResponse(status_code=201, content=body, headers=headers)
    finally:
        await gateway.release_node(node, True)


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    token = _extract_token(request)

    # ---------------- 绑定查询：只能进入凭据指定的原节点原进程 ----------------
    if token is not None:
        claims = verify_session_token(token)
        if claims is None:
            return JSONResponse(
                status_code=401,
                content={
                    "error": "invalid_session_token",
                    "detail": "凭据缺失、格式错误或签名不匹配，网关无法验证其绑定关系",
                },
            )

        node = gateway.get(claims["node"])
        if node is None:
            return JSONResponse(
                status_code=404,
                content={
                    "error": "bound_node_unknown",
                    "node": claims["node"],
                    "detail": "凭据绑定的节点名不在当前网关配置中",
                },
            )

        # 排空或故障：返回暂不可用并保留绑定，绝不改投另一节点
        if not node.can_serve():
            await gateway.count_rejected(node)
            reason = "node_draining" if node.draining else "node_unhealthy"
            return JSONResponse(
                status_code=503,
                content={
                    "error": reason,
                    "node": node.name,
                    "detail": "绑定节点暂不可接流，绑定关系保留；节点恢复后可凭同一凭据继续查询",
                },
            )

        # 缓存未命中（如网关刚重启）时按需发现一次上游进程身份
        async with gateway.cond:
            cached_instance = node.instance_id
        if cached_instance is None:
            discovered = await gateway.discover_instance(node)
            if discovered is None:
                await gateway.count_rejected(node)
                return JSONResponse(
                    status_code=503,
                    content={
                        "error": "instance_undiscoverable",
                        "node": node.name,
                        "detail": "暂时无法确认上游进程身份，绑定关系保留，请稍后重试",
                    },
                )
            cached_instance = discovered["instance_id"]

        # 上游进程已重启（instance_id 变化）：旧凭据明确失效，拒绝并提示重新开立
        if not hmac.compare_digest(cached_instance, claims["instance"]):
            await gateway.count_rejected(node)
            return JSONResponse(
                status_code=410,
                content={
                    "error": "session_bound_instance_gone",
                    "node": node.name,
                    "bound_instance_id": claims["instance"],
                    "current_instance_id": cached_instance,
                    "detail": "上游进程已重启，进程内会话与旧绑定凭据已失效；请重新调用 /proxy/session/open 开立会话",
                },
            )

        await gateway.bind_inflight(node)
        return await _forward(node, path, request)

    # ---------------- 普通 GET：继续轮询 ----------------
    node = await gateway.pick_node()
    if node is None:
        return JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
            },
        )
    return await _forward(node, path, request)


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "session": {
                "header": SESSION_HEADER,
                "query_param": SESSION_QUERY_PARAM,
                "secret_configured": bool(os.environ.get("SESSION_SECRET")),
            },
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求，并阻塞等待在途请求全部结束。

    只有在途数归零才返回 drained=true；超时仍有在途请求时返回 202，
    drained=false 且节点继续保持排空状态。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    drained, inflight = await gateway.drain(node, timeout)
    body = {
        "node": name,
        "draining": True,
        "drained": drained,
        "inflight": inflight,
        "detail": "在途请求已全部结束" if drained else "等待在途请求结束超时，节点仍处于排空中",
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "instance_id": snap["instance_id"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以健康探测结论为准",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}
