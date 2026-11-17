#!/usr/bin/env python3
"""网关本地自测（不依赖 docker，使用 stdlib + 三个 uvicorn 子进程）。

覆盖关键场景：
  1.  轮询分发
  2.  状态 API
  3.  空闲节点排空 -> 立即 drained=true
  4.  排空后新请求不进入该节点
  5.  有在途慢请求时排空 -> 等在途结束才完成，且原响应可取回
  6.  全部不可接流 -> 503
  7.  上游 5xx -> 不虚报成功、节点降级、在途归零
  8.  探测成功不解除排空（交错核心）
  9.  恢复不绕过故障探测（交错核心）
  10. 仅 GET：POST 转发路径返回 405
  11. /docs 可打开
  12. 排空等待超时 -> 202，在途数准确
  13. 上游挂起 -> 网关请求超时 504，不虚报成功、在途归零
  14. 多并发在途请求：排空须等全部结束，在途计数全程准确
  15. 未知节点管理接口返回 404
  16. 会话开立与绑定查询（凭据不可伪造、只进原实例）
  17. 普通 GET 仍轮询，绑定 GET 固定节点（互不影响）
  18. 绑定节点排空 -> 503 保留绑定、绝不改投；恢复后同凭据继续
  19. 绑定节点故障 -> 503 保留绑定；恢复后同凭据继续
  20. 网关重启（上游未重启）-> 旧凭据仍可用
  21. 上游进程重启 -> 旧凭据明确失效 410，不误接同名新实例
  22. 全部不可接流时会话开立 -> 503；新会话只选可接流节点
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

PYLIBS = os.environ.get("PYLIBS", "/tmp/pylibs")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable

GW_PORT = 19080
U1_PORT = 19081
U2_PORT = 19082

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, url: str, timeout: float = 20.0, data: bytes | None = None,
         headers: dict | None = None):
    """返回 (status, headers, body_text)。4xx/5xx 不抛异常。"""
    req = urllib.request.Request(url, method=method, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def jget(url: str, timeout: float = 20.0):
    status, _, body = http("GET", url, timeout)
    return status, json.loads(body)


def mint_local_token(node: str, instance_id: str, sid: str, secret: str) -> str:
    """用与网关相同的算法在测试侧签发凭据（用于构造绑定未知节点等用例）。"""
    payload = {"v": 1, "node": node, "instance": instance_id, "sid": sid,
               "iat": int(time.time())}
    payload_b = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    sig = hmac.new(secret.encode(), payload_b, hashlib.sha256).digest()
    b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
    return f"{b64(payload_b)}.{b64(sig)}"


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = http("GET", url, timeout=2)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def spawn(module_dir: str, port: int, extra_env: dict | None = None):
    # -I 隔离模式会忽略 PYTHONPATH 且不含 cwd，故用 runpy 包装器显式注入依赖与应用目录
    launcher = (
        "import sys,runpy,os;"
        f"sys.path[:0]=[{PYLIBS!r}, os.getcwd()];"
        "sys.argv=['uvicorn']+sys.argv[1:];"
        "runpy.run_module('uvicorn', run_name='__main__')"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(extra_env or {})
    proc = subprocess.Popen(
        [PYTHON, "-I", "-c", launcher, "app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=module_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc


def kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


def node_status(node: str) -> dict:
    status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
    assert status == 200
    return next(n for n in body["nodes"] if n["name"] == node)


def set_mode(port: int, mode: str) -> None:
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/mode?mode={mode}")
    assert status == 200, body


def main() -> int:
    procs = []
    try:
        print("== 启动两个上游和网关 ==")
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
        procs = [u1, u2]
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")

        gw_env = {
            "UPSTREAMS": f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT}",
            "PROBE_INTERVAL": "1",
            "PROBE_TIMEOUT": "1",
            "REQUEST_TIMEOUT": "15",
            "DRAIN_DEFAULT_TIMEOUT": "10",
            # 固定密钥：网关重启场景（场景 20）要求旧凭据在新网关进程仍可验证
            "SESSION_SECRET": "self-test-fixed-secret-0123456789abcdef",
        }
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关启动失败"
        time.sleep(1.2)  # 等首轮探测结果稳定

        # 1. 轮询分发
        print("== 1. 轮询分发 ==")
        seen = []
        for _ in range(4):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            assert status == 200, body
            seen.append(body["node"])
        check("4 个请求在两节点间交替", seen == ["node1", "node2", "node1", "node2"], str(seen))
        st = node_status("node1")
        check("rr_hits 各 2 次", st["rr_hits"] == 2, str(st))
        check("在途计数已归零", st["inflight"] == 0, str(st))

        # 2. 状态 API 内容
        print("== 2. 状态 API ==")
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
        check("两节点均健康", all(n["healthy"] for n in body["nodes"]))
        check("两节点均未排空", all(not n["draining"] for n in body["nodes"]))
        check("eligible 含两节点", sorted(body["eligible_nodes"]) == ["node1", "node2"])

        # 3. 空闲节点排空立即完成
        print("== 3. 空闲节点排空 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        b = json.loads(body)
        check("排空返回 200", status == 200, body)
        check("drained=true", b.get("drained") is True, body)
        check("inflight=0", b.get("inflight") == 0, body)
        st = node_status("node1")
        check("状态显示 draining", st["draining"] is True and st["eligible"] is False)

        # 4. 排空后新请求不进入 node1
        print("== 4. 排空后新请求只去 node2 ==")
        nodes = set()
        for _ in range(3):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            nodes.add(body["node"])
        check("新请求全部落在 node2", nodes == {"node2"}, str(nodes))

        # 5. 在途慢请求 + 排空交错：排空等在途结束，原响应可取回
        print("== 5. 在途慢请求期间排空 node2 ==")
        result = {}

        def slow_call():
            result["status"], _, result["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20
            )

        t = threading.Thread(target=slow_call)
        t.start()
        # 等到请求确实进入 node2
        inflight_seen = 0
        deadline = time.time() + 5
        while time.time() < deadline:
            st2 = node_status("node2")
            inflight_seen = max(inflight_seen, st2["inflight"])
            if st2["inflight"] == 1:
                break
            time.sleep(0.1)
        check("慢请求已进入 node2 且 inflight=1", inflight_seen == 1, str(st2))

        drain_started = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=10"
        )
        drain_elapsed = time.time() - drain_started
        b = json.loads(body)
        t.join(timeout=20)
        check("排空请求被在途请求阻塞约 3s", drain_elapsed >= 2.5, f"{drain_elapsed:.2f}s")
        check("排空最终成功 drained=true", status == 200 and b.get("drained") is True, body)
        check("慢请求原响应成功取回 200", result.get("status") == 200, str(result))
        check("响应来自 node2 且内容完整",
              json.loads(result["body"]).get("node") == "node2"
              and json.loads(result["body"]).get("slept", 0) >= 2.5,
              result.get("body"))
        check("结束后两节点在途均为 0",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # 6. 两节点都排空 -> 503
        print("== 6. 无可用节点返回 503 ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("返回 503", status == 503, body)
        check("503 原因可解释", json.loads(body).get("error") == "no_eligible_upstream", body)

        # 7. 上游 5xx：不虚报成功 + 节点降级 + 在途归零
        print("== 7. 上游故障 (5xx) ==")
        # 先恢复 node1 以便发请求（node2 仍排空）
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 等周期性探测先发现故障
        st = node_status("node1")
        check("探测已将 node1 标记为不健康", st["healthy"] is False, str(st))
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("无健康节点时返回 503 而非虚假 200", status == 503, body)
        # 临时恢复健康标志位才能真正打到 5xx（模拟请求路径上的失败降级）
        set_mode(U1_PORT, "healthy")
        time.sleep(1.3)
        st = node_status("node1")
        check("探测恢复后 node1 健康", st["healthy"] is True, str(st))
        set_mode(U1_PORT, "error")  # 不等待探测，让请求先撞上 5xx
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("5xx 状态如实返回", status == 500, f"status={status} body={body}")
        time.sleep(0.3)
        st = node_status("node1")
        check("请求失败后节点即时降级", st["healthy"] is False, str(st))
        check("失败后在途计数仍归零", st["inflight"] == 0, str(st))

        # 8. 交错核心：探测成功不得解除排空
        print("== 8. 探测成功不解除排空 ==")
        set_mode(U1_PORT, "healthy")
        # node1 当前未排空，先排空，再让周期性探测持续成功
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        assert status == 200
        time.sleep(2.5)  # 跨越至少 2 轮探测，上游为 healthy，探测必然成功
        st = node_status("node1")
        check("多轮探测成功后排空标志仍在", st["draining"] is True, str(st))
        check("探测成功只更新健康位", st["healthy"] is True and st["eligible"] is False, str(st))

        # 9. 交错核心：恢复不绕过故障探测
        print("== 9. 恢复不绕过故障探测 ==")
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 探测确认故障
        st_before = node_status("node1")
        check("恢复前 node1 不健康", st_before["healthy"] is False, str(st_before))
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        b = json.loads(body)
        check("resume 清除排空但节点仍不可接流",
              status == 200 and b["draining"] is False and b["eligible"] is False, body)
        st = node_status("node1")
        check("draining=False 且 healthy=False",
              st["draining"] is False and st["healthy"] is False, str(st))
        # 故障节点上请求不会被分配：node2 仍排空 -> 503
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("故障节点不会被轮询选中 (503)", status == 503)
        # 上游恢复后经探测自动回到可接流
        set_mode(U1_PORT, "healthy")
        time.sleep(1.5)
        st = node_status("node1")
        check("探测恢复后节点重新 eligible", st["eligible"] is True, str(st))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("流量恢复进入 node1", status == 200 and body["node"] == "node1")

        # 10. 仅 GET 转发
        print("== 10. 非 GET 方法被拒 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/proxy/echo", data=b"x")
        check("POST 转发路径返回 405", status == 405, f"status={status}")

        # 11. /docs 可打开
        print("== 11. /docs 与 openapi.json ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/docs")
        check("/docs 返回 200", status == 200 and "swagger" in body.lower())
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/openapi.json")
        paths = body["paths"]
        check("openapi 含管理与转发接口",
              "/admin/nodes/{name}/drain" in paths and "/proxy/{path}" in paths)

        # 12. 排空等待超时：202 + 在途准确
        print("== 12. 排空超时返回 202 ==")
        result12 = {}

        def slow_call12():
            result12["status"], _, result12["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=4", timeout=15
            )

        t12 = threading.Thread(target=slow_call12)
        t12.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b = json.loads(body)
        check("短超时排空返回 202", status == 202, body)
        check("drained=false 且 inflight=1", b.get("drained") is False and b.get("inflight") == 1, body)
        t12.join(timeout=15)
        check("在途慢请求仍拿到完整 200", result12.get("status") == 200, str(result12))
        time.sleep(0.3)
        # 在途归零后再次排空（节点已在排空中）应立即完成
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        b = json.loads(body)
        check("在途归零后排空立即完成", status == 200 and b["drained"] is True, body)

        # 13. 请求超时：重启网关为短超时，上游 hang（探测与业务请求都会挂住）
        print("== 13. 上游挂起 -> 网关 504 超时 ==")
        kill(gw)
        procs.remove(gw)
        short_env = dict(gw_env)
        short_env["REQUEST_TIMEOUT"] = "1"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, short_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        set_mode(U1_PORT, "healthy")
        # 等首轮探测把 node1 标记为健康可接流（网关重启后状态全清）
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 经探测恢复可接流", node_status("node1")["eligible"] is True)
        # 切 hang 后立即发请求：赶在下一轮周期探测摘掉它之前验证网关自身的请求超时
        set_mode(U1_PORT, "hang")
        t0 = time.time()
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
        elapsed = time.time() - t0
        if status == 503:
            # 探测抢先把节点摘掉，则手动恢复后重试一次，确保覆盖请求超时路径
            set_mode(U1_PORT, "healthy")
            deadline = time.time() + 6
            while time.time() < deadline and not node_status("node1")["eligible"]:
                time.sleep(0.2)
            set_mode(U1_PORT, "hang")
            t0 = time.time()
            status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
            elapsed = time.time() - t0
        check("挂起请求在约 1s 内返回 504", status == 504 and elapsed < 4,
              f"status={status} elapsed={elapsed:.1f}s body={body}")
        check("未虚报成功", status != 200)
        time.sleep(0.3)
        st = node_status("node1")
        check("超时后在途计数准确归零", st["inflight"] == 0, str(st))
        check("超时后节点被标记不健康", st["healthy"] is False, str(st))
        set_mode(U1_PORT, "healthy")

        # 14. 多个并发在途请求：排空须等全部结束，在途计数全程准确
        print("== 14. 多并发在途请求排空 ==")
        # 恢复正常请求超时并重启网关（清空场景 13 的短超时与节点状态）
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 可接流", node_status("node1")["eligible"] is True)
        # 先排空 node2，把流量集中到 node1
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        results14 = []

        def slow_call14(idx: int):
            s, _, b = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20)
            results14.append((idx, s, b))

        threads = [threading.Thread(target=slow_call14, args=(i,)) for i in range(4)]
        for th in threads:
            th.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] < 4:
            time.sleep(0.1)
        st = node_status("node1")
        check("4 个在途请求计数准确", st["inflight"] == 4, str(st))
        t0 = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=15"
        )
        elapsed = time.time() - t0
        b = json.loads(body)
        for th in threads:
            th.join(timeout=20)
        check("排空阻塞到全部 4 个在途请求结束", elapsed >= 2.5 and b.get("drained") is True,
              f"elapsed={elapsed:.1f}s body={body}")
        check("4 个慢请求全部取回完整 200",
              len(results14) == 4 and all(s == 200 for _, s, _ in results14),
              str(results14))
        check("全部结束后在途归零", node_status("node1")["inflight"] == 0)

        # 16. 会话开立与绑定查询：不可伪造、只进原实例
        print("== 16. 会话开立与绑定查询 ==")
        # 前置：场景 14 留下了排空状态，先把两节点恢复到可接流
        for n in ("node1", "node2"):
            if not node_status(n)["eligible"]:
                status, _, _ = http(
                    "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{n}/resume")
                assert status == 200
        deadline = time.time() + 10
        while time.time() < deadline and not all(
                node_status(n)["eligible"] for n in ("node1", "node2")):
            time.sleep(0.2)
        check("前置：两节点均可接流",
              all(node_status(n)["eligible"] for n in ("node1", "node2")))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/session/open")
        check("开立会话返回 201", status == 201, body)
        token = body["token"]
        bound_node = body["node"]
        bound_instance = body["instance_id"]
        sid = body["session_id"]
        check("返回 token / 节点 / 实例 / 会话号",
              bool(token) and bound_node in ("node1", "node2") and len(bound_instance) > 8 and len(sid) > 8,
              str(body))
        # 上游侧确实建立了进程内会话
        up_port = U1_PORT if bound_node == "node1" else U2_PORT
        status, cstate = jget(f"http://127.0.0.1:{up_port}/control/state")
        check("上游进程内会话数增加", status == 200 and cstate["sessions"] >= 1, str(cstate))
        check("上游 instance_id 与凭据一致", cstate["instance_id"] == bound_instance, str(cstate))

        hdr = {"X-Session-Token": token}
        status, body = (lambda r: (r[0], json.loads(r[2])))(
            http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
                 headers=hdr))
        check("绑定查询 200 且落在原节点原实例",
              status == 200 and body["node"] == bound_node
              and body["instance_id"] == bound_instance and body["queries"] == 1,
              str(body))
        # 再来一次：查询计数累加，节点/实例不变
        status, body = (lambda r: (r[0], json.loads(r[2])))(
            http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
                 headers=hdr))
        check("第二次绑定查询计数累加且仍固定原节点",
              status == 200 and body["queries"] == 2 and body["node"] == bound_node, str(body))
        # 查询参数方式同样生效
        status, body = jget(
            f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}&session_token={token}")
        check("查询参数携带凭据同样绑定", status == 200 and body["node"] == bound_node, str(body))
        # 伪造/篡改/畸形凭据一律拒绝
        forged = token[:-2] + ("aa" if token[-2:] != "aa" else "bb")
        status, _, body = http(
            "GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo",
            headers={"X-Session-Token": forged})
        check("篡改签名的凭据返回 401", status == 401, body)
        status, _, body = http(
            "GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo",
            headers={"X-Session-Token": "not-a-token"})
        check("畸形凭据返回 401", status == 401, body)
        # 管理面可见会话统计与实例身份
        st = node_status(bound_node)
        check("状态面展示 instance_id 与开立/绑定计数",
              st["instance_id"] == bound_instance and st["session_opened"] >= 1
              and st["session_bound_hits"] >= 3, str(st))

        # 17. 普通 GET 继续轮询；绑定 GET 始终固定，互不影响
        print("== 17. 轮询与绑定互不影响 ==")
        seen = []
        for _ in range(4):
            status, b = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            assert status == 200
            seen.append(b["node"])
        check("普通 GET 仍在两节点间轮询", len(set(seen)) == 2, str(seen))
        bound_seen = set()
        for _ in range(4):
            status, b = (lambda r: (r[0], json.loads(r[2])))(
                http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
                     headers=hdr))
            assert status == 200, b
            bound_seen.add(b["node"])
        check("绑定 GET 始终固定在原节点", bound_seen == {bound_node}, str(bound_seen))

        # 18. 绑定节点排空 -> 503 保留绑定、绝不改投；恢复后同凭据继续
        print("== 18. 排空期间绑定查询 503 且不改投 ==")
        other_node = "node2" if bound_node == "node1" else "node1"
        status, _, _ = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{bound_node}/drain?timeout=5")
        assert status == 200
        status, _, body = http(
            "GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
            headers=hdr)
        b18 = json.loads(body)
        check("绑定节点排空时返回 503", status == 503, body)
        check("503 原因为 node_draining 且保留绑定提示",
              b18.get("error") == "node_draining" and b18.get("node") == bound_node, body)
        check("绝不改投另一节点（响应里没有对端节点信息）", "instance_id" not in b18, body)
        st = node_status(bound_node)
        check("被拒绑定计入 session_rejected", st["session_rejected"] >= 1, str(st))
        # 同节点上的普通轮询也不会选中它；另一节点仍可服务普通流量
        status, b = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("普通流量只去未排空的另一节点", status == 200 and b["node"] == other_node, str(b))
        # 恢复后凭同一凭据继续查询
        status, _, _ = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{bound_node}/resume")
        assert status == 200
        deadline = time.time() + 8
        while time.time() < deadline and not node_status(bound_node)["eligible"]:
            time.sleep(0.2)
        check("恢复后绑定节点重新可接流", node_status(bound_node)["eligible"] is True)
        status, b = (lambda r: (r[0], json.loads(r[2])))(
            http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
                 headers=hdr))
        check("同一凭据恢复后继续查询成功且仍是原实例",
              status == 200 and b["node"] == bound_node
              and b["instance_id"] == bound_instance, str(b))

        # 19. 绑定节点探测故障 -> 503 保留绑定；恢复后同凭据继续
        print("== 19. 故障期间绑定查询 503 且不改投 ==")
        set_mode(up_port, "error")
        deadline = time.time() + 8
        while time.time() < deadline and node_status(bound_node)["healthy"]:
            time.sleep(0.2)
        check("探测已将绑定节点标记不健康", node_status(bound_node)["healthy"] is False)
        status, _, body = http(
            "GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
            headers=hdr)
        b19 = json.loads(body)
        check("故障期间绑定查询返回 503 而非改投",
              status == 503 and b19.get("error") == "node_unhealthy"
              and b19.get("node") == bound_node, body)
        set_mode(up_port, "healthy")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status(bound_node)["eligible"]:
            time.sleep(0.2)
        status, b = (lambda r: (r[0], json.loads(r[2])))(
            http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
                 headers=hdr))
        check("故障恢复后同一凭据继续可用、仍是原实例",
              status == 200 and b["instance_id"] == bound_instance, str(b))

        # 20. 网关重启（上游未重启）：旧凭据仍可使用
        print("== 20. 网关重启后旧凭据仍有效 ==")
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        time.sleep(1.5)  # 等首轮探测完成实例身份发现
        status, b = (lambda r: (r[0], json.loads(r[2])))(
            http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
                 headers=hdr))
        check("网关重启后旧凭据查询成功", status == 200, str(b))
        check("仍路由到原节点原实例（会话未丢）",
              b.get("node") == bound_node and b.get("instance_id") == bound_instance, str(b))
        check("会话查询计数在上游侧持续累加（进程内会话仍在）", b.get("queries", 0) >= 5, str(b))

        # 21. 上游进程重启：旧凭据明确失效 410，不误接同名新实例
        print("== 21. 上游进程重启 -> 旧凭据 410 失效 ==")
        if bound_node == "node1":
            kill(u1)
            procs.remove(u1)
            u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
            procs.insert(0, u1)
            assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        else:
            kill(u2)
            procs.remove(u2)
            u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
            procs.insert(1, u2)
            assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")
        # 等网关探测发现新 instance_id
        deadline = time.time() + 10
        while time.time() < deadline:
            st = node_status(bound_node)
            if st["instance_id"] and st["instance_id"] != bound_instance and st["eligible"]:
                break
            time.sleep(0.2)
        st = node_status(bound_node)
        check("网关发现同名节点的新实例身份",
              st["instance_id"] and st["instance_id"] != bound_instance and st["eligible"],
              str(st))
        status, _, body = http(
            "GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
            headers=hdr)
        b21 = json.loads(body)
        check("旧凭据明确失效返回 410", status == 410, body)
        check("410 同时给出旧/新实例号，证明未误接同名新实例",
              b21.get("error") == "session_bound_instance_gone"
              and b21.get("bound_instance_id") == bound_instance
              and b21.get("current_instance_id") == st["instance_id"], body)
        # 新实例上查旧 sid：即使绕过网关也不存在（进程内存已清空）
        status, body = jget(f"http://127.0.0.1:{up_port}/session/query?sid={sid}")
        check("新进程内旧会话不存在（上游直连 404）", status == 404, str(body))
        # 重新开立会话：新凭据绑定当前可接流节点的真实实例（轮询可能选中任一节点）
        open_status, open_body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/session/open")
        new_open_node = open_body["node"]
        st_new = node_status(new_open_node)
        check("重启后可开立新会话且绑定所选节点的真实实例",
              open_status == 201
              and open_body["instance_id"] == st_new["instance_id"]
              and open_body["instance_id"] != bound_instance, str(open_body))
        new_token = open_body["token"]
        new_sid = open_body["session_id"]
        status, b = (lambda r: (r[0], json.loads(r[2])))(
            http("GET",
                 f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={new_sid}",
                 headers={"X-Session-Token": new_token}))
        check("新凭据在新实例上查询成功", status == 200 and b["queries"] == 1, str(b))
        # 旧凭据不会因为新会话开立而复活
        status, _, body = http(
            "GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/query?sid={sid}",
            headers=hdr)
        check("旧凭据仍然 410", status == 410, body)

        # 22. 无可用节点时会话开立 -> 503；新会话只选可接流节点
        print("== 22. 会话开立只选可接流节点 ==")
        http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/session/open")
        b22 = json.loads(body)
        check("全部排空时开立会话返回 503", status == 503, body)
        check("503 原因可解释", b22.get("error") == "no_eligible_upstream", body)
        # 恢复 node2 后开立，新会话必须落在 node2
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/resume")
        assert status == 200
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node2")["eligible"]:
            time.sleep(0.2)
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/session/open")
        check("部分可接流时新会话只落在可接流节点",
              status == 201 and body["node"] == "node2", str(body))
        http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)

        # 23. 未知节点管理接口返回 404
        print("== 23. 未知节点 404 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/drain")
        check("排空未知节点返回 404", status == 404, body)
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/resume")
        check("恢复未知节点返回 404", status == 404, body)
        # 凭据绑定未知节点名 -> 404（而非轮询转发）
        mystery = mint_local_token("ghost", bound_instance, sid,
                                   "self-test-fixed-secret-0123456789abcdef")
        status, _, body = http(
            "GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo",
            headers={"X-Session-Token": mystery})
        check("绑定未知节点的凭据返回 404", status == 404, body)

        print(f"\n==== 结果: {passed} passed, {failed} failed ====")
        return 1 if failed else 0

    finally:
        for p in procs:
            kill(p)


if __name__ == "__main__":
    sys.exit(main())
