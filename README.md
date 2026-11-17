# HTTP 入口网关（支持优雅排空 / 健康探测 / 轮询转发）

滚动更新场景下的入口网关：运维人员先排空待停机实例，使新请求不再进入，
同时保证在途长请求完整取回原响应，全部结束后才报告排空完成。

技术栈：**FastAPI + httpx**，由 **Docker Compose** 启动 1 个网关 + 2 个可模拟
慢请求 / 故障的上游。

## 目录结构

```
.
├── docker-compose.yml      # 编排：gateway + upstream1 + upstream2
├── gateway/
│   ├── app.py              # 网关：仅转发 GET、轮询、探测、排空/恢复/状态
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py              # 模拟上游：slow / flaky / 故障模式控制
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    └── self_test.py        # 本地端到端自测（15 组场景，52 项断言）
```

## 一键启动

```bash
docker compose up --build
# 网关:        http://localhost:18080
# 上游 node1:  http://localhost:18081
# 上游 node2:  http://localhost:18082
# 交互文档:    http://localhost:18080/docs
```

网关环境变量（见 `docker-compose.yml`）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `UPSTREAMS` | `node1=http://upstream1:8000,node2=http://upstream2:8000` | 上游列表 `name=url` |
| `PROBE_INTERVAL` | `2.0` | 周期探测间隔（秒） |
| `PROBE_TIMEOUT` | `1.5` | 探测超时（秒） |
| `REQUEST_TIMEOUT` | `15.0` | 转发请求超时（秒） |
| `DRAIN_DEFAULT_TIMEOUT` | `30.0` | 排空 API 默认等待上限（秒） |

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/{path}      # 轮询转发到健康且未排空的节点，透传查询串与响应
                       # 无可用节点 -> 503；上游超时 -> 504；连不上 -> 502
                       # 非 GET 方法 -> 405
```

管理：

```
GET  /admin/status                     # 全部节点健康/排空/在途/计数/最近探测
POST /admin/nodes/{name}/drain?timeout=# 排空：立即停接新流，阻塞到在途清零
                                       #   在途归零 -> 200 drained=true
                                       #   超时仍有在途 -> 202 drained=false
POST /admin/nodes/{name}/resume        # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/probe                      # 手动触发一轮探测
```

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /control/state            # 查看模式与在途数
POST /control/mode?mode=       # healthy | slow | error | hang
GET  /slow?seconds=N           # 慢请求（验证在途不被截断）
GET  /flaky                    # 按模式返回 200 / 500 / 挂起
GET  /                         # 健康端点（error->503, hang->挂起）
```

## 典型演练

```bash
# 1. 发一个慢请求（10 秒），在另一终端立刻排空 node2
curl "http://localhost:18080/proxy/slow?seconds=10" &
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
# 现象：drain 请求阻塞，约 10 秒后才返回 drained=true；慢请求拿到完整 200

# 2. 让 node1 进入故障模式，观察探测自动摘除
curl -X POST "http://localhost:18081/control/mode?mode=error"
sleep 3
curl http://localhost:18080/admin/status      # node1 healthy=false

# 3. 故障未消除时恢复 node1：不会绕过探测，节点仍不可接流
curl -X POST http://localhost:18080/admin/nodes/node1/resume
# eligible=false；上游修好后经周期探测自动恢复接流
```

## 关键设计与并发正确性

- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining` 只由
  人工 drain/resume 写入。因此**探测成功永不解除排空**，两者交错互不串改。
- **恢复不绕过故障探测**：`resume` 先清排空并保守置 `healthy=false`，再做一次
  真实探测；探测通过前该节点不会被轮询选中。
- **轮询 + 原子登记**：在同一把 `asyncio.Condition` 锁内完成选节点与
  `inflight += 1`，保证"选中即可接流、计数即生效"。
- **在途计数可靠**：转发用 `try/finally` 归还计数，成功、5xx、超时、连接错误、
  客户端取消都会准确递减；排空等待在条件变量上，请求一结束即被唤醒。
- **排空语义**：置位后新请求立即绕过该节点；已在途请求不被取消，继续等待并
  透传原响应；只有 `inflight` 归零才返回 `drained=true`。
- **不虚报成功**：5xx 如实透传并即时降级；超时返回 504、连接错误返回 502，
  均标记失败并刷新健康状态，绝不返回虚假 200。
- **无节点可接流**：所有节点都不健康或排空中时，`GET /proxy/*` 返回 503。

## 自测

```bash
# 无需 docker；需本地具备 fastapi/uvicorn/httpx（可 pip install 到独立目录）
python3 tests/self_test.py
```

覆盖 15 组场景、52 项断言，含两个关键交错：
**探测成功不解除排空**、**恢复不绕过故障探测**；以及在途慢请求排空、
多并发在途排空、503/504/502、超时不虚报、在途计数准确、405、/docs 等。
