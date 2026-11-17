# HTTP 入口网关（轮询转发 / 健康探测 / 优雅排空 / 可选会话绑定）

滚动更新场景下的入口网关：运维人员先排空待停机实例，使新请求不再进入，
同时保证在途长请求完整取回原响应，全部结束后才报告排空完成。

当上游把会话保存在**实例进程内存**中时，普通轮询会让客户端的连续查询打散到
不同实例而丢会话。网关额外提供**可选的会话绑定 GET 流程**：客户端开立会话并
取得不可伪造的绑定凭据，之后带凭据的查询固定进入原节点的原进程；普通 GET
仍按轮询分发，两条路径互不影响。

技术栈：**FastAPI + httpx**，由 **Docker Compose** 启动 1 个网关 + 2 个可模拟
慢请求 / 故障、并在进程内存中维护会话的上游。

## 目录结构

```
.
├── docker-compose.yml      # 编排：gateway + upstream1 + upstream2
├── gateway/
│   ├── app.py              # 网关：轮询、探测、排空/恢复、HMAC 会话绑定
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py              # 模拟上游：进程内会话、instance_id、slow/flaky/故障模式
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    └── self_test.py        # 本地端到端自测（23 组场景，89 项断言）
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
| `SESSION_SECRET` | 未设置时随机生成 | 会话凭据 HMAC 密钥；**compose 固定注入**，网关滚动重启后旧凭据仍可验证 |

## 会话绑定流程（全 GET）

```bash
# 1) 开立会话：网关只在「健康且未排空」的节点上建会话，返回绑定凭据
curl http://localhost:18080/proxy/session/open
# 201
# {
#   "token": "<HMAC 绑定凭据>",
#   "node": "node1",
#   "instance_id": "上游进程实例号（重启即变）",
#   "session_id": "上游进程内会话号",
#   "usage": "后续查询携带头 X-Session-Token: <token>，或查询参数 ?session_token=<token>"
# }

# 2) 带凭据查询：固定路由到原节点原进程（头优先，也支持查询参数）
curl -H "X-Session-Token: <token>" \
  "http://localhost:18080/proxy/session/query?sid=<session_id>"
# 等价：curl "http://localhost:18080/proxy/session/query?sid=<sid>&session_token=<token>"

# 3) 不带凭据的普通 GET 继续轮询，行为与以前完全一致
curl http://localhost:18080/proxy/echo
```

### 绑定语义与失效矩阵

| 情形 | 行为 |
| --- | --- |
| 凭据签名错误 / 畸形 | `401 invalid_session_token`，不转发 |
| 凭据绑定的节点名不在配置中 | `404 bound_node_unknown`，不退化为轮询 |
| 绑定节点**排空**或**探测故障** | `503 node_draining` / `503 node_unhealthy`，**保留绑定、绝不改投**；恢复后凭同一凭据继续 |
| 网关重启、上游未重启（compose 固定 `SESSION_SECRET`） | 凭据无状态可重新验证，会话仍在上游进程内存中，**继续可用** |
| 上游**进程重启**（同名新实例，`instance_id` 变化） | `410 session_bound_instance_gone`，响应同时给出旧/新实例号；需重新 `/proxy/session/open`，**不会误接同名新实例** |
| 所有节点都不可接流时开立会话 | `503 no_eligible_upstream`；新会话只会选可接流节点 |

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/session/open      # 在可接流节点开立进程内会话，返回 HMAC 绑定凭据
GET /proxy/{path}            # 无凭据：轮询；带 X-Session-Token/session_token：绑定转发
                             # 无可用节点 -> 503；上游超时 -> 504；连不上 -> 502
                             # 非 GET 方法 -> 405
```

管理：

```
GET  /admin/status                     # 节点健康/排空/在途/计数/最近探测/实例身份/会话统计
POST /admin/nodes/{name}/drain?timeout=# 排空：立即停接新流（含绑定请求），阻塞到在途清零
                                       #   在途归零 -> 200 drained=true
                                       #   超时仍有在途 -> 202 drained=false
POST /admin/nodes/{name}/resume        # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/probe                      # 手动触发一轮探测
```

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /instance                  # 进程身份：instance_id / uptime / 会话数（不受故障模式影响）
GET  /session/open              # 进程内开立会话（201）
GET  /session/query?sid=...     # 查询进程内会话；不存在 -> 404
GET  /control/state             # 查看模式、在途数、会话数、instance_id
POST /control/mode?mode=        # healthy | slow | error | hang
GET  /slow?seconds=N            # 慢请求（验证在途不被截断）
GET  /flaky                     # 按模式返回 200 / 500 / 挂起
GET  /                          # 健康端点（error->503, hang->挂起）
```

## 典型演练

```bash
# 1. 开立会话并连续查询：响应始终来自同一节点同一实例
TOKEN=$(curl -s http://localhost:18080/proxy/session/open | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
for i in 1 2 3; do curl -s -H "X-Session-Token: $TOKEN" http://localhost:18080/proxy/echo; echo; done

# 2. 排空绑定节点：绑定查询得到 503 且不会被转到另一节点；resume 后同凭据恢复
curl -X POST http://localhost:18080/admin/nodes/node1/drain
curl -H "X-Session-Token: $TOKEN" http://localhost:18080/proxy/echo   # 503 node_draining
curl -X POST http://localhost:18080/admin/nodes/node1/resume

# 3. 重启上游进程（docker compose restart upstream1）：旧凭据返回 410，需重新开立
docker compose restart upstream1
curl -H "X-Session-Token: $TOKEN" http://localhost:18080/proxy/echo   # 410 session_bound_instance_gone

# 4. 重启网关（上游不动）：固定 SESSION_SECRET 下旧凭据继续可用
docker compose restart gateway
curl -H "X-Session-Token: $TOKEN" http://localhost:18080/proxy/echo   # 200，仍是原实例
```

## 关键设计与并发正确性

- **不可伪造的无状态凭据**：令牌为 `base64url(payload).base64url(HMAC-SHA256)`，
  载荷含 `node`、上游 `instance_id`、`sid`、签发时间；签名用常量时间比较。
  凭据只存客户端，网关不维护会话登记表，因此网关重启不影响凭据验证。
- **实例身份而非仅节点名**：上游每个进程启动生成随机 `instance_id`，网关经
  `/instance` 探测缓存。上游重启后身份变化，旧凭据命中 `410` 而非误接同名新实例；
  `/instance` 是控制面端点，故障模式下仍可取回身份。
- **绑定优先、绝不改投**：带凭据的请求只认凭据指定的节点与实例；节点排空/故障
  时返回 503 并保留绑定，恢复后原凭据继续。新会话开立与普通 GET 共用同一条
  「健康且未排空」选取路径，新会话不可能建在不可接流节点上。
- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining` 只由
  人工 drain/resume 写入。因此**探测成功永不解除排空**，两者交错互不串改。
- **恢复不绕过故障探测**：`resume` 先清排空并保守置 `healthy=false`，再做一次
  真实探测；探测通过前该节点不会被轮询或绑定请求选中。
- **轮询 + 原子登记**：在同一把 `asyncio.Condition` 锁内完成选节点与
  `inflight += 1`；绑定请求确定可转发后同样登记在途，排空对两类请求一视同仁。
- **在途计数可靠**：转发用 `try/finally` 归还计数，成功、5xx、超时、连接错误、
  客户端取消都会准确递减；排空等待在条件变量上，请求一结束即被唤醒。
- **不虚报成功**：5xx 如实透传并即时降级；超时返回 504、连接错误返回 502，
  均标记失败并刷新健康状态，绝不返回虚假 200。

## 自测

```bash
# 无需 docker；需本地具备 fastapi/uvicorn/httpx（可 pip install 到独立目录）
python3 tests/self_test.py
```

覆盖 23 组场景、89 项断言。原有 15 组场景（轮询、排空交错、503/504/502、
在途计数、405、/docs 等）全部保留；新增 8 组会话场景：

- 会话开立 / 绑定查询 / 凭据防伪（篡改、畸形 401）
- 轮询与绑定互不影响
- 绑定节点**排空** -> 503 保留绑定、不改投，恢复后同凭据继续
- 绑定节点**探测故障** -> 503 保留绑定，恢复后同凭据继续
- **网关重启**（上游未重启、固定密钥）-> 旧凭据继续可用
- **上游进程重启** -> 旧凭据 410、旧会话直连 404、新凭据绑定新实例
- 无可用节点时开立会话 503；新会话只落可接流节点
- 绑定未知节点名 404；绑定请求参与在途/排空计数
