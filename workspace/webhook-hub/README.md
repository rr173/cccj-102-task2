# Whub — 多租户 Webhook 投递中枢

替业务系统向外部合作方**可靠、保序、可轮换密钥**地投递 Webhook。
纯 Python 3 标准库实现（无第三方依赖），SQLite 持久化，一条命令启动完整环境。

## 需求 → 实现对照

| 需求 | 实现方式 |
|---|---|
| 租户为端点轮换签名密钥 | 端点持有一串不可变的密钥版本（`endpoint_versions`），`POST …/rotate` 仅提升 `active_version` 指针；密钥用 HMAC-SHA256，请求头带 `kid` |
| 设置并行上限 | 每端点独立的可热更新令牌信号量 `parallelism`（`PATCH` 立即生效，无需重启） |
| 同一业务对象保留先后关系 | 事件带 `object_key`；端点内单调 `seq`，每个 key 只有**队头**可被领取（队头阻塞） |
| 互不相关对象并发发送 | 不同 `object_key` 互不阻塞，在 `parallelism` 内并发（`db.claim_due`） |
| 超时 / 429 / 临时下线按端点独立退避 | 每端点独立 Runner 与独立 `not_before`；指数退避 + 满抖动，429/503 尊重 `Retry-After`；退避期间让出 HTTP 令牌但不丢消息 |
| 不能拖住其他租户 | Runner、线程、并行度、退避全部端点级隔离；端点永久失败只阻塞本 key，不影响他人 |
| 端点迁移：旧队列旧签名、新事件新签名 | 入队瞬间把 `(sig_version, target_url, kid)` **快照**进 delivery；轮换后旧 delivery 仍用旧版本签名发旧 URL，新入队事件只走新版本 |
| 人工重放不重复确认 | 成功是终态，重复 replay 返回 `409` 幂等拒绝；replay 复用同一 `delivery_id`/`event_id`，接收方按 event_id 去重 |
| 一条命令启动 | `./run.sh`（hub+sink）；容器环境 `docker compose up --build` |

## 一条命令

```bash
./run.sh            # hub :8080 + 模拟接收方 sink :9000，常驻
./run.sh e2e        # 同上，就绪后自动运行 24 项端到端断言后退出
```

容器：

```bash
docker compose up --build     # hub :8080，sink :9000
```

也可单独运行：

```bash
python3 -m whub hub                     # 仅投递中枢
python3 -m whub sink --port 9000        # 仅模拟接收方
python3 -m whub e2e --hub … --sink …    # 对已运行的环境验收
```

预置两个演示租户（API Key 直接可用）：

- Acme：`whk_demo_acme_key`
- Globex：`whk_demo_globex_key`

## 快速体验

```bash
# 1) 注册端点（指向模拟接收方），并行度 2
curl -s -X POST localhost:8080/v1/endpoints \
  -H 'Authorization: Bearer whk_demo_acme_key' -H 'Content-Type: application/json' \
  -d '{"url":"http://127.0.0.1:9000/hook","parallelism":2}'

# 2) 投递事件；同一 object_key 保序，不同 key 并发
curl -s -X POST localhost:8080/v1/endpoints/<EP>/events \
  -H 'Authorization: Bearer whk_demo_acme_key' -H 'Content-Type: application/json' \
  -d '{"object_key":"order-123","idempotency_key":"biz-evt-1","payload":{"a":1}}'

# 3) 轮换签名密钥 / 迁移 URL（已排队事件不受影响）
curl -s -X POST localhost:8080/v1/endpoints/<EP>/rotate \
  -H 'Authorization: Bearer whk_demo_acme_key' -H 'Content-Type: application/json' \
  -d '{"url":"http://127.0.0.1:9000/hook-v2","kid":"key-2"}'

# 4) 查询与人工重放
curl -s -H 'Authorization: Bearer whk_demo_acme_key' \
  localhost:8080/v1/deliveries/<DLV>
curl -s -X POST -H 'Authorization: Bearer whk_demo_acme_key' \
  localhost:8080/v1/deliveries/<DLV>/replay
```

## HTTP 出站签名

每个 Webhook 请求带：

```
X-Whub-Event-Id:     evt_…          # 业务事件 ID（接收方幂等键，重试/重放不变）
X-Whub-Delivery-Id:  dlv_…          # 投递 ID（重放复用，绝不二次确认）
X-Whub-Key-Id:       key-2          # 使用的密钥版本标识
X-Whub-Timestamp:    1789612888
X-Whub-Signature:    t=1789612888,v1=BASE64(HMAC_SHA256(secret,
                        "{timestamp}.{delivery_id}.{raw_body}"))
Content-Type: application/json
```

接收方校验示例见 [`examples/receiver_verify.py`](examples/receiver_verify.py)。
接收方**必须**以 `X-Whub-Event-Id` 幂等：网络超时时中枢会重试，崩溃恢复后也可能重发，
但成功确认过的事件，人工 replay 也会被中枢以 `409` 拒绝。

## 投递语义

- 结果分类：`2xx` 成功；`408/429/5xx`（连接错误、DNS、超时、下线）→ 指数退避重试；
  其余 `4xx` → `dead`（毒消息，等人工处理，不无限重试）。
- `dead` 是同 `object_key` 的队头：人工 `replay` 修复后继续，或 `DELETE` 跳过放行后续。
- 至少一次投递（at-least-once）+ 入口/出口双重幂等键。
- 崩溃恢复：在途投递有 60s 租约；**进程启动时**立即回收上一进程遗留的在途投递，
  退避截止时间持久化，重启后按原节奏继续。

## API 一览

鉴权：`Authorization: Bearer <tenant api_key>`，所有资源按租户隔离（越权返回 404）。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/admin/tenants` | 创建租户，返回 `api_key` |
| POST | `/v1/endpoints` | 创建端点（url, parallelism, secret?, kid?） |
| GET  | `/v1/endpoints` / `/v1/endpoints/{ep}` | 列表 / 详情（含状态计数、各版本排队数） |
| PATCH| `/v1/endpoints/{ep}` | 热更新 `parallelism` / `disabled` |
| POST | `/v1/endpoints/{ep}/events` | 事件入队（object_key, payload, idempotency_key?） |
| POST | `/v1/endpoints/{ep}/rotate` | 轮换密钥 / 迁移 URL（secret?, kid?, url?） |
| GET  | `/v1/endpoints/{ep}/versions` | 密钥版本历史 |
| POST | `/v1/endpoints/{ep}/versions/{v}/retire` | 旧版本排空后废弃（仍有排队则 409） |
| GET  | `/v1/endpoints/{ep}/deliveries` | 投递列表 |
| GET  | `/v1/deliveries/{dlv}` | 投递状态 |
| POST | `/v1/deliveries/{dlv}/replay` | 人工重放（成功/在途 → 409 幂等拒绝） |
| DELETE | `/v1/deliveries/{dlv}` | 放弃（canceled），放行同 key 后续 |
| GET  | `/healthz` `/metrics` | 健康检查 / Prometheus 计数 |

## 模拟接收方（sink）管理接口

- `POST /admin/rules` `{path, mode: ok|delay|ratelimit|down|fail|badsig, delay?, retry_after?}`
- `POST /admin/keys` `{kid, secret}`：登记签名密钥（轮换后把新 kid 也登记）
- `GET /admin/stats`：计数、到达顺序、最大并行度、重复/坏签名统计
- `POST /admin/reset`

## 配置（环境变量）

`WHUB_PORT` `WHUB_DB` `WHUB_HTTP_TIMEOUT` `WHUB_BACKOFF_BASE` `WHUB_BACKOFF_CAP`
`WHUB_MAX_WORKERS` `WHUB_POLL_INTERVAL` `WHUB_DISCOVER_INTERVAL` `WHUB_REAP_INTERVAL`
`WHUB_SEED=0`（关闭演示租户）。

## 代码结构

```
whub/
  config.py      运行配置
  db.py          SQLite：租户/端点/版本/事件/投递 + 保序领取、退避、replay、租约回收
  sender.py      出站 HTTP、HMAC 签名、结果分类、Retry-After、指数退避
  dispatcher.py  每端点独立 Runner（并行令牌+退避）、Supervisor（发现/回收/懒启动）
  api.py         控制面/入口 HTTP API
  sink.py        模拟外部合作方（签名校验、幂等、故障注入、统计）
  e2e.py         24 项端到端验收
  main.py        CLI：hub | sink | e2e
```

> 持久层接口刻意做成薄封装（`db.Store`），从 SQLite 换 Postgres/Redis 队列时，
> 只需替换 `claim_due`/`mark_*`/`reap_*` 等方法，调度与 API 层不变。
