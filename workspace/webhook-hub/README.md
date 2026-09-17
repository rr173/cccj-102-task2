# Whub — 多租户 Webhook 投递中枢（含双 worker 高可用）

替业务系统向外部合作方**可靠、保序、可轮换密钥**地投递 Webhook。
纯 Python 3 标准库实现（无第三方依赖），SQLite 持久化。

两种形态：

- **单 worker（默认 entrypoint，保持兼容）**：`python3 -m whub hub`，store 与 worker 同进程；
- **双 worker 高可用**：store、worker-a、worker-b 是**各自独立的 OS process**，
  共用同一个 durable store，以带 TTL / epoch / fence_id 的 **lane lease**
  决定每条 delivery lane（= 端点）的唯一执行者。

---

## 一条命令

```bash
./run.sh                         # 旧版单 worker：hub :8080 + sink :9000
./run.sh e2e                     # 旧版 24 项端到端验收

./run-ha.sh                      # HA 验收：8 个双进程场景，写 report.json
./run-ha.sh 3                    # 只复现场景 3（长暂停 + 陈旧写拒绝）
./run-ha.sh smoke                # 常驻 store+worker-a+worker-b+sink 四个真实进程
TTL=2 ./run-ha.sh                # 缩短租约做更激进的故障切换
```

容器：`docker compose up --build`（默认单 worker hub）。

单独角色运行（每个都是独立 OS 进程，可分布在不同机器上，只要共享同一个
SQLite 文件所在卷/网络盘；生产可替换为 Postgres，仲裁语义不变）：

```bash
python3 -m whub store --port-file run/store.port --debug-admin
WHUB_WORKER_ID=worker-a python3 -m whub worker --worker-id worker-a
WHUB_WORKER_ID=worker-b python3 -m whub worker --worker-id worker-b
python3 -m whub sink --port 9000
python3 -m whub ha --ttl 4       # 单 entrypoint 编排 + 验收
```

预置两个演示租户（API Key 直接可用）：Acme `whk_demo_acme_key`、
Globex `whk_demo_globex_key`。

---

## 高可用 lease / epoch / fence 协议

### 持久化协议（`lane_leases` 每 lane 一行）

| 列 | 含义 |
|---|---|
| `lane_id` | delivery lane（= endpoint_id） |
| `owner_id` | 当前持有 worker；NULL = 公共池 |
| `lease_epoch` | 单调递增的归属代次 |
| `fence_id` | 该代次持有的随机 fencing token |
| `expires_at` | 租约到期时刻（**数据库时钟**） |
| `draining` / `drain_deadline` | drain 模式与收尾期限 |
| `last_handoff_reason` | 最近一次交接原因 |
| `created_at` / `updated_at` | 时间戳 |

另有 `workers`（成员注册/心跳/drain）、`lease_audit`（所有权变更审计链）、
`coord_state`（再均衡协调器的 DB 选举租约）。

### epoch 状态机

```
                    acquire(epoch+1, 新 fence)
        ┌──────────────────────────────────────┐
        │                                       ▼
     公共池(owner=NULL) ─────────────────────►  worker 持有(epoch=e, fence=f)
        ▲                                        │  │  │  │
        │ release/drain_release/                │  │  │  └─ handoff/rebalance(epoch+1)
        │ worker_restart/orphan_reclaim         │  │  │      old→new 直接换主，新 fence
        │ (都 epoch+1)                          │  │  └──── drain deadline → drain_handoff(epoch+1)
        └──────────────────────────────────────┘  └─ expiry steal：expires_at < db_now
                                                    （SIGKILL/长暂停，epoch+1，同事务回收 inflight）
```

- **首次 acquire 与 expiry steal 都递增 epoch**；每次换主都发放全新随机 `fence_id`。
- **renew** 只延长 `expires_at`，谓词必须同时匹配 `owner_id + lease_epoch +
  fence_id` **且租约未过期**；epoch 不变。
- release / claim / success / retry / dead / release_delivery 等**所有
  worker 侧状态变更都携带同一 fence 三元组条件**，任何不匹配由数据库判定
  `stale_epoch` 并拒绝写入。
- stop-the-world（`SIGSTOP`）超过 TTL 的 worker 恢复后，其旧 fence 的
  renew / claim / complete 三类写全部失配——**不能续期、不能取件、不能
  覆盖新 owner 已写入的结果**。
- failover（steal/handoff/restart）在**同一个数据库事务**里把旧 owner 的
  inflight 退回 pending，并**原样保留**该投递的 `object_key / seq /
  not_before / target_url / sig_version / kid`：继任者沿用序号、退避水位、
  目标地址与签名代次。

### 时间边界（不信任本地墙上时钟）

所有期限只用数据库时钟 `julianday('now')*86400`（UNIX 秒，亚毫秒）；
过期判定、`not_before`、`drain_deadline` 全部在 SQL 一侧完成。worker 被
冻结、漂移都无法用本地时钟续命。

- 生产默认 `WHUB_LEASE_TTL=30s`，约每 **ttl/5** 续期一次，抖动
  `WHUB_RENEW_JITTER=±20%`；
- 验收用 `--ttl 4`（可低至 2s）缩短故障切换窗口，但状态机完全相同；
- expiry steal 的宽限：带在途孤儿的 lane 过期 `0.5*ttl` 后可偷，纯空闲
  lane 需 `1.5*ttl`，避免重负载下健康 owner 的单次续期调度抖动被误判。

### 不允许的仲裁方式

协议**不使用**线程锁、PID 文件或单进程内存表做跨进程仲裁。进程内唯一的
锁只是 sqlite3 连接的线程安全约束；权威归属永远是 `lane_leases` 行，
worker 本地不保存所有权真值，每拍以数据库行 reconcile。

---

## drain 与渐进再均衡

- `POST /admin/workers/{id}/drain {"deadline": 30}`：进入 drain，
  **停止接受新 lane**，手头工作在 deadline 前收尾；已空 lane 以
  `drain_release`（epoch+1）提前交还；到点仍有在途工作的 lane，lease 继续
  续住直到对端以 **`drain_handoff`（epoch+1）** 强制接手，expiry 部分
  交还公共池。drain 期间服务窗口持续有成功 receipt。
- 成员加入/移除时，由经 `coord_state` **DB 选举**出的单一协调器 leader
  做渐进再均衡：**单轮最多搬动 `WHUB_REBALANCE_MAX_MOVES`（默认 1）条
  lane**，避免所有权一齐翻转。`POST /admin/rebalance {"max_moves":1}`
  手动触发。
- 任何单条 lane 的故障（429/timeout/down/dead）只阻塞本端点/本 object_key，
  绝不形成全局闸门；其余 lane 的吞吐继续增长。

---

## 控制面（集群运维视图）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/admin/workers` | worker 列表：alive/draining/drain_deadline/owned/active |
| GET | `/admin/leases` | lane lease 列表：owner_id/epoch/fence/expires_at/draining/reason |
| GET | `/admin/audit?lane=` | 所有权变更审计链（lane, old/new owner, old/new epoch, reason） |
| POST | `/admin/workers/{id}/drain` / `/undrain` | drain 控制 |
| POST | `/admin/rebalance` | 触发一轮受 cap 约束的再均衡 |
| POST | `/admin/lanes/{ep}/assign` | 控制面强制指派（epoch+1） |
| POST | `/admin/outage {"on":bool}` | 注入/解除 durable store 写故障 |
| POST | `/admin/debug/{renew,claim,complete}` | 旧 owner 陈旧写注入（需 `WHUB_DEBUG_ADMIN=1`） |
| GET | `/admin/metrics` / `/metrics` | 指标 JSON / Prometheus |
| GET | `/admin/coord` | 当前再均衡协调器 leader |

业务接口（端点/事件/轮换/投递/重放）与旧版一致，见下文「API 一览」；
端点与投递响应里都带 `lease: {owner_id, lease_epoch, expires_at, draining,
last_handoff_reason}`。

### 稳定 JSON 错误码

`stale_epoch`（409，fence/epoch 不匹配或已过期）、`lease_not_owned`（409）、
`store_unavailable`（503，durable store 拒绝写入）、`worker_not_found`（404）。

### 指标

`whub_lease_ops{worker,op=…}` 区分 `acquire / renew / steal /
stale_write_rejected / drain_handoff / orphan_recovered / rebalance_handoff`；
`/admin/workers` 按 worker 汇总 `owned / active` 数量。

### 所有权变更日志

一次变更一条结构化记录，字段足以从单条记录还原因果链：

```
OWNERSHIP lane=ep_… old_owner=worker-a new_owner=worker-b \
          old_epoch=3 new_epoch=4 reason=expiry_steal requeued=2
```

---

## 自动验收（真实双进程 + 故障注入）

`python3 -m whub ha`（或 `./run-ha.sh`）为每个场景启动**全新数据库与全新
进程组**（store / worker-a / worker-b / 故障注入 sink），退出清理全部子进程。
等待全部是确定性条件轮询，不靠固定长 sleep 猜结果；每个断言都**同时检查
数据库行（lease/delivery）与 receiver 观测（receipts）**，只看日志不算通过。

| # | 场景 | 注入 |
|---|---|---|
| 1 | 并行上线唯一 owner、lane 分散、无二次 ack | — |
| 2 | 处理连续 job 时 owner 被 `kill -9`，TTL 后接手，序号单调、已 ack 不重做、全部收敛 | SIGKILL |
| 3 | 超过 TTL 的 stop-the-world 后恢复，renew/claim/complete 三类陈旧写全被拒，新 owner 结果不被覆盖 | SIGSTOP/SIGCONT + 陈旧写 |
| 4 | 429/timeout 的未来 `not_before` 随 failover 保留，继任者不提前发送，其他 lane 继续增长 | SIGKILL |
| 5 | v1 批次积压时切 v2 并立刻 failover，积压/新增分别到各自 path，验签代次与快照一致 | rotate + SIGKILL |
| 6 | drain：owned 只减不增、deadline 前收尾、剩余高 epoch 接手、审计链完整、服务不断 | drain |
| 7 | 连续加入/移除/重启 worker + 多次 rebalance，单轮搬运 ≤ cap，无 orphan、无全停窗口 | SIGTERM/SIGKILL/重启 |
| 8 | durable store 拒绝写入时停止一切出站副作用，恢复后重竞争、旧 epoch 不可复活 | store outage marker |

机器可读报告写到 `<data-dir>/report.json`，逐场景列出 **epoch 变化、
receipt 数、最终 owner、最大中断时长、陈旧写拒绝次数**。

---

## 其余投递语义（与单 worker 版一致）

- 同一 `object_key` 严格保序（队头阻塞），不同 key 在 `parallelism` 内并发；
- 结果分类：2xx 成功；408/429/5xx/连接错误 → 指数退避（429/503 尊重
  Retry-After）；其余 4xx → `dead` 等人工处理；
- 入队瞬间快照 `(sig_version, target_url, kid, secret)`，轮换后旧排队事件
  继续旧签名/旧 URL，新事件只走新版本；
- at-least-once + 入口/出口双重幂等键；成功是终态，重复 replay 返回 409。

### HTTP 出站签名

```
X-Whub-Event-Id     evt_…     # 接收方幂等键（重试/重放不变）
X-Whub-Delivery-Id  dlv_…
X-Whub-Key-Id       key-2
X-Whub-Timestamp    1789612888
X-Whub-Signature    t=1789612888,v1=BASE64(HMAC_SHA256(secret,
                      "{timestamp}.{delivery_id}.{raw_body}"))
```

接收方示例见 [`examples/receiver_verify.py`](examples/receiver_verify.py)。

## API 一览（租户面）

鉴权 `Authorization: Bearer <tenant api_key>`，按租户隔离（越权 404）。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/admin/tenants` | 创建租户，返回 api_key |
| POST | `/v1/endpoints` | 创建端点（url, parallelism, secret?, kid?） |
| GET  | `/v1/endpoints` / `/{ep}` | 列表 / 详情（含 lease、计数、版本排队数） |
| PATCH| `/v1/endpoints/{ep}` | 热更新 parallelism / disabled |
| POST | `/v1/endpoints/{ep}/events` | 事件入队（object_key, payload, idempotency_key?） |
| POST | `/v1/endpoints/{ep}/rotate` | 轮换密钥 / 迁移 URL |
| GET  | `/v1/endpoints/{ep}/versions` | 密钥版本历史 |
| POST | `/v1/endpoints/{ep}/versions/{v}/retire` | 排空后废弃旧版本 |
| GET  | `/v1/endpoints/{ep}/deliveries` | 投递列表 |
| GET  | `/v1/deliveries/{dlv}` | 投递状态（含 lease） |
| POST | `/v1/deliveries/{dlv}/replay` | 人工重放（成功/在途 → 409） |
| DELETE | `/v1/deliveries/{dlv}` | 放弃（canceled），放行同 key 后续 |
| GET  | `/healthz` `/metrics` | 健康检查 / Prometheus 指标 |

### sink（故障注入接收方）管理接口

- `POST /admin/rules` `{path, mode: ok|delay|ratelimit|down|fail|badsig, delay?, retry_after?}`
- `POST /admin/keys` `{kid, secret}`；`GET /admin/stats`；`POST /admin/reset`

## 配置（环境变量）

通用：`WHUB_PORT` `WHUB_DB` `WHUB_HTTP_TIMEOUT` `WHUB_BACKOFF_BASE`
`WHUB_BACKOFF_CAP` `WHUB_MAX_WORKERS` `WHUB_POLL_INTERVAL` `WHUB_SEED=0`。

HA/lease：`WHUB_LEASE_TTL`（默认 30）`WHUB_RENEW_INTERVAL`（默认 ttl/3，
运行时编排取 ttl/5）`WHUB_RENEW_JITTER`（0.2）`WHUB_COORD_INTERVAL`
`WHUB_REBALANCE_MAX_MOVES`（1）`WHUB_DRAIN_DEADLINE` `WHUB_WORKER_STALE_AFTER`
`WHUB_WORKER_ID` `WHUB_OUTAGE_MARKER` `WHUB_DEBUG_ADMIN`。

## 代码结构

```
whub/
  config.py      运行配置（生产默认值 + 可缩短 TTL）
  db.py          SQLite：lane lease/epoch/fence 即时事务、条件 UPDATE、审计、协调器选举
  sender.py      出站 HTTP、HMAC 签名、结果分类、Retry-After、指数退避
  dispatcher.py  跨进程 Worker（lease 管理/reconcile/fence gate）、Coordinator（再均衡）
  api.py         租户面 + 集群面控制面（workers/leases/drain/rebalance/audit/debug）
  cluster.py     HA 验收的真实多 OS 进程编排（SIGKILL/SIGSTOP/outage）
  acceptance.py  8 个双进程验收场景 + 机器可读报告
  ha.py          单 entrypoint：store+a+b+sink+验收
  sink.py        故障注入接收方（签名校验、幂等、统计）
  e2e.py         旧版单进程 24 项端到端验收
  main.py        CLI：hub | store | worker | sink | ha | e2e
```

> 持久层是薄封装（`db.Store`），从 SQLite 换 Postgres 时只需替换
> lease/claim/mark 等条件写，epoch/fence 谓词与调度、API 层不变。
