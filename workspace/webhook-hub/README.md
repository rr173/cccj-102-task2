# Whub — 多租户 Webhook 投递中枢（单进程 / 双 worker 高可用）

替业务系统向外部合作方**可靠、保序、可轮换密钥**地投递 Webhook。
纯 Python 3 标准库（无第三方依赖），SQLite 持久化。

支持两种形态：

1. **兼容单进程**：`hub`（内嵌 store + 单 worker），旧入口与行为保持不变；
2. **双 worker 高可用（HA）**：`store` + `worker-a` + `worker-b` 是**三个独立
   OS 进程**，共用一个 durable store，以带 **TTL / epoch / fence_id** 的 lease
   决定每条 delivery lane 的唯一执行者。

跨进程仲裁**只发生在数据库事务里**——没有线程锁、PID 文件或进程内内存表
参与所有权裁决。

---

## 一条命令

```bash
# HA 形态：自起 store+worker-a+worker-b+故障注入 receiver，跑 8 场景验收后退出
./run-ha.sh
TTL=3 REPORT=out.json ./run-ha.sh          # 缩短 TTL、指定报告路径

# HA 常驻演示集群（4 个独立 OS 进程），便于手工 curl 运维视图
./run-ha.sh up

# 兼容的单进程形态（hub:8080 + sink:9000）
./run.sh
./run.sh e2e                               # 旧功能端到端验收
```

等价的直接命令：

```bash
python3 -m whub acceptance                 # 自起全新集群跑 8 场景，产出 JSON 报告
python3 -m whub store                      # 独立 durable store 进程
python3 -m whub worker --worker-id worker-a --host 127.0.0.1 --port 8091
python3 -m whub worker --worker-id worker-b --host 127.0.0.1 --port 8092
python3 -m whub hub                        # 兼容入口：内嵌 store + worker-solo
python3 -m whub sink --port 9000           # 故障注入接收方
```

容器：

```bash
docker compose up store worker-a worker-b sink   # HA 形态
docker compose up hub sink                       # 兼容单进程
```

---

## Lease / epoch / fence 协议

仲裁单位是 **lane**：端点（endpoint）内同一 `object_key` 的一条串行投递通道。
不同 lane 互不阻塞（在端点 `parallelism` 内并发），同一 lane 严格按 `seq` 保序。

持久化协议在 `lanes` 表暴露（等价于题目要求的字段）：

| 字段 | 含义 |
|---|---|
| `lane_id` | lane 主键（`endpoint_id + object_key` 派生） |
| `owner_id` | 当前持有 worker；`NULL` = 公共池，任何成员可 acquire |
| `lease_epoch` | 单调递增的所有权代次 |
| `fence_id` | 本次 owner 存续期的随机令牌（换 owner 即换发） |
| `expires_at` | 租约到期时刻，**以 store 单调时钟计** |
| `draining` | owner 是否处于 drain（冗余镜像，权威值在 `workers` 表） |
| `updated_at` | 最近一次状态变更 |
| `last_handoff_reason` | 最近一次所有权变更原因 |
| `last_seq / not_before / target_url / sig_version / kid` | failover 必须沿用的序号、退避水位与不可变快照 |

### Epoch 状态机

```
                 入队(enqueue)  ──>  lane 行：owner=NULL, epoch=0（公共池）
                                        │
                          acquire（首次，epoch 0→1，发 fence F1）
                                        ▼
                              ┌──────────────────────┐
        renew(owner+epoch+fence 全等、未过期)：只延 expires_at，epoch/fence 不变
                              │   OWNED(owner=W,e=n,f=Fn)  │
                              └──────────────────────┘
                 │  expiry_steal / orphan_recovered     │ drain_handoff
                 │  （租约过期 / owner 进程消失）          │ rebalance / worker_shutdown
                 │  仅当旧行 epoch=n 的 CAS 赢            │ release（回公共池）
                 ▼                                      ▼
          OWNED(owner=W',e=n+1,f=F(n+1))          owner=NULL（epoch 保留）
                 │                                      │ 下一次 acquire
       旧 owner 的 renew/claim/complete              仍 epoch+1
       WHERE owner=W AND epoch=n AND fence=Fn
                 └── 影响 0 行 ⇒ stale_epoch，事务拒绝并计数
```

不变式：

* **首次 acquire 与 expiry steal 都让 epoch+1**；renew 绝不动 epoch/fence；
* **renew 只延长 owner+epoch+fence 全等且未过期的行**；
* `claim / success / retry / dead / release` 都携带 **identical fence 条件**
  （`owner_id=? AND lease_epoch=? AND fence_id=?`），任何一个不匹配即整事务拒绝；
* 暂停过久（超过 TTL）的 worker 即使恢复联网，旧 fence 的 renew / claim /
  complete 三类写**全部**被数据库拒绝，且不能覆盖新 owner 已写入的结果；
* failover 后继任者**沿用** lane 行既有的 `last_seq`（序号单调）、`not_before`
  退避水位、以及每条 job 入队时快照的 `target_url / sig_version / kid`；
* **任何 lane 的故障都不会形成全局闸门**：退避水位是 per-lane（端点级），
  发送是 per-lane 串行，单条 lane 卡死只阻塞它自己的后续 job。

### 时间边界（不信任 worker 本地钟）

所有期限由 **store 进程独占的混合单调时钟**给出（`meta.clock_hi` 高水位 +
墙上时钟取大，持久化且跨重启单调）。worker 的本地墙上时钟**从不参与**仲裁。

| 配置（环境变量） | 生产默认 | 说明 |
|---|---|---|
| `WHUB_LEASE_TTL` | `30s` | lease 存活时长 |
| `WHUB_RENEW_INTERVAL` | `10s` | 续租周期（≈TTL/3，留两次重试余量） |
| `WHUB_RENEW_JITTER` | `0.5s` | 续租抖动预算，避免多 worker 同点竞争 |
| `WHUB_HEARTBEAT_INTERVAL` | `5s` | 仅存活登记，不携带所有权 |
| `WHUB_SWEEP_INTERVAL` | `1s` | 扫描 acquire / steal 周期（验收调 0.15s） |
| `WHUB_ACQUIRE_BUDGET` | `4` | 每轮最多新 acquire 的 lane 数 |
| `WHUB_REBALANCE_BUDGET` | `2` | 渐进 rebalance 单轮最多搬运 lane 数（cap） |
| `WHUB_DRAIN_DEADLINE` | `20s` | drain 给手头工作的收尾期限 |
| `WHUB_BACKOFF_BASE/CAP` | `0.5/30s` | 指数退避基数/上限 |

出站发送前还有一道 fence 探针（`touch`，写事务）：store 拒绝写入（outage）
或 fence 已陈旧时，**绝不产生出站副作用**。

---

## Worker 生命周期与运维视图

| 接口 | 说明 |
|---|---|
| `GET  /admin/workers` | worker 列表：`worker_id, incarnation, draining, drain_until, last_heartbeat, owned, active` |
| `GET  /admin/leases` | lease 列表：`lane_id, owner_id, lease_epoch, fence_id(截断), expires_at, draining, last_handoff_reason, not_before, sig_version, target_url, last_seq` |
| `GET  /admin/ownership-log` | 一次所有权变更一条：`lane, old_owner, new_owner, old_epoch, new_epoch, reason`，可从单条记录还原因果链 |
| `GET  /admin/orphans` | 无主但仍有未完成 job 的 lane（稳态应为空） |
| `GET  /admin/counters` · `/metrics` | 按 worker 的持久化计数与 Prometheus 指标 |
| `POST /admin/workers/{id}/drain` | `{draining:true, deadline:20}` 进入/退出 drain |
| `POST /admin/rebalance` | `{budget:2}` 触发一轮受 cap 约束的渐进搬运 |
| `POST /admin/store-outage` | `{reject_writes:true}` 注入 store 写拒绝（场景 8） |

**稳定 JSON 错误码**：`stale_epoch`（409）、`lease_not_owned`（409）、
`store_unavailable`（503）、`not_found`（404）、`bad_request`（400）。

**指标区分**：`acquire / renew / steal / stale_write_rejected / drain_handoff /
orphan_recovered`，并按 worker 暴露 `whub_owned_lanes`、`whub_active_jobs`、
`whub_orphan_lanes`。

### Drain 语义

`POST /admin/workers/worker-a/drain {"draining":true}`：

* 立即停止接受新 lane（sweep 预算置 0）；
* deadline 之前允许手头已 claim 的工作正常收尾；
* deadline 之后不再续租，过期部分交还公共池由其他成员 steal；空闲 lane 会被
  其他成员立即 `drain_handoff`（更高 epoch）接走；
* 整个过程 owned 数量**只减不增**，服务期间持续有成功 receipt。

### 渐进 rebalance（成员加入）

新成员加入后的短窗口内，各 worker 以 `WHUB_REBALANCE_BUDGET` 为 cap 做均衡：
**单轮最多搬配置数量的 lane，且只搬空闲 lane**（任何在途 leased job 的 lane
绝不参与），避免所有权一齐翻转、避免打断正在进行的 HTTP 发送。

---

## 8 个自动验收场景（真实多进程 + 真实故障注入）

`./run-ha.sh` 对每个场景都使用**全新集群、全新数据库**，退出时清理全部子进程
（SIGKILL 兜底，含进程组）。等待全部是**条件轮询**（查数据库行 + receiver 观测），
不用固定长 sleep 猜结果。

| # | 场景 | 注入 | 关键断言 |
|---|---|---|---|
| 1 | a/b 并行上线 | — | 每 lane 唯一 owner、lane 分散到两进程、receiver 无二次 ack |
| 2 | 处理连续 job 时崩溃 | **`kill -9`** | b 等 TTL 接手；seq 1..N 单调；已落 ack 不重做；在途 job 全收敛 |
| 3 | 超 TTL stop-the-world 后恢复 | **SIGSTOP/SIGCONT** | 旧 fence 的 renew/claim/complete 三类写全被拒；新 owner 状态不变 |
| 4 | 429/timeout 的 future `not_before` | `kill -9` + receiver 429 | 继任者未到水位不提前发出；其余 lane 吞吐继续增长 |
| 5 | v1 快照后切 v2 并 failover | rotate + `kill -9` | 积压批次走旧 path/旧签名代次，新批次走新 path/v2，无坏签名 |
| 6 | drain a | drain + 背景流量 | owned 只减不增；deadline 内收尾、剩余被 b 以更高 epoch 接走；receipt 不断 |
| 7 | 反复加入/移除/重启 | 重启 + `kill -9` + 多次 rebalance | 每轮搬运 ≤ cap；最终无 orphan；处理曲线无全停窗口 |
| 8 | durable store 暂时拒写 | store 写闸门 | worker 停止出站副作用（不靠内存 ownership）；恢复后重新竞争，旧 epoch 不可复活 |

产出机器可读报告 `acceptance-report.json`，每个场景列出：epoch 变化链、
receipt 数、最终 owner、最大中断时长（`max_outage_seconds`）、
陈旧写拒绝次数（`stale_write_rejected`）、每条 check 与详情。

> 任何场景都**同时检查数据库行与 receiver 观测**，仅验证日志文字视为未通过。

worker 进程还提供测试钩子（`WHUB_FREEZE_SUPPORT=1`，生产可关）：
`POST /test/freeze`（进程内 STW）、`POST /test/stale-attempts`
（显式用旧 fence 发起三类陈旧写）。场景 3 实际使用更强的 OS 级 SIGSTOP。

---

## 持久化与发送语义（沿用原有保证）

* 事件入队即生成 lane（若不存在）+ 不可变快照 job（`sig_version,target_url,kid`）
  与端点内单调 `seq`；轮换/迁移后旧 job 永远按旧版本签名发旧 URL，新 job 只走新版本；
* 签名代次随快照：v1 = base64 HMAC over `ts.delivery_id.body`；
  v2 = hex HMAC over `ts.event_id.delivery_id.body`，请求头带
  `X-Whub-Signature-Version`；
* 结果分类：2xx 成功；408/429/5xx/连接错误 → 指数退避（满抖动，尊重 Retry-After）；
  其余 4xx → `dead`（队头阻塞，等人工 replay/skip）；
* 至少一次投递 + 入口（idempotency_key）/出口（event_id）双重幂等；
* 人工 replay 成功终态返回 409，绝不产生二次确认。

业务 API（`/v1/...`）、租户隔离、演示租户 API Key 等与旧版一致，见下。

预置演示租户（`hub` 或 store `WHUB_SEED=1`）：
Acme `whk_demo_acme_key`、Globex `whk_demo_globex_key`。

---

## 代码结构

```
whub/
  config.py      运行配置：TTL/续租/jitter/budget/drain 等时间边界全部可配
  engine.py      共享 durable store 仲裁引擎：全部 lease/job 变更为单事务，
                 fence 在 WHERE 中校验；store 独占单调时钟；写闸门
  client.py      store 客户端：DirectClient(进程内) / HttpStore(跨进程) 同一接口
  api.py         统一 HTTP 面：业务 API + 控制面 + /rpc（store 进程）+ worker 钩子
  worker.py      投递 worker：lease 持有者；出站前必过 touch 写事务
  sender.py      出站 HTTP、v1/v2 HMAC 签名、结果分类、指数退避
  sink.py        故障注入 receiver（签名校验/幂等/429/timeout/down/统计）
  store.py       引擎 re-export 垫片（HTTP 面由 api.py 的 store 形态承载）
  acceptance.py  8 场景真实多进程验收 + 机器可读报告
  e2e.py         旧功能端到端验收
  main.py        CLI：hub | store | worker | sink | acceptance | e2e
  test_core.py   引擎 fence/epoch/序号/快照/写闸门单元测试
run.sh           兼容单进程入口（hub+sink / e2e）
run-ha.sh        HA 入口（acceptance 自起集群，或 up 常驻四进程）
```
