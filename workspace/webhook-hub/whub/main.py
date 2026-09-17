"""命令行入口：python3 -m whub

  hub        兼容入口：单进程内嵌 store + 单 worker（:8080）
  store      独立 durable store 进程（HA）
  worker     投递 worker 进程（HA，worker-a / worker-b 各一个 OS 进程）
  sink       故障注入接收方
  acceptance 一条命令拉起 store+worker-a+worker-b+sink 并执行 8 场景验收
  e2e        旧的功能端到端（对已运行的 hub+sink）
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from .config import HubConfig


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s[%(threadName)s] %(message)s",
        datefmt="%H:%M:%S")


def _seed_if_needed(store) -> None:
    if store.rpc("tenant_by_key", api_key="whk_demo_acme_key"):
        return
    store.rpc("create_tenant", tid="tnt_demo_acme",
              name="Acme（演示租户A）", api_key="whk_demo_acme_key")
    store.rpc("create_tenant", tid="tnt_demo_globex",
              name="Globex（演示租户B）", api_key="whk_demo_globex_key")
    logging.getLogger("whub").info("seeded demo tenants")


def run_hub(args) -> int:
    """兼容单 worker 入口：store 仲裁内嵌本进程，行为与旧版一致。"""
    from .api import HubServer
    from .client import DirectClient
    from .engine import Engine
    from .worker import Worker

    cfg = HubConfig.from_env()
    os.makedirs(os.path.dirname(cfg.db_path) or ".", exist_ok=True)
    engine = Engine(cfg.db_path)
    store = DirectClient(engine)
    if cfg.seed:
        _seed_if_needed(store)
    worker = Worker(cfg.worker_id or "worker-solo", store, cfg,
                    host=cfg.host, port=cfg.port)
    worker.start()
    server = HubServer((cfg.host, cfg.port), store, cfg, worker=worker,
                       engine=engine)
    logging.getLogger("whub").info(
        "hub (single-worker, embedded store) on %s:%s db=%s",
        cfg.host, cfg.port, cfg.db_path)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        worker.stop()
        server.server_close()
        engine.close()
    return 0


def run_store(args) -> int:
    from .api import HubServer
    from .client import DirectClient
    from .engine import Engine

    cfg = HubConfig.from_env()
    if getattr(args, "host", None):
        cfg.host = args.host
    if getattr(args, "port", None):
        cfg.port = args.port
    os.makedirs(os.path.dirname(cfg.db_path) or ".", exist_ok=True)
    engine = Engine(cfg.db_path)
    store = DirectClient(engine)
    if cfg.seed and args.seed:
        _seed_if_needed(store)
    # store 进程：完整业务/控制面 + /rpc 透传，不运行 worker
    server = HubServer((cfg.host, cfg.port), store, cfg, worker=None,
                       engine=engine, enable_rpc=True)
    logging.getLogger("whub").info(
        "durable store on %s:%s db=%s (ttl=%ss renew=%ss budget=%s/%s)",
        cfg.host, cfg.port, cfg.db_path, cfg.lease_ttl, cfg.renew_interval,
        cfg.acquire_budget, cfg.rebalance_budget)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        engine.close()
    return 0


def run_worker(args) -> int:
    from .api import HubServer
    from .client import HttpStore
    from .worker import Worker

    cfg = HubConfig.from_env()
    if getattr(args, "host", None):
        cfg.host = args.host
    if getattr(args, "port", None):
        cfg.port = args.port
    if not cfg.store_url:
        raise SystemExit("worker requires WHUB_STORE_URL")
    wid = args.worker_id or cfg.worker_id or f"worker-{os.getpid()}"
    store = HttpStore(cfg.store_url)
    # 等待 store 就绪（进程可能先于 store 拉起）
    import time as _time
    for _ in range(100):
        try:
            store.health()
            break
        except OSError:
            _time.sleep(0.1)
    worker = Worker(wid, store, cfg, host=cfg.host, port=cfg.port)
    worker.start()
    server = HubServer((cfg.host, cfg.port), store, cfg, worker=worker)
    logging.getLogger("whub").info(
        "worker %s on %s:%s -> store %s (pid=%d)",
        wid, cfg.host, cfg.port, cfg.store_url, os.getpid())
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        worker.stop()
        server.server_close()
    return 0


def run_sink(args) -> int:
    from .sink import SinkServer, SinkState
    state = SinkState()
    for spec in args.register:
        kid, secret = spec.split(":", 1)
        state.keys[kid] = secret
    server = SinkServer((args.host, args.port), state)
    logging.getLogger("whub.sink").info(
        "sink on %s:%s keys=%s (pid=%d)", args.host, args.port,
        list(state.keys), os.getpid())
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
    return 0


def run_acceptance(args) -> int:
    from .acceptance import Acceptance
    acc = Acceptance(ttl=args.ttl, report_path=args.report,
                     keep_logs=args.keep_logs, scenarios=args.scenarios)
    return acc.run()


def run_e2e(args) -> int:
    from .e2e import E2E
    try:
        return E2E(args.hub, args.sink).run()
    except Exception as ex:
        logging.getLogger("whub.e2e").exception("e2e crashed: %s", ex)
        return 2


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="whub")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("hub", help="单进程兼容入口（内嵌 store + 单 worker）").set_defaults(func=run_hub)

    s = sub.add_parser("store", help="独立 durable store 进程")
    s.add_argument("--host", default=None)
    s.add_argument("--port", type=int, default=None)
    s.add_argument("--no-seed", dest="seed", action="store_false")
    s.set_defaults(func=run_store)

    w = sub.add_parser("worker", help="投递 worker 进程")
    w.add_argument("--worker-id", default=None)
    w.add_argument("--host", default=None)
    w.add_argument("--port", type=int, default=None)
    w.set_defaults(func=run_worker)

    k = sub.add_parser("sink", help="故障注入接收方")
    k.add_argument("--host", default="0.0.0.0")
    k.add_argument("--port", type=int, default=9000)
    k.add_argument("--register", action="append", default=[])
    k.set_defaults(func=run_sink)

    a = sub.add_parser("acceptance", help="HA 自动验收（自起集群与故障注入）")
    a.add_argument("--ttl", type=float, default=4.0,
                   help="验收用 lease TTL（秒，确定性轮询而非固定 sleep）")
    a.add_argument("--report", default="acceptance-report.json")
    a.add_argument("--keep-logs", action="store_true")
    a.add_argument("--scenarios", default="",
                   help="只跑指定场景，逗号分隔（1..8）")
    a.set_defaults(func=run_acceptance)

    e = sub.add_parser("e2e", help="旧功能验收（对已运行的 hub+sink）")
    e.add_argument("--hub", default="http://127.0.0.1:8080")
    e.add_argument("--sink", default="http://127.0.0.1:9000")
    e.set_defaults(func=run_e2e)

    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
