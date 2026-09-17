"""命令行入口：python3 -m whub (hub|store|worker|sink|ha|e2e)

角色：
- hub     ：默认单 worker 形态（一个进程内同时拥有共享 store 与 worker），
            与旧版用法完全兼容：``python3 -m whub hub``；
- store   ：只开共享 durable store + 控制面 API（不投递）；
- worker  ：以独立 OS process 加入同一 store，参与 lane lease 竞争；
- ha      ：单条 entrypoint，同机拉起 store + worker-a + worker-b +
            故障注入 receiver + 验收程序；
- sink/e2e：模拟接收方 / 旧版功能验收（保持兼容）。
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time

from .config import HubConfig
from .db import Store
from .util import new_id


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        datefmt="%H:%M:%S")


def _open_store(cfg: HubConfig) -> Store:
    os.makedirs(os.path.dirname(cfg.db_path) or ".", exist_ok=True)
    return Store(cfg.db_path, outage_marker=cfg.outage_marker, role=cfg.role)


def _serve_api(cfg: HubConfig, store: Store, worker=None) -> "HubServer":
    from .api import HubServer
    port = cfg.api_port
    last_err = None
    for candidate in ([port] if port else
                      [0] + list(range(18080, 18280))):
        try:
            server = HubServer((cfg.host, candidate), store, cfg, worker)
            break
        except OSError as e:
            last_err = e
    else:
        raise last_err
    actual = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, name="whub-api",
                         daemon=True)
    t.start()
    cfg.api_port = actual
    if cfg.port_file:
        with open(cfg.port_file, "w") as f:
            f.write(str(actual))
    logging.getLogger("whub").info(
        "%s API listening on %s:%s (db=%s, worker=%s)",
        cfg.role, cfg.host, actual, cfg.db_path,
        worker.worker_id if worker else "-")
    return server


def _seed(store: Store) -> None:
    if store.tenant_by_key("whk_demo_acme_key"):
        return
    store.create_tenant("tnt_demo_acme", "Acme（演示租户A）",
                        "whk_demo_acme_key")
    store.create_tenant("tnt_demo_globex", "Globex（演示租户B）",
                        "whk_demo_globex_key")
    logging.getLogger("whub").info(
        "seeded demo tenants: Acme=whk_demo_acme_key "
        "Globex=whk_demo_globex_key")


def _install_signals(on_term) -> None:
    def _handler(signum, frame):
        on_term()
    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def run_hub(args) -> int:
    """默认单 worker 形态：store + worker 同进程（旧用法保持可用）。"""
    cfg = HubConfig.from_env()
    cfg.role = "hub"
    store = _open_store(cfg)
    if cfg.seed:
        _seed(store)
    from .dispatcher import Worker
    worker = Worker(store, cfg,
                    worker_id=cfg.worker_id or f"worker-{os.getpid()}")
    cfg.worker_id = worker.worker_id
    worker.start()
    server = _serve_api(cfg, store, worker)
    stopping = threading.Event()
    _install_signals(lambda: stopping.set())
    try:
        stopping.wait()
    finally:
        worker.stop()
        server.shutdown()
        server.server_close()
        store.close()
    return 0


def run_store(args) -> int:
    cfg = HubConfig.from_env()
    cfg.role = "store"
    cfg.api_port = args.port or cfg.port
    if args.port_file:
        cfg.port_file = args.port_file
    if args.outage_marker:
        cfg.outage_marker = args.outage_marker
    if args.debug_admin:
        cfg.debug_admin = True
    store = _open_store(cfg)
    if cfg.seed and not args.no_seed:
        _seed(store)
    server = _serve_api(cfg, store, None)
    stopping = threading.Event()
    _install_signals(lambda: stopping.set())
    try:
        stopping.wait()
    finally:
        server.shutdown()
        server.server_close()
        store.close()
    return 0


def run_worker(args) -> int:
    cfg = HubConfig.from_env()
    cfg.role = "worker"
    cfg.api_port = args.api_port
    if args.port_file:
        cfg.port_file = args.port_file
    if args.worker_id:
        cfg.worker_id = args.worker_id
    if args.ttl:
        cfg.lease_ttl = args.ttl
    store = _open_store(cfg)
    from .dispatcher import Worker
    worker = Worker(store, cfg, worker_id=cfg.worker_id or None)
    cfg.worker_id = worker.worker_id
    worker.start()
    server = _serve_api(cfg, store, worker)
    stopping = threading.Event()
    _install_signals(lambda: stopping.set())
    try:
        stopping.wait()
    finally:
        worker.stop()
        server.shutdown()
        server.server_close()
        store.close()
    return 0


def run_sink(args) -> int:
    from .sink import SinkServer, SinkState
    state = SinkState()
    for spec in args.register:
        kid, secret = spec.split(":", 1)
        state.keys[kid] = secret
    server = SinkServer((args.host, args.port), state)
    logging.getLogger("whub.sink").info(
        "sink listening on %s:%s, known keys=%s",
        args.host, args.port, list(state.keys))
    stopping = threading.Event()
    _install_signals(lambda: (stopping.set(), server.shutdown()))
    try:
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        stopping.wait()
    finally:
        server.server_close()
    return 0


def run_ha(args) -> int:
    from .ha import HARunner
    runner = HARunner(args)
    return runner.run()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="whub")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("hub", help="启动投递中枢（默认单 worker 形态）").set_defaults(
        func=run_hub)

    s = sub.add_parser("store", help="仅启动共享 store + 控制面 API")
    s.add_argument("--port", type=int, default=0)
    s.add_argument("--port-file", default="")
    s.add_argument("--outage-marker", default="")
    s.add_argument("--debug-admin", action="store_true")
    s.add_argument("--no-seed", action="store_true")
    s.set_defaults(func=run_store)

    w = sub.add_parser("worker", help="以独立进程加入共享 store")
    w.add_argument("--worker-id", default="")
    w.add_argument("--api-port", type=int, default=0)
    w.add_argument("--port-file", default="")
    w.add_argument("--ttl", type=float, default=0.0)
    w.set_defaults(func=run_worker)

    k = sub.add_parser("sink", help="启动模拟外部接收方")
    k.add_argument("--host", default="0.0.0.0")
    k.add_argument("--port", type=int, default=9000)
    k.add_argument("--register", action="append", default=[])
    k.set_defaults(func=run_sink)

    h = sub.add_parser("ha", help="单 entrypoint：store+worker-a+b+sink+验收")
    h.add_argument("--ttl", type=float, default=3.0,
                   help="验收用 lease TTL（秒）；默认 3s")
    h.add_argument("--data-dir", default="")
    h.add_argument("--keep", action="store_true",
                   help="验收完成后不退出（保留环境便于手工检查）")
    h.add_argument("--scenario", default="",
                   help="只运行单个场景（1..8 / 名字）")
    h.add_argument("--report", default="",
                   help="机器可读报告输出路径（默认 data-dir/report.json）")
    h.set_defaults(func=run_ha)

    e = sub.add_parser("e2e", help="旧版单进程端到端验收")
    e.add_argument("--hub", default=os.environ.get("WHUB_URL",
                                                   "http://127.0.0.1:8080"))
    e.add_argument("--sink", default=os.environ.get("SINK_URL",
                                                    "http://127.0.0.1:9000"))
    e.set_defaults(func=lambda a: _run_e2e(a.hub, a.sink))

    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    return args.func(args)


def _run_e2e(hub_url: str, sink_url: str) -> int:
    from .e2e import E2E
    scenario = E2E(hub_url, sink_url)
    try:
        return scenario.run()
    except Exception as ex:
        logging.getLogger("whub.e2e").exception("e2e crashed: %s", ex)
        return 2


if __name__ == "__main__":
    sys.exit(main())
