"""命令行入口：python3 -m whub (hub|sink|e2e)"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time

from .config import HubConfig
from .db import Store
from .dispatcher import Supervisor
from .sink import SinkServer, SinkState
from .util import new_id


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
        datefmt="%H:%M:%S")


def run_hub(args) -> int:
    cfg = HubConfig.from_env()
    os.makedirs(os.path.dirname(cfg.db_path) or ".", exist_ok=True)
    store = Store(cfg.db_path)
    if cfg.seed:
        _seed(store)
    sup = Supervisor(store, cfg)
    sup.start()
    from .api import HubServer
    server = HubServer((cfg.host, cfg.port), store, sup, cfg)
    log = logging.getLogger("whub")
    log.info("webhook hub listening on %s:%s (db=%s)", cfg.host, cfg.port,
             cfg.db_path)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        sup.stop()
        server.server_close()
        store.close()
    return 0


def _seed(store: Store) -> None:
    """启动即种入两个演示租户，做到“一条命令直接可用”。"""
    if store.tenant_by_key("whk_demo_acme_key"):
        return
    store.create_tenant("tnt_demo_acme", "Acme（演示租户A）",
                        "whk_demo_acme_key")
    store.create_tenant("tnt_demo_globex", "Globex（演示租户B）",
                        "whk_demo_globex_key")
    logging.getLogger("whub").info(
        "seeded demo tenants: Acme=whk_demo_acme_key Globex=whk_demo_globex_key")


def run_sink(args) -> int:
    state = SinkState()
    if args.register:
        for spec in args.register:
            kid, secret = spec.split(":", 1)
            state.keys[kid] = secret
    server = SinkServer((args.host, args.port), state)
    logging.getLogger("whub.sink").info(
        "sink listening on %s:%s, known keys=%s",
        args.host, args.port, list(state.keys))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="whub")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)

    h = sub.add_parser("hub", help="启动投递中枢")
    h.set_defaults(func=run_hub)

    s = sub.add_parser("sink", help="启动模拟外部接收方")
    s.add_argument("--host", default="0.0.0.0")
    s.add_argument("--port", type=int, default=9000)
    s.add_argument("--register", action="append", default=[],
                   help="注册签名密钥 kid:secret，可多次")
    s.set_defaults(func=run_sink)

    e = sub.add_parser("e2e", help="运行端到端验收场景")
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
