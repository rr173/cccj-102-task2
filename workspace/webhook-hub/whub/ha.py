"""单 entrypoint：一条命令拉起 shared store + worker-a + worker-b +
故障注入 receiver，并运行验收程序。

用法：
    python3 -m whub ha                    # 起全部进程，跑 8 场景，写报告后清理
    python3 -m whub ha --keep             # 验收完成后保留环境（手工排查）
    python3 -m whub ha --scenario 3       # 只跑场景 3（各自独立全新 DB）
    ./run-ha.sh                           # shell 单脚本等价入口

注意：每个验收场景自行创建全新的 Cluster（新 tempdir/新 DB/新进程组），
本 entrypoint 负责参数、报告路径与退出码；--keep 时额外常驻一个演示集群。
"""
from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import time

from .acceptance import run_acceptance

log = logging.getLogger("whub.ha")


class HARunner:
    def __init__(self, args):
        self.args = args

    def run(self) -> int:
        data_dir = self.args.data_dir or tempfile.mkdtemp(prefix="whub-ha-")
        os.makedirs(data_dir, exist_ok=True)
        log.info("HA 验收：data_dir=%s ttl=%ss scenario=%r",
                 data_dir, self.args.ttl, self.args.scenario or "all")
        report = run_acceptance(
            only=self.args.scenario, ttl=self.args.ttl,
            base_root=data_dir, keep=self.args.keep)
        report_path = self.args.report or os.path.join(data_dir, "report.json")
        if os.path.abspath(report_path) != os.path.abspath(
                report["report_path"]):
            with open(report_path, "w") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
        log.info("机器可读报告：%s", report_path)
        failed = report["summary"]["failed"]
        if failed:
            log.error("%d 个场景失败", failed)
            return 1
        log.info("🎉 全部 %d 个 HA 场景通过", report["summary"]["passed"])
        if self.args.keep:
            log.info("--keep：报告与各场景数据库/日志保留在 %s，按 Ctrl-C 退出",
                     data_dir)
            try:
                while True:
                    time.sleep(3600)
            except KeyboardInterrupt:
                pass
        return 0
