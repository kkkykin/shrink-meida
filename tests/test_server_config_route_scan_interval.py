from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from shrink_media_server.config import ServerConfig


class TestServerConfigRouteScanInterval(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._old_cwd = os.getcwd()
        self._old_env = os.environ.copy()
        self._tmp = tempfile.TemporaryDirectory(prefix="shrink_media_server_config_tests_")
        self.workdir = Path(self._tmp.name)
        os.chdir(self.workdir)

        for key in (
            "SERVER_CONFIG_FILE",
            "SERVER_DB_URL",
            "SERVER_HOST",
            "SERVER_PORT",
            "SERVER_SCAN_ON_STARTUP",
            "SERVER_SCAN_INTERVAL_SECONDS",
            "OPENLIST_BASE_URL",
            "OPENLIST_USER",
            "OPENLIST_PASS",
            "OPENLIST_OTP",
            "ROUTES_JSON",
            "WORKER_TOKENS",
            "WORKER_TOKENS_SCOPES_JSON",
        ):
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        try:
            os.chdir(self._old_cwd)
            os.environ.clear()
            os.environ.update(self._old_env)
        finally:
            self._tmp.cleanup()
        super().tearDown()

    def test_route_scan_interval_seconds_override_is_loaded(self) -> None:
        config_path = self.workdir / "server.yaml"
        config_path.write_text(
            "\n".join(
                [
                    "db_url: sqlite:///test.db",
                    "openlist:",
                    "  base_url: http://127.0.0.1:15244",
                    "  user: admin",
                    "  pass: pass",
                    "server:",
                    "  scan_on_startup: false",
                    "  scan_interval_seconds: 300",
                    "routes:",
                    "  - id: r1",
                    "    in_root: /in1",
                    "    out_root: /out1",
                    "    scan_interval_seconds: 60",
                    "  - id: r2",
                    "    in_root: /in2",
                    "    out_root: /out2",
                    "  - id: r3",
                    "    in_root: /in3",
                    "    out_root: /out3",
                    "    scan_interval_seconds: 0",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        config = ServerConfig.load(config_file=config_path)

        self.assertEqual(int(config.scan_interval_seconds), 300)
        self.assertEqual([r.id for r in config.routes], ["r1", "r2", "r3"])
        self.assertEqual(config.routes[0].scan_interval_seconds, 60)
        self.assertIsNone(config.routes[1].scan_interval_seconds)
        self.assertEqual(config.routes[2].scan_interval_seconds, 0)

