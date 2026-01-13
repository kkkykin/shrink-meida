from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from shrink_media_server.config import Route, ServerConfig
from shrink_media_server.models import Database, Task


@dataclass
class _FakeEntry:
    path: str
    is_dir: bool
    size: int = 0
    mtime_ns: int = 0


class ScannerHarness(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory(prefix="shrink_media_scanner_tests_")
        self.workdir = Path(self._tmp.name)
        self.db_path = self.workdir / "server.sqlite"
        self.db = Database(f"sqlite:///{self.db_path}")
        self.db.create_tables()
        self.session = self.db.get_session()

    def tearDown(self) -> None:
        try:
            self.session.close()
        finally:
            self.db.engine.dispose()
            self._tmp.cleanup()
        super().tearDown()


class TestScanner(ScannerHarness):
    def test_scan_route_creates_tasks_and_skips_duplicates(self) -> None:
        from shrink_media_server.scanner import scan_route

        entries = [
            _FakeEntry(path="/in/dir", is_dir=True),
            _FakeEntry(path="/in/dir/a.mov", is_dir=False, size=10, mtime_ns=123),
            _FakeEntry(path="/other/b.mov", is_dir=False, size=99, mtime_ns=456),
        ]

        def fake_iter(_client, root_path: str):  # noqa: ANN001
            self.assertEqual(root_path, "/in")
            return iter(entries)

        openlist = SimpleNamespace(client=object())
        profile = {"image_codec": "webp"}

        with patch("shrink_media_server.scanner.iter_openlist_recursive", side_effect=fake_iter):
            created1, skipped1 = scan_route(
                session=self.session,
                openlist=openlist,  # type: ignore[arg-type]
                route_id="r1",
                in_root="/in",
                profile=profile,
            )
            created2, skipped2 = scan_route(
                session=self.session,
                openlist=openlist,  # type: ignore[arg-type]
                route_id="r1",
                in_root="/in",
                profile=profile,
            )

        self.assertEqual((created1, skipped1), (1, 0))
        self.assertEqual((created2, skipped2), (0, 1))

        tasks = self.session.query(Task).all()
        self.assertEqual(len(tasks), 1)
        t = tasks[0]
        self.assertEqual(t.route_id, "r1")
        self.assertEqual(t.src_path, "/in/dir/a.mov")
        self.assertEqual(t.src_rel, "dir/a.mov")
        self.assertEqual(int(t.src_size), 10)
        self.assertEqual(int(t.src_mtime_ns), 123)
        self.assertEqual(t.status, "queued")
        self.assertEqual(t.profile_json, json.dumps(profile, ensure_ascii=False, separators=(",", ":")))

    def test_scan_all_routes_summarizes_counts(self) -> None:
        from shrink_media_server.scanner import scan_all_routes

        config = ServerConfig(
            db_url="sqlite:///ignored.sqlite",
            openlist_base_url="http://openlist.invalid",
            openlist_user="user",
            openlist_password="pass",
            openlist_otp=None,
            routes=[
                Route(id="r1", in_root="/in1", out_root="/out1", profile={"container": "mp4"}),
                Route(id="r2", in_root="/in2", out_root="/out2", profile={"image_codec": "webp"}),
            ],
            bootstrap_tokens=["bootstrap-token"],
            host="127.0.0.1",
            port=8000,
        )

        def fake_iter(_client, root_path: str):  # noqa: ANN001
            if root_path == "/in1":
                return iter([_FakeEntry(path="/in1/a.mov", is_dir=False, size=1, mtime_ns=1)])
            if root_path == "/in2":
                return iter(
                    [
                        _FakeEntry(path="/in2/b.jpg", is_dir=False, size=2, mtime_ns=2),
                        _FakeEntry(path="/in2/sub/c.png", is_dir=False, size=3, mtime_ns=3),
                    ]
                )
            raise AssertionError(f"unexpected root_path: {root_path}")

        openlist = SimpleNamespace(client=object())

        with (
            patch("shrink_media_server.scanner.iter_openlist_recursive", side_effect=fake_iter),
            patch("builtins.print"),
        ):
            summary = scan_all_routes(config, openlist, self.session)  # type: ignore[arg-type]

        self.assertEqual(summary, {"r1": {"created": 1, "skipped": 0}, "r2": {"created": 2, "skipped": 0}})

        tasks = self.session.query(Task).order_by(Task.src_path).all()
        self.assertEqual(len(tasks), 3)
        got = {(t.route_id, t.src_path, t.src_rel, t.profile_json) for t in tasks}
        self.assertIn(
            ("r1", "/in1/a.mov", "a.mov", json.dumps({"container": "mp4"}, ensure_ascii=False, separators=(",", ":"))),
            got,
        )
        self.assertIn(
            ("r2", "/in2/b.jpg", "b.jpg", json.dumps({"image_codec": "webp"}, ensure_ascii=False, separators=(",", ":"))),
            got,
        )
        self.assertIn(
            ("r2", "/in2/sub/c.png", "sub/c.png", json.dumps({"image_codec": "webp"}, ensure_ascii=False, separators=(",", ":"))),
            got,
        )

