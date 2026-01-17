from __future__ import annotations

import json
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from server_test_utils import FakeOpenListManager


class TestRouteModes(unittest.TestCase):
    bootstrap_token = "bootstrap-token"

    def setUp(self) -> None:
        super().setUp()
        self._old_cwd = os.getcwd()
        self._old_env = os.environ.copy()
        self._tmp = tempfile.TemporaryDirectory(prefix="shrink_media_route_mode_tests_")
        self.workdir = Path(self._tmp.name)
        os.chdir(self.workdir)

        db_path = self.workdir / "server.sqlite"
        os.environ["SERVER_DB_URL"] = f"sqlite:///{db_path}"
        os.environ["ROUTES_JSON"] = json.dumps(
            [
                {
                    "id": "r_copy",
                    "in_root": "/in",
                    "out_root": "/out",
                    "mode": "copy",
                    "profile": {},
                }
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        os.environ["WORKER_TOKENS"] = self.bootstrap_token
        os.environ["OPENLIST_BASE_URL"] = "http://openlist.invalid"
        os.environ["OPENLIST_USER"] = "user"
        os.environ["OPENLIST_PASS"] = "pass"
        os.environ["SERVER_SCAN_ON_STARTUP"] = "0"
        os.environ["SERVER_SCAN_INTERVAL_SECONDS"] = "0"

        from shrink_media_server import api as server_api

        self.server_api = server_api
        self._real_openlist_cls = server_api.OpenListManager
        server_api.OpenListManager = FakeOpenListManager

        self.app = server_api.init_app()
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        try:
            self.client.close()
        finally:
            try:
                db = getattr(self.server_api, "db", None)
                if db is not None:
                    db.engine.dispose()
            finally:
                self.server_api.OpenListManager = self._real_openlist_cls
                os.chdir(self._old_cwd)
                os.environ.clear()
                os.environ.update(self._old_env)
                self._tmp.cleanup()
        super().tearDown()

    def _auth_headers(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    def register_worker(self, *, name: str = "w1") -> tuple[int, str]:
        r = self.client.post(
            "/v1/workers/register",
            headers=self._auth_headers(self.bootstrap_token),
            json={"name": name, "caps": {"os": "linux"}},
        )
        self.assertEqual(r.status_code, 200, r.text)
        data = r.json()
        return int(data["worker_id"]), str(data["worker_token"])

    def create_task(self) -> str:
        from shrink_media_server.models import Task

        task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        session = self.server_api.db.get_session()
        try:
            session.add(
                Task(
                    id=task_id,
                    route_id="r_copy",
                    src_path="/in/a.mov",
                    src_rel="a.mov",
                    src_size=1,
                    src_mtime_ns=1,
                    status="queued",
                    profile_json="{}",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()
            return task_id
        finally:
            session.close()

    def test_worker_does_not_lease_copy_mode_tasks(self) -> None:
        worker_id, worker_token = self.register_worker()
        _task_id = self.create_task()

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 10},
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["tasks"], [])

