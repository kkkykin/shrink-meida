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


class MultiRouteHarness(unittest.TestCase):
    bootstrap_token = "bootstrap-token"

    def setUp(self) -> None:
        super().setUp()
        self._old_cwd = os.getcwd()
        self._old_env = os.environ.copy()
        self._tmp = tempfile.TemporaryDirectory(prefix="shrink_media_routes_tests_")
        self.workdir = Path(self._tmp.name)
        os.chdir(self.workdir)

        db_path = self.workdir / "server.sqlite"
        os.environ["SERVER_DB_URL"] = f"sqlite:///{db_path}"
        os.environ["ROUTES_JSON"] = json.dumps(
            [
                {"id": "r1", "in_root": "/in1", "out_root": "/out1", "profile": {}},
                {"id": "r2", "in_root": "/in2", "out_root": "/out2", "profile": {}},
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        os.environ["WORKER_TOKENS"] = self.bootstrap_token
        os.environ["OPENLIST_BASE_URL"] = "http://openlist.invalid"
        os.environ["OPENLIST_USER"] = "user"
        os.environ["OPENLIST_PASS"] = "pass"

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

    def create_task(
        self,
        *,
        route_id: str,
        src_path: str,
        src_rel: str,
        status: str = "queued",
    ) -> str:
        from shrink_media_server.models import Task

        task_id = str(uuid.uuid4())
        session = self.server_api.db.get_session()
        try:
            task = Task(
                id=task_id,
                route_id=route_id,
                src_path=src_path,
                src_rel=src_rel,
                src_size=123,
                src_mtime_ns=1,
                status=status,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            session.add(task)
            session.commit()
            return task_id
        finally:
            session.close()

    def get_task(self, task_id: str):
        from shrink_media_server.models import Task

        session = self.server_api.db.get_session()
        try:
            return session.query(Task).filter(Task.id == task_id).first()
        finally:
            session.close()


class TestRouteIsolation(MultiRouteHarness):
    def test_upload_intent_uses_correct_out_root_per_route(self) -> None:
        worker_id, worker_token = self.register_worker()
        t1 = self.create_task(route_id="r1", src_path="/in1/a.mov", src_rel="a.mov")
        t2 = self.create_task(route_id="r2", src_path="/in2/b.mov", src_rel="b.mov")

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 2},
        )
        self.assertEqual(r.status_code, 200, r.text)
        tasks = r.json()["tasks"]
        self.assertEqual({t["task_id"] for t in tasks}, {t1, t2})

        for t in tasks:
            task_id = t["task_id"]
            r2 = self.client.post(
                f"/v1/tasks/{task_id}/upload_intent",
                headers=self._auth_headers(worker_token),
                json={"worker_id": worker_id, "out_size": 9, "out_ext": ".mp4", "action": "ok"},
            )
            self.assertEqual(r2.status_code, 200, r2.text)
            staging_path = r2.json()["staging_path"]

            task_obj = self.get_task(task_id)
            self.assertIsNotNone(task_obj)
            if t["route_id"] == "r1":
                self.assertTrue(staging_path.startswith(f"/out1/.shrink_media_staging/{task_id}/"))
                self.assertEqual(task_obj.final_path, "/out1/a__mov.mp4")
            elif t["route_id"] == "r2":
                self.assertTrue(staging_path.startswith(f"/out2/.shrink_media_staging/{task_id}/"))
                self.assertEqual(task_obj.final_path, "/out2/b__mov.mp4")
            else:
                raise AssertionError(f"unexpected route_id: {t['route_id']}")

