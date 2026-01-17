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


class TokenScopeHarness(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._old_cwd = os.getcwd()
        self._old_env = os.environ.copy()
        self._tmp = tempfile.TemporaryDirectory(prefix="shrink_media_token_scopes_tests_")
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
        os.environ["WORKER_TOKENS_SCOPES_JSON"] = json.dumps(
            {
                "token-r1-image": {
                    "allow_routes": ["r1"],
                    "allow_kinds": ["image"],
                    "base_url": "http://openlist.lan:15244",
                },
                "token-r2-any": {"allow_routes": ["r2"]},
                "token-all": {},
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
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

    def register_worker(self, *, bootstrap_token: str, name: str = "w1") -> tuple[int, str]:
        r = self.client.post(
            "/v1/workers/register",
            headers=self._auth_headers(bootstrap_token),
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


class TestWorkerTokenScopes(TokenScopeHarness):
    def test_token_base_url_is_used_for_openlist_capabilities(self) -> None:
        w_lan_id, w_lan_token = self.register_worker(bootstrap_token="token-r1-image", name="w-lan")
        w_default_id, w_default_token = self.register_worker(bootstrap_token="token-all", name="w-default")

        self.create_task(route_id="r1", src_path="/in1/a.jpg", src_rel="a.jpg")
        self.create_task(route_id="r1", src_path="/in1/b.jpg", src_rel="b.jpg")

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(w_lan_token),
            json={"worker_id": w_lan_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)
        tasks = r.json()["tasks"]
        self.assertEqual(len(tasks), 1)
        self.assertTrue(tasks[0]["download"]["url"].startswith("http://openlist.lan:15244/"))

        r2 = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(w_default_token),
            json={"worker_id": w_default_id, "n": 1},
        )
        self.assertEqual(r2.status_code, 200, r2.text)
        tasks2 = r2.json()["tasks"]
        self.assertEqual(len(tasks2), 1)
        self.assertTrue(tasks2[0]["download"]["url"].startswith("http://openlist.invalid/"))

    def test_lease_is_limited_by_route_id(self) -> None:
        w1_id, w1_token = self.register_worker(bootstrap_token="token-r1-image", name="w-r1")
        w2_id, w2_token = self.register_worker(bootstrap_token="token-all", name="w-all")

        t1 = self.create_task(route_id="r1", src_path="/in1/a.jpg", src_rel="a.jpg")
        t2 = self.create_task(route_id="r2", src_path="/in2/b.jpg", src_rel="b.jpg")

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(w1_token),
            json={"worker_id": w1_id, "n": 10},
        )
        self.assertEqual(r.status_code, 200, r.text)
        tasks = r.json()["tasks"]
        self.assertEqual([t["task_id"] for t in tasks], [t1])
        self.assertEqual(tasks[0]["route_id"], "r1")

        t2_obj = self.get_task(t2)
        self.assertIsNotNone(t2_obj)
        self.assertEqual(t2_obj.status, "queued")

        r2 = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(w2_token),
            json={"worker_id": w2_id, "n": 10},
        )
        self.assertEqual(r2.status_code, 200, r2.text)
        tasks2 = r2.json()["tasks"]
        self.assertEqual([t["task_id"] for t in tasks2], [t2])

    def test_lease_is_limited_by_kind(self) -> None:
        w_img_id, w_img_token = self.register_worker(bootstrap_token="token-r1-image", name="w-img")
        w2_id, w2_token = self.register_worker(bootstrap_token="token-all", name="w-all")

        t_img = self.create_task(route_id="r1", src_path="/in1/a.jpg", src_rel="a.jpg")
        t_vid = self.create_task(route_id="r1", src_path="/in1/b.mov", src_rel="b.mov")

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(w_img_token),
            json={"worker_id": w_img_id, "n": 10},
        )
        self.assertEqual(r.status_code, 200, r.text)
        tasks = r.json()["tasks"]
        self.assertEqual([t["task_id"] for t in tasks], [t_img])

        t_vid_obj = self.get_task(t_vid)
        self.assertIsNotNone(t_vid_obj)
        self.assertEqual(t_vid_obj.status, "queued")

        r2 = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(w2_token),
            json={"worker_id": w2_id, "n": 10},
        )
        self.assertEqual(r2.status_code, 200, r2.text)
        tasks2 = r2.json()["tasks"]
        self.assertEqual([t["task_id"] for t in tasks2], [t_vid])
