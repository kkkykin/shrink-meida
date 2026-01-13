from __future__ import annotations

from datetime import datetime, timedelta, timezone

from server_test_utils import FakeRemoteInfo, ServerHarness


class TestShrinkMediaServerApi(ServerHarness):
    def test_register_requires_bootstrap_token(self) -> None:
        r = self.client.post("/v1/workers/register", json={"name": "w0", "caps": {}})
        self.assertEqual(r.status_code, 401)

        r = self.client.post(
            "/v1/workers/register",
            headers=self._auth_headers("bad-token"),
            json={"name": "w0", "caps": {}},
        )
        self.assertEqual(r.status_code, 401)

    def test_lease_and_heartbeat(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task()

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)
        tasks = r.json()["tasks"]
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["task_id"], task_id)
        lease0 = tasks[0]["lease_expires_at"]

        r = self.client.post(
            f"/v1/tasks/{task_id}/heartbeat",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id},
        )
        self.assertEqual(r.status_code, 200, r.text)
        lease1 = r.json()["lease_expires_at"]
        self.assertNotEqual(lease0, lease1)

    def test_upload_intent_sets_staging_and_final(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/dir/a.mov", src_rel="dir/a.mov")

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)

        r = self.client.post(
            f"/v1/tasks/{task_id}/upload_intent",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "out_size": 456, "out_ext": ".mp4", "action": "ok"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        staging_path = body["staging_path"]
        self.assertTrue(staging_path.startswith(f"{self.out_root}/.shrink_media_staging/{task_id}/"))
        self.assertTrue(staging_path.endswith("/blob"))
        self.assertEqual(body["upload"]["url"], f"/v1/tasks/{task_id}/upload_proxy")
        self.assertEqual(body["upload"]["method"], "PUT")

        task = self.get_task(task_id)
        self.assertIsNotNone(task)
        self.assertEqual(task.staging_path, staging_path)
        self.assertEqual(task.final_path, f"{self.out_root}/dir/a__mov.mp4")
        self.assertEqual(task.action, "ok")
        self.assertEqual(int(task.out_size), 456)

    def test_upload_intent_rejects_skip(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task()

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)

        r = self.client.post(
            f"/v1/tasks/{task_id}/upload_intent",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "out_size": 0, "out_ext": "", "action": "skip"},
        )
        self.assertEqual(r.status_code, 400)

    def test_complete_is_idempotent(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task()

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)

        r = self.client.post(
            f"/v1/tasks/{task_id}/upload_intent",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "out_size": 9, "out_ext": ".mp4", "action": "ok"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        staging_path = r.json()["staging_path"]

        self.openlist.files[staging_path] = FakeRemoteInfo(path=staging_path, size=9, name="blob")

        payload = {"worker_id": worker_id, "staging_path": staging_path, "action": "ok", "out_size": 9, "metrics": {}}
        r = self.client.post(
            f"/v1/tasks/{task_id}/complete",
            headers=self._auth_headers(worker_token),
            json=payload,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["ok"])

        r = self.client.post(
            f"/v1/tasks/{task_id}/complete",
            headers=self._auth_headers(worker_token),
            json=payload,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["ok"])

    def test_fail_is_idempotent(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task()

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)

        payload = {"worker_id": worker_id, "err": "ffmpeg failed", "retryable": True}
        r = self.client.post(
            f"/v1/tasks/{task_id}/fail",
            headers=self._auth_headers(worker_token),
            json=payload,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["ok"])

        r = self.client.post(
            f"/v1/tasks/{task_id}/fail",
            headers=self._auth_headers(worker_token),
            json=payload,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["ok"])

    def test_lease_expired_task_can_be_reassigned(self) -> None:
        _w1_id, _w1_token = self.register_worker(name="w1")
        w2_id, w2_token = self.register_worker(name="w2")

        expired_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        task_id = self.create_task(status="leased", lease_worker_id=999, lease_expires_at=expired_at)

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(w2_token),
            json={"worker_id": w2_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)
        tasks = r.json()["tasks"]
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["task_id"], task_id)

        task = self.get_task(task_id)
        self.assertIsNotNone(task)
        self.assertEqual(task.status, "leased")
        self.assertEqual(int(task.lease_worker_id), w2_id)

