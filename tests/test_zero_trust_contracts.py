from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from server_test_utils import FakeRemoteInfo, ServerHarness

class TestZeroTrustContracts(ServerHarness):
    def test_worker_cannot_upload_outside_staging(self) -> None:
        """
        TODO.md: Worker 只能上传到：
          route.out_root/.shrink_media_staging/<task_id>/<nonce>/blob
        """
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
        self.assertTrue(
            self.server_api._is_task_staging_path(path=staging_path, out_root=self.out_root, task_id=task_id)
        )

        session = self.server_api.db.get_session()
        try:
            task = session.query(self.server_api.Task).filter(self.server_api.Task.id == task_id).first()
            self.assertIsNotNone(task)
            task.staging_path = f"{self.out_root}/not-staging/blob"
            task.final_path = f"{self.out_root}/a__mov.mp4"
            task.action = "ok"
            task.out_size = 9
            session.commit()
        finally:
            session.close()

        r = self.client.post(
            f"/v1/tasks/{task_id}/complete",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "staging_path": f"{self.out_root}/not-staging/blob", "action": "ok", "out_size": 9, "metrics": {}},
        )
        self.assertEqual(r.status_code, 500)
        self.assertIn("Invalid staging_path", r.text)

    def test_finalize_is_idempotent(self) -> None:
        """
        TODO.md: finalize/complete/fail 必须是幂等接口（重复调用不制造脏状态）。
        """
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

    def test_lease_expires_and_can_be_reassigned(self) -> None:
        """
        TODO.md: heartbeat 续租；超时自动回收重派。
        """
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
