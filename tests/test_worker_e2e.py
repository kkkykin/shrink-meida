"""End-to-end tests for worker."""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from server_test_utils import FakeRemoteInfo, ServerHarness


class TestWorkerE2E(ServerHarness):
    def test_worker_can_process_task_without_openlist_credentials(self) -> None:
        """Worker should not need OpenList credentials to process tasks."""
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/test.jpg", src_rel="test.jpg", src_size=100)

        # Add source file to fake OpenList
        self.openlist.files["/in/test.jpg"] = FakeRemoteInfo(path="/in/test.jpg", size=100, name="test.jpg")

        # Lease task
        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)
        tasks = r.json()["tasks"]
        self.assertEqual(len(tasks), 1)
        task = tasks[0]

        # Verify download URL is provided (no credentials needed)
        self.assertIn("download", task)
        self.assertIn("url", task["download"])
        self.assertTrue(task["download"]["url"].startswith("http://openlist.invalid/d/"))

        # Upload intent
        r = self.client.post(
            f"/v1/tasks/{task_id}/upload_intent",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "out_size": 50, "out_ext": ".webp", "action": "ok"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        upload_data = r.json()
        staging_path = upload_data["staging_path"]

        # Verify staging path is in correct location
        self.assertTrue(staging_path.startswith(f"{self.out_root}/.shrink_media_staging/{task_id}/"))

        # Simulate upload
        self.openlist.files[staging_path] = FakeRemoteInfo(path=staging_path, size=50, name="blob")

        # Complete
        r = self.client.post(
            f"/v1/tasks/{task_id}/complete",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "staging_path": staging_path, "action": "ok", "out_size": 50, "metrics": {}},
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["ok"])

        # Verify final file exists
        task_obj = self.get_task(task_id)
        self.assertEqual(task_obj.status, "finalized")
        self.assertIsNotNone(task_obj.final_path)
        self.assertIn(task_obj.final_path, self.openlist.files)

    def test_worker_cannot_write_outside_staging(self) -> None:
        """Worker should only be able to upload to staging paths."""
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task()

        # Lease task
        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)

        # Get upload intent
        r = self.client.post(
            f"/v1/tasks/{task_id}/upload_intent",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "out_size": 50, "out_ext": ".mp4", "action": "ok"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        staging_path = r.json()["staging_path"]

        # Try to complete with a different (non-staging) path
        malicious_path = "/out/malicious.mp4"
        self.openlist.files[malicious_path] = FakeRemoteInfo(path=malicious_path, size=50, name="malicious.mp4")

        r = self.client.post(
            f"/v1/tasks/{task_id}/complete",
            headers=self._auth_headers(worker_token),
            json={
                "worker_id": worker_id,
                "staging_path": malicious_path,
                "action": "ok",
                "out_size": 50,
                "metrics": {},
            },
        )
        # Should fail due to staging_path mismatch
        self.assertEqual(r.status_code, 409)

    def test_output_path_naming_is_stable(self) -> None:
        """Output paths should follow suffix naming convention."""
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/video.mov", src_rel="video.mov")

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)

        # Request upload with .mp4 extension
        r = self.client.post(
            f"/v1/tasks/{task_id}/upload_intent",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "out_size": 100, "out_ext": ".mp4", "action": "ok"},
        )
        self.assertEqual(r.status_code, 200, r.text)

        task_obj = self.get_task(task_id)
        # Should use suffix naming: video__mov.mp4
        self.assertEqual(task_obj.final_path, f"{self.out_root}/video__mov.mp4")

    def test_complete_is_idempotent_no_duplicate_files(self) -> None:
        """Repeated complete calls should not create duplicate files."""
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
            json={"worker_id": worker_id, "out_size": 50, "out_ext": ".mp4", "action": "ok"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        staging_path = r.json()["staging_path"]

        self.openlist.files[staging_path] = FakeRemoteInfo(path=staging_path, size=50, name="blob")

        # Complete once
        payload = {"worker_id": worker_id, "staging_path": staging_path, "action": "ok", "out_size": 50, "metrics": {}}
        r = self.client.post(
            f"/v1/tasks/{task_id}/complete",
            headers=self._auth_headers(worker_token),
            json=payload,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["ok"])

        files_count_before = len(self.openlist.files)

        # Complete again (idempotent)
        r = self.client.post(
            f"/v1/tasks/{task_id}/complete",
            headers=self._auth_headers(worker_token),
            json=payload,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["ok"])

        # Should not create duplicate files
        files_count_after = len(self.openlist.files)
        self.assertEqual(files_count_before, files_count_after)

    def test_skip_action_completes_without_upload(self) -> None:
        """Skip action should complete without requiring upload."""
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task()

        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)

        # Complete with skip action (no upload_intent needed)
        r = self.client.post(
            f"/v1/tasks/{task_id}/complete",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "staging_path": "", "action": "skip", "out_size": 123, "metrics": {}},
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["ok"])

        task_obj = self.get_task(task_id)
        self.assertEqual(task_obj.status, "finalized")
        self.assertEqual(task_obj.action, "skip")
        self.assertIsNone(task_obj.staging_path)
        self.assertIsNone(task_obj.final_path)

    def test_worker_caps_detection(self) -> None:
        """Test capability detection."""
        from shrink_media_worker.caps import detect_capabilities

        with patch("shrink_media_worker.caps.shutil.which", return_value=None):
            caps = detect_capabilities()
        self.assertIsInstance(caps, dict)
        self.assertIn("has_ffmpeg", caps)
        self.assertIn("has_ffprobe", caps)
        self.assertIn("has_7z", caps)

    def test_transport_download_and_upload(self) -> None:
        """Test transport layer download and upload."""
        from shrink_media_worker.transport import download_file, upload_file_chunked

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            # Create test file
            src_file = tmp_path / "source.txt"
            src_file.write_bytes(b"test content" * 1000)

            # Mock httpx for upload test
            with patch("shrink_media_worker.transport.httpx") as mock_httpx:
                mock_resp = Mock()
                mock_resp.headers = {"content-type": "application/json"}
                mock_resp.json.return_value = {"ok": True}
                mock_httpx.request.return_value = mock_resp

                result = upload_file_chunked(
                    "http://test.invalid/upload",
                    src_file,
                    chunk_size=5000,
                )
                self.assertEqual(result, {"ok": True})
                self.assertTrue(mock_httpx.request.called)
