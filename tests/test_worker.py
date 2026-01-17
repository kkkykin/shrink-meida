from __future__ import annotations

import os
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import Mock, patch
from typing import Any
from urllib.parse import urlsplit

import httpx

from server_test_utils import FakeRemoteInfo, ServerHarness


def _url_to_testclient_path(url: str) -> str:
    parts = urlsplit(url)
    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"
    return path


@dataclass
class _TestClientHttpAdapter:
    """
    Minimal adapter so shrink_media_worker.Worker can talk to FastAPI TestClient.
    Exposes the subset of the httpx.Client API used by the worker (get/post/close).
    """

    client: Any

    def get(self, url: str, *, headers: dict[str, str] | None = None):  # noqa: ANN201 - response type depends on client
        return self.client.get(_url_to_testclient_path(url), headers=headers)

    def post(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ):  # noqa: ANN201 - response type depends on client
        return self.client.post(_url_to_testclient_path(url), json=json, headers=headers)

    def close(self) -> None:
        # Let ServerHarness manage lifecycle.
        return None


@dataclass
class _FakeProcessResult:
    ok: bool
    action: str
    msg: str
    out_local: Path | None = None


class TestWorkerRegister(ServerHarness):
    def test_register_with_existing_worker_token_discovers_worker_id(self) -> None:
        worker_id, worker_token = self.register_worker()

        from shrink_media_worker.worker import Worker, WorkerConfig

        config = WorkerConfig(server_url="http://testserver", worker_token=worker_token)
        worker = Worker(config)
        worker.client = _TestClientHttpAdapter(self.client)  # type: ignore[assignment]

        with patch("builtins.print"):
            worker.register()
        self.assertEqual(worker.worker_id, worker_id)
        self.assertEqual(worker.worker_token, worker_token)

    def test_register_invalid_worker_token_falls_back_to_bootstrap(self) -> None:
        from shrink_media_worker.worker import Worker, WorkerConfig

        config = WorkerConfig(
            server_url="http://testserver",
            bootstrap_token=self.bootstrap_token,
            worker_token="bad-token",
        )
        worker = Worker(config)
        worker.client = _TestClientHttpAdapter(self.client)  # type: ignore[assignment]

        with patch("builtins.print"):
            worker.register()
        self.assertIsInstance(worker.worker_id, int)
        self.assertGreaterEqual(int(worker.worker_id or 0), 1)
        self.assertIsInstance(worker.worker_token, str)
        self.assertNotEqual(worker.worker_token, "bad-token")

    def test_register_requires_bootstrap_token_if_no_worker_token(self) -> None:
        from shrink_media_worker.worker import Worker, WorkerConfig

        config = WorkerConfig(server_url="http://testserver")
        worker = Worker(config)
        worker.client = _TestClientHttpAdapter(self.client)  # type: ignore[assignment]

        with self.assertRaises(RuntimeError):
            worker.register()


class TestWorkerProcessFlow(ServerHarness):
    def _make_worker(self, *, worker_id: int, worker_token: str):
        from shrink_media_worker.worker import Worker, WorkerConfig

        config = WorkerConfig(server_url="http://testserver", worker_token=worker_token)
        worker = Worker(config)
        worker.worker_id = worker_id
        worker.client = _TestClientHttpAdapter(self.client)  # type: ignore[assignment]
        return worker

    def _lease_one(self, *, worker_id: int, worker_token: str) -> dict[str, Any]:
        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)
        tasks = r.json()["tasks"]
        self.assertEqual(len(tasks), 1)
        return tasks[0]

    def test_process_task_upload_proxy_includes_auth_and_completes(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/dir/test.jpg", src_rel="dir/test.jpg", src_size=8)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(url: str, dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            self.assertTrue(url.startswith("http://openlist.invalid/d/"))
            self.assertIsNone(headers)
            dest.write_bytes(b"x" * int(task["src_size"]))

        def fake_process_one_local(**kwargs: Any):
            self.assertEqual(kwargs["out_name_mode"], "suffix")
            self.assertEqual(int(kwargs["src_size_hint"]), int(task["src_size"]))

            src_local = kwargs["src_local"]
            in_root = kwargs["in_root"]
            out_root = kwargs["out_root"]
            self.assertTrue(Path(src_local).exists())
            self.assertTrue(Path(src_local).is_relative_to(Path(in_root)))
            self.assertTrue(str(Path(src_local)).endswith("dir/test.jpg"))

            out_local = Path(out_root) / "out.webp"
            out_local.write_bytes(b"y" * 9)
            return _FakeProcessResult(ok=True, action="ok", msg="ok", out_local=out_local)

        def fake_upload_file_chunked(
            url: str,
            src: Path,
            *,
            method: str = "PUT",
            chunk_size: int = 5 * 1024 * 1024,
            headers: dict | None = None,
            timeout: int = 300,
        ) -> dict:
            headers = headers or {}
            self.assertEqual(method.upper(), "PUT")
            self.assertEqual(headers.get("Authorization"), f"Bearer {worker_token}")

            path = _url_to_testclient_path(url)
            resp = self.client.request(method, path, content=src.read_bytes(), headers=headers)
            self.assertEqual(resp.status_code, 200, resp.text)
            return resp.json()

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("shrink_media_worker.worker.process_one_local", side_effect=fake_process_one_local),
            patch("shrink_media_worker.worker.upload_file_chunked", side_effect=fake_upload_file_chunked),
            patch("builtins.print"),
        ):
            worker.process_task(task)

        task_obj = self.get_task(task_id)
        self.assertIsNotNone(task_obj)
        self.assertEqual(task_obj.status, "finalized")
        self.assertEqual(task_obj.final_path, f"{self.out_root}/dir/test__jpg.webp")
        self.assertIn(task_obj.final_path, self.openlist.files)
        self.assertNotIn(str(task_obj.staging_path), self.openlist.files)

    def test_process_task_copy_action_uploads_and_completes(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/a.mov", src_rel="a.mov", src_size=8)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(_url: str, dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            dest.write_bytes(b"x" * int(task["src_size"]))

        def fake_process_one_local(**kwargs: Any):
            out_local = Path(kwargs["out_root"]) / "out.mp4"
            out_local.write_bytes(b"y" * 9)
            return _FakeProcessResult(ok=True, action="copy", msg="fallback copy", out_local=out_local)

        def fake_upload_file_chunked(
            url: str,
            src: Path,
            *,
            method: str = "PUT",
            chunk_size: int = 5 * 1024 * 1024,
            headers: dict | None = None,
            timeout: int = 300,
        ) -> dict:
            headers = headers or {}
            self.assertEqual(method.upper(), "PUT")
            self.assertEqual(headers.get("Authorization"), f"Bearer {worker_token}")

            path = _url_to_testclient_path(url)
            resp = self.client.request(method, path, content=src.read_bytes(), headers=headers)
            self.assertEqual(resp.status_code, 200, resp.text)
            return resp.json()

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("shrink_media_worker.worker.process_one_local", side_effect=fake_process_one_local),
            patch("shrink_media_worker.worker.upload_file_chunked", side_effect=fake_upload_file_chunked),
            patch("builtins.print"),
        ):
            worker.process_task(task)

        task_obj = self.get_task(task_id)
        self.assertIsNotNone(task_obj)
        self.assertEqual(task_obj.status, "finalized")
        self.assertEqual(task_obj.action, "copy")
        self.assertEqual(task_obj.final_path, f"{self.out_root}/a__mov.mp4")
        self.assertIn(task_obj.final_path, self.openlist.files)

    def test_process_task_audio_uploads_and_completes(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/song.mp3", src_rel="song.mp3", src_size=8)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(_url: str, dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            dest.write_bytes(b"x" * int(task["src_size"]))

        def fake_process_one_local(**kwargs: Any):
            out_local = Path(kwargs["out_root"]) / "out.m4a"
            out_local.write_bytes(b"y" * 9)
            return _FakeProcessResult(ok=True, action="ok", msg="ok", out_local=out_local)

        def fake_upload_file_chunked(
            url: str,
            src: Path,
            *,
            method: str = "PUT",
            chunk_size: int = 5 * 1024 * 1024,
            headers: dict | None = None,
            timeout: int = 300,
        ) -> dict:
            headers = headers or {}
            self.assertEqual(method.upper(), "PUT")
            self.assertEqual(headers.get("Authorization"), f"Bearer {worker_token}")

            path = _url_to_testclient_path(url)
            resp = self.client.request(method, path, content=src.read_bytes(), headers=headers)
            self.assertEqual(resp.status_code, 200, resp.text)
            return resp.json()

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("shrink_media_worker.worker.process_one_local", side_effect=fake_process_one_local),
            patch("shrink_media_worker.worker.upload_file_chunked", side_effect=fake_upload_file_chunked),
            patch("builtins.print"),
        ):
            worker.process_task(task)

        task_obj = self.get_task(task_id)
        self.assertIsNotNone(task_obj)
        self.assertEqual(task_obj.status, "finalized")
        self.assertEqual(task_obj.action, "ok")
        self.assertEqual(task_obj.final_path, f"{self.out_root}/song__mp3.m4a")
        self.assertIn(task_obj.final_path, self.openlist.files)

    def test_process_task_comic_uploads_and_completes(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/comic.zip", src_rel="comic.zip", src_size=8)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(_url: str, dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            dest.write_bytes(b"x" * int(task["src_size"]))

        def fake_process_one_local(**kwargs: Any):
            out_local = Path(kwargs["out_root"]) / "out.cbz"
            out_local.write_bytes(b"y" * 9)
            return _FakeProcessResult(ok=True, action="ok", msg="ok", out_local=out_local)

        def fake_upload_file_chunked(
            url: str,
            src: Path,
            *,
            method: str = "PUT",
            chunk_size: int = 5 * 1024 * 1024,
            headers: dict | None = None,
            timeout: int = 300,
        ) -> dict:
            headers = headers or {}
            self.assertEqual(method.upper(), "PUT")
            self.assertEqual(headers.get("Authorization"), f"Bearer {worker_token}")

            path = _url_to_testclient_path(url)
            resp = self.client.request(method, path, content=src.read_bytes(), headers=headers)
            self.assertEqual(resp.status_code, 200, resp.text)
            return resp.json()

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("shrink_media_worker.worker.process_one_local", side_effect=fake_process_one_local),
            patch("shrink_media_worker.worker.upload_file_chunked", side_effect=fake_upload_file_chunked),
            patch("builtins.print"),
        ):
            worker.process_task(task)

        task_obj = self.get_task(task_id)
        self.assertIsNotNone(task_obj)
        self.assertEqual(task_obj.status, "finalized")
        self.assertEqual(task_obj.action, "ok")
        self.assertEqual(task_obj.final_path, f"{self.out_root}/comic__zip.cbz")
        self.assertIn(task_obj.final_path, self.openlist.files)

    def test_process_task_skip_completes_without_upload(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/a.mov", src_rel="a.mov", src_size=11)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(_url: str, dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            dest.write_bytes(b"x" * int(task["src_size"]))

        def fake_process_one_local(**_kwargs: Any):
            return _FakeProcessResult(ok=True, action="dry-run", msg="no savings", out_local=None)

        def should_not_upload(*_args: Any, **_kwargs: Any) -> dict:
            raise AssertionError("upload_file_chunked must not be called for skip action")

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("shrink_media_worker.worker.process_one_local", side_effect=fake_process_one_local),
            patch("shrink_media_worker.worker.upload_file_chunked", side_effect=should_not_upload),
            patch("builtins.print"),
        ):
            worker.process_task(task)

        task_obj = self.get_task(task_id)
        self.assertIsNotNone(task_obj)
        self.assertEqual(task_obj.status, "finalized")
        self.assertEqual(task_obj.action, "skip")
        self.assertIsNone(task_obj.staging_path)
        self.assertIsNone(task_obj.final_path)

    def test_process_task_missing_output_reports_fail(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/a.mov", src_rel="a.mov", src_size=11)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(_url: str, dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            dest.write_bytes(b"x" * int(task["src_size"]))

        def fake_process_one_local(**kwargs: Any):
            out_local = Path(kwargs["out_root"]) / "missing.mp4"
            return _FakeProcessResult(ok=True, action="ok", msg="ok", out_local=out_local)

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("shrink_media_worker.worker.process_one_local", side_effect=fake_process_one_local),
            patch("builtins.print"),
        ):
            worker.process_task(task)

        task_obj = self.get_task(task_id)
        self.assertIsNotNone(task_obj)
        self.assertEqual(task_obj.status, "queued")
        self.assertEqual(task_obj.last_error, "Output file not found")
        self.assertIsNone(task_obj.lease_worker_id)

    def test_process_task_failure_reports_fail(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/a.mov", src_rel="a.mov", src_size=11)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(_url: str, dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            dest.write_bytes(b"x" * int(task["src_size"]))

        def fake_process_one_local(**_kwargs: Any):
            raise RuntimeError("boom")

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("shrink_media_worker.worker.process_one_local", side_effect=fake_process_one_local),
            patch("builtins.print"),
        ):
            worker.process_task(task)

        task_obj = self.get_task(task_id)
        self.assertIsNotNone(task_obj)
        self.assertEqual(task_obj.status, "queued")
        self.assertEqual(task_obj.last_error, "boom")
        self.assertIsNone(task_obj.lease_worker_id)

    def test_process_task_download_unauthorized_is_not_retryable(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/a.mov", src_rel="a.mov", src_size=11)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(url: str, _dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            _ = headers, timeout
            req = httpx.Request("GET", url)
            resp = httpx.Response(401, request=req)
            resp.raise_for_status()

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("builtins.print"),
        ):
            worker.process_task(task)

        task_obj = self.get_task(task_id)
        self.assertIsNotNone(task_obj)
        self.assertEqual(task_obj.status, "deadletter")
        self.assertIn("401", str(task_obj.last_error or ""))

        # Ensure it won't be leased again.
        r = self.client.post(
            "/v1/tasks/lease",
            headers=self._auth_headers(worker_token),
            json={"worker_id": worker_id, "n": 1},
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["tasks"], [])

    def test_process_task_direct_upload_does_not_leak_worker_token(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/a.jpg", src_rel="a.jpg", src_size=3)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        def fake_direct_upload_info(_dst_path: str, _size: int) -> dict:
            return {"url": "http://openlist.invalid/upload", "method": "PUT", "chunk_size": 1024, "headers": {"X-Test": "1"}}

        self.openlist.get_direct_upload_info = fake_direct_upload_info  # type: ignore[assignment]

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(_url: str, dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            dest.write_bytes(b"x" * int(task["src_size"]))

        def fake_process_one_local(**_kwargs: Any):
            out_local = Path(_kwargs["out_root"]) / "out.webp"
            out_local.write_bytes(b"y" * 7)
            return _FakeProcessResult(ok=True, action="ok", msg="ok", out_local=out_local)

        def fake_upload_file_chunked(
            url: str,
            src: Path,
            *,
            method: str = "PUT",
            chunk_size: int = 5 * 1024 * 1024,
            headers: dict | None = None,
            timeout: int = 300,
        ) -> dict:
            headers = headers or {}
            self.assertEqual(url, "http://openlist.invalid/upload")
            self.assertEqual(headers.get("X-Test"), "1")
            self.assertNotIn("Authorization", headers)

            with worker.active_tasks_lock:
                active_task_id = next(iter(worker.active_tasks))
            staging_path = self.get_task(active_task_id).staging_path
            self.assertIsInstance(staging_path, str)
            self.openlist.files[staging_path] = FakeRemoteInfo(path=staging_path, size=src.stat().st_size, name="blob")
            return {"ok": True}

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("shrink_media_worker.worker.process_one_local", side_effect=fake_process_one_local),
            patch("shrink_media_worker.worker.upload_file_chunked", side_effect=fake_upload_file_chunked),
            patch("builtins.print"),
        ):
            worker.process_task(task)

        task_obj = self.get_task(task_id)
        self.assertIsNotNone(task_obj)
        self.assertEqual(task_obj.status, "finalized")
        self.assertEqual(task_obj.final_path, f"{self.out_root}/a__jpg.webp")
        self.assertIn(task_obj.final_path, self.openlist.files)

    def test_process_task_relative_download_uses_auth_header(self) -> None:
        worker_id, worker_token = self.register_worker()
        task_id = self.create_task(src_path="/in/a.mov", src_rel="a.mov", src_size=4)
        task = self._lease_one(worker_id=worker_id, worker_token=worker_token)
        self.assertEqual(task["task_id"], task_id)

        task["download"]["url"] = f"/v1/tasks/{task_id}/download_proxy"

        worker = self._make_worker(worker_id=worker_id, worker_token=worker_token)

        def fake_download(url: str, dest: Path, *, headers: dict | None = None, timeout: int = 300) -> None:
            self.assertEqual(url, f"http://testserver/v1/tasks/{task_id}/download_proxy")
            self.assertEqual(headers, {"Authorization": f"Bearer {worker_token}"})
            dest.write_bytes(b"x" * int(task["src_size"]))

        def fake_process_one_local(**_kwargs: Any):
            return _FakeProcessResult(ok=True, action="dry-run", msg="skip", out_local=None)

        with (
            patch("shrink_media_worker.worker.download_file", side_effect=fake_download),
            patch("shrink_media_worker.worker.process_one_local", side_effect=fake_process_one_local),
            patch("builtins.print"),
        ):
            worker.process_task(task)


class TestWorkerHeartbeat(unittest.TestCase):
    def test_heartbeat_loop_sends_for_active_tasks(self) -> None:
        from shrink_media_worker.worker import Worker, WorkerConfig

        worker = Worker(WorkerConfig(server_url="http://testserver", worker_token="tok"))
        worker.worker_id = 1
        worker.running = True
        with worker.active_tasks_lock:
            worker.active_tasks["t1"] = {}
            worker.active_tasks["t2"] = {}

        seen: list[str] = []

        def fake_heartbeat_task(task_id: str) -> None:
            seen.append(task_id)

        sleep_calls = 0

        def fake_sleep(_seconds: float) -> None:
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls >= 2:
                worker.running = False

        with (
            patch.object(worker, "heartbeat_task", side_effect=fake_heartbeat_task),
            patch("shrink_media_worker.worker.time.sleep", side_effect=fake_sleep),
        ):
            worker.heartbeat_loop()

        self.assertEqual(set(seen), {"t1", "t2"})


class TestWorkerConfig(unittest.TestCase):
    def test_worker_config_from_env(self) -> None:
        from shrink_media_worker.worker import WorkerConfig

        with patch.dict(
            os.environ,
            {
                "WORKER_SERVER_URL": "http://example.invalid/",
                "WORKER_BOOTSTRAP_TOKEN": "boot",
                "WORKER_TOKEN": "tok",
                "WORKER_NAME": "w0",
                "WORKER_LEASE_BATCH_SIZE": "2",
                "WORKER_HEARTBEAT_INTERVAL": "7",
            },
            clear=True,
        ):
            cfg = WorkerConfig.from_env()

        self.assertEqual(cfg.server_url, "http://example.invalid")
        self.assertEqual(cfg.bootstrap_token, "boot")
        self.assertEqual(cfg.worker_token, "tok")
        self.assertEqual(cfg.worker_name, "w0")
        self.assertEqual(cfg.lease_batch_size, 2)
        self.assertEqual(cfg.heartbeat_interval, 7)


class TestWorkerCaps(unittest.TestCase):
    def test_detect_capabilities_parses_encoder_list(self) -> None:
        from shrink_media_worker.caps import detect_capabilities

        def fake_which(cmd: str) -> str | None:
            return f"/bin/{cmd}"

        fake_encoder_text = "\n".join(
            [
                "libx264",
                "libx265",
                "h264_nvenc",
                "hevc_nvenc",
                "libopus",
                "libwebp",
                "libaom-av1",
            ]
        )
        fake_cp = Mock()
        fake_cp.stdout = fake_encoder_text
        fake_cp.stderr = ""

        with (
            patch("shrink_media_worker.caps.shutil.which", side_effect=fake_which),
            patch("shrink_media_worker.caps.subprocess.run", return_value=fake_cp),
        ):
            caps = detect_capabilities()

        self.assertTrue(caps["has_ffmpeg"])
        self.assertTrue(caps["has_ffprobe"])
        self.assertTrue(caps["has_7z"])
        self.assertTrue(caps["has_libx264"])
        self.assertTrue(caps["has_libx265"])
        self.assertTrue(caps["has_h264_nvenc"])
        self.assertTrue(caps["has_hevc_nvenc"])
        self.assertTrue(caps["has_libopus"])
        self.assertTrue(caps["has_libwebp"])
        self.assertTrue(caps["has_libaom_av1"])


class TestWorkerTransport(unittest.TestCase):
    def test_download_file_streams_to_disk(self) -> None:
        from shrink_media_worker.transport import download_file

        body = [b"abc", b"def", b"ghi"]

        class FakeStream:
            def __enter__(self):  # noqa: ANN204
                return self

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN204
                return False

            def raise_for_status(self) -> None:
                return None

            def iter_bytes(self, chunk_size: int):  # noqa: ANN201
                return iter(body)

        with tempfile.TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "out.bin"

            with patch("shrink_media_worker.transport.httpx.stream", return_value=FakeStream()):
                download_file("http://example.invalid/file", dest)

            self.assertEqual(dest.read_bytes(), b"".join(body))

    def test_upload_file_chunked_single_request(self) -> None:
        from shrink_media_worker.transport import upload_file_chunked

        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "a.bin"
            src.write_bytes(b"hello")

            mock_resp = Mock()
            mock_resp.headers = {"content-type": "application/json"}
            mock_resp.json.return_value = {"ok": True}
            mock_resp.raise_for_status.return_value = None

            def fake_request(method: str, url: str, *, content: bytes, headers: dict, timeout: int, follow_redirects: bool):
                self.assertEqual(method, "PUT")
                self.assertEqual(url, "http://example.invalid/upload")
                self.assertEqual(content, b"hello")
                self.assertNotIn("Content-Range", headers)
                return mock_resp

            with patch("shrink_media_worker.transport.httpx.request", side_effect=fake_request):
                out = upload_file_chunked("http://example.invalid/upload", src, chunk_size=10, headers={"X": "1"})

            self.assertEqual(out, {"ok": True})

    def test_upload_file_chunked_sets_content_range(self) -> None:
        from shrink_media_worker.transport import upload_file_chunked

        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "b.bin"
            src.write_bytes(b"0123456789ab")  # 12 bytes

            calls: list[tuple[str, bytes]] = []

            mock_resp = Mock()
            mock_resp.headers = {"content-type": "application/json"}
            mock_resp.json.return_value = {"ok": True}
            mock_resp.raise_for_status.return_value = None

            def fake_request(method: str, url: str, *, content: bytes, headers: dict, timeout: int, follow_redirects: bool):
                calls.append((headers.get("Content-Range", ""), content))
                return mock_resp

            with patch("shrink_media_worker.transport.httpx.request", side_effect=fake_request):
                out = upload_file_chunked("http://example.invalid/upload", src, chunk_size=5, headers={"X": "1"})

            self.assertEqual(out, {"ok": True})
            self.assertEqual(
                [h for h, _ in calls],
                ["bytes 0-4/12", "bytes 5-9/12", "bytes 10-11/12"],
            )
            self.assertEqual([len(c) for _, c in calls], [5, 5, 2])

    def test_upload_file_chunked_retries_failed_chunk(self) -> None:
        from shrink_media_worker.transport import upload_file_chunked

        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "c.bin"
            src.write_bytes(b"012345")  # 6 bytes => 2 chunks with chunk_size=5

            mock_resp = Mock()
            mock_resp.headers = {"content-type": "application/json"}
            mock_resp.json.return_value = {"ok": True}
            mock_resp.raise_for_status.return_value = None

            request_calls = 0
            sleep_durations: list[int] = []

            def fake_request(method: str, url: str, *, content: bytes, headers: dict, timeout: int, follow_redirects: bool):
                nonlocal request_calls
                request_calls += 1
                if request_calls == 1:
                    raise RuntimeError("network glitch")
                return mock_resp

            def fake_sleep(seconds: int) -> None:
                sleep_durations.append(int(seconds))

            with (
                patch("shrink_media_worker.transport.httpx.request", side_effect=fake_request),
                patch("shrink_media_worker.transport.time.sleep", side_effect=fake_sleep),
            ):
                out = upload_file_chunked("http://example.invalid/upload", src, chunk_size=5, headers={})

            self.assertEqual(out, {"ok": True})
            self.assertEqual(request_calls, 3)
            self.assertEqual(sleep_durations, [1])
