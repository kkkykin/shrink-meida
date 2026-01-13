"""Worker main loop for shrink_media C/S architecture."""
from __future__ import annotations

import os
import signal
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import httpx

from shrink_media.processor import process_one_local

from .caps import detect_capabilities
from .transport import download_file, upload_file_chunked


@dataclass
class WorkerConfig:
    """Worker configuration from environment."""

    server_url: str
    bootstrap_token: Optional[str] = None
    worker_token: Optional[str] = None
    worker_name: str = "worker"
    lease_batch_size: int = 1
    heartbeat_interval: int = 60  # seconds

    @classmethod
    def from_env(cls) -> WorkerConfig:
        """Load configuration from environment variables."""
        server_url = os.getenv("WORKER_SERVER_URL", "http://localhost:8000")
        bootstrap_token = os.getenv("WORKER_BOOTSTRAP_TOKEN")
        worker_token = os.getenv("WORKER_TOKEN")
        worker_name = os.getenv("WORKER_NAME", f"worker-{os.getpid()}")
        lease_batch_size = int(os.getenv("WORKER_LEASE_BATCH_SIZE", "1"))
        heartbeat_interval = int(os.getenv("WORKER_HEARTBEAT_INTERVAL", "60"))

        return cls(
            server_url=server_url.rstrip("/"),
            bootstrap_token=bootstrap_token,
            worker_token=worker_token,
            worker_name=worker_name,
            lease_batch_size=lease_batch_size,
            heartbeat_interval=heartbeat_interval,
        )


class Worker:
    """Worker main loop."""

    def __init__(self, config: WorkerConfig):
        self.config = config
        self.worker_id: Optional[int] = None
        self.worker_token: Optional[str] = config.worker_token
        self.client = httpx.Client(timeout=300.0)
        self.running = True
        self.active_tasks_lock = threading.Lock()
        self.active_tasks: dict[str, dict] = {}
        self.heartbeat_thread: Optional[threading.Thread] = None

    def _auth_headers(self) -> dict[str, str]:
        """Get authorization headers."""
        if not self.worker_token:
            raise RuntimeError("Worker not registered")
        return {"Authorization": f"Bearer {self.worker_token}"}

    def register(self) -> None:
        """Register worker with server."""
        if self.worker_token:
            # Already have token, verify it works and discover worker_id.
            try:
                resp = self.client.get(f"{self.config.server_url}/v1/workers/me", headers=self._auth_headers())
                resp.raise_for_status()
                data = resp.json()
                self.worker_id = int(data["worker_id"])
                print(f"Using existing worker token (worker_id={self.worker_id})")
                return
            except Exception:
                print("Existing token invalid, re-registering...")

        if not self.config.bootstrap_token:
            raise RuntimeError("No bootstrap token provided (set WORKER_BOOTSTRAP_TOKEN)")

        caps = detect_capabilities()
        resp = self.client.post(
            f"{self.config.server_url}/v1/workers/register",
            json={"name": self.config.worker_name, "caps": caps},
            headers={"Authorization": f"Bearer {self.config.bootstrap_token}"},
        )
        resp.raise_for_status()
        data = resp.json()
        self.worker_id = data["worker_id"]
        self.worker_token = data["worker_token"]
        print(f"Registered as worker {self.worker_id}")
        print(f"Worker token: {self.worker_token}")
        print("Save this token to WORKER_TOKEN env var for future runs")

    def lease_tasks(self) -> list[dict]:
        """Lease tasks from server."""
        resp = self.client.post(
            f"{self.config.server_url}/v1/tasks/lease",
            json={"worker_id": self.worker_id, "n": self.config.lease_batch_size},
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("tasks", [])

    def heartbeat_task(self, task_id: str) -> None:
        """Send heartbeat for a task."""
        try:
            resp = self.client.post(
                f"{self.config.server_url}/v1/tasks/{task_id}/heartbeat",
                json={"worker_id": self.worker_id},
                headers=self._auth_headers(),
            )
            resp.raise_for_status()
        except Exception as e:
            print(f"Heartbeat failed for task {task_id}: {e}")

    def heartbeat_loop(self) -> None:
        """Background thread to send heartbeats."""
        while self.running:
            time.sleep(self.config.heartbeat_interval)
            if not self.running:
                break
            with self.active_tasks_lock:
                task_ids = list(self.active_tasks.keys())
            for task_id in task_ids:
                if not self.running:
                    break
                self.heartbeat_task(task_id)

    def process_task(self, task: dict) -> None:
        """Process a single task."""
        task_id = task["task_id"]
        with self.active_tasks_lock:
            self.active_tasks[task_id] = task

        try:
            print(f"\n[{task_id}] Processing {task['src_rel']}")

            # Download source
            with tempfile.TemporaryDirectory(prefix=f"worker_{task_id}_") as tmpdir:
                tmp_path = Path(tmpdir)
                src_local = tmp_path / "input"
                download_url = task["download"].get("url")
                if not download_url:
                    raise RuntimeError("No download URL provided")

                download_headers: Optional[dict[str, str]] = None
                if download_url.startswith("/"):
                    download_url = f"{self.config.server_url}{download_url}"
                    download_headers = self._auth_headers()
                elif download_url.startswith(self.config.server_url):
                    download_headers = self._auth_headers()

                print(f"[{task_id}] Downloading from {download_url}")
                download_file(download_url, src_local, headers=download_headers)

                # Parse profile
                profile = task.get("profile", {})

                # Create temporary in/out roots
                in_root = tmp_path / "in"
                out_root = tmp_path / "out"
                in_root.mkdir()
                out_root.mkdir()

                # Move input to in_root with relative path structure
                src_rel = task["src_rel"]
                src_in_root = in_root / src_rel
                src_in_root.parent.mkdir(parents=True, exist_ok=True)
                src_local.rename(src_in_root)

                # Process
                print(f"[{task_id}] Transcoding...")
                result = process_one_local(
                    src_local=src_in_root,
                    in_root=in_root,
                    out_root=out_root,
                    container=profile.get("container", "mp4"),
                    video_policy=profile.get("video_policy", "transcode"),
                    audio_policy=profile.get("audio_policy", "transcode"),
                    allow_opus_in_mp4=profile.get("allow_opus_in_mp4", False),
                    video_encoder=profile.get("video_encoder", "libx264"),
                    video_crf=profile.get("video_crf", 23),
                    video_preset=profile.get("video_preset", "medium"),
                    pix_fmt=profile.get("pix_fmt", "yuv420p"),
                    image_codec=profile.get("image_codec", "webp"),
                    webp_quality=profile.get("webp_quality", 85),
                    webp_lossless=profile.get("webp_lossless", False),
                    avif_crf=profile.get("avif_crf", 28),
                    avif_pix_fmt=profile.get("avif_pix_fmt", "yuv420p"),
                    faststart=profile.get("faststart", True),
                    overwrite=True,
                    dry_run=False,
                    min_savings=profile.get("min_savings", 0.05),
                    try_archives=profile.get("try_archives", True),
                    comic_min_images=profile.get("comic_min_images", 3),
                    comic_keep_non_images=profile.get("comic_keep_non_images", False),
                    comic_accept_bigger=profile.get("comic_accept_bigger", False),
                    archive_password=profile.get("archive_password"),
                    out_name_mode="suffix",
                    src_size_hint=task["src_size"],
                )

                if not result.ok:
                    raise RuntimeError(result.msg)

                action = result.action
                if action == "dry-run":
                    action = "skip"

                # Handle skip action
                if action == "skip":
                    print(f"[{task_id}] Skipped: {result.msg}")
                    resp = self.client.post(
                        f"{self.config.server_url}/v1/tasks/{task_id}/complete",
                        json={
                            "worker_id": self.worker_id,
                            "staging_path": "",
                            "action": "skip",
                            "out_size": task["src_size"],
                            "metrics": {},
                        },
                        headers=self._auth_headers(),
                    )
                    resp.raise_for_status()
                    print(f"[{task_id}] Completed (skip)")
                    return

                # Get output file
                if not result.out_local or not result.out_local.exists():
                    raise RuntimeError("Output file not found")

                out_size = result.out_local.stat().st_size
                out_ext = result.out_local.suffix

                # Get upload capability
                print(f"[{task_id}] Requesting upload capability...")
                resp = self.client.post(
                    f"{self.config.server_url}/v1/tasks/{task_id}/upload_intent",
                    json={
                        "worker_id": self.worker_id,
                        "out_size": out_size,
                        "out_ext": out_ext,
                        "action": action,
                    },
                    headers=self._auth_headers(),
                )
                resp.raise_for_status()
                upload_data = resp.json()
                staging_path = upload_data["staging_path"]
                upload_info = upload_data["upload"]

                # Upload
                print(f"[{task_id}] Uploading {out_size} bytes...")
                upload_url = upload_info["url"]
                upload_headers = dict(upload_info.get("headers", {}) or {})
                if upload_url.startswith("/"):
                    # Relative URL to server.
                    upload_url = f"{self.config.server_url}{upload_url}"
                    upload_headers.setdefault("Authorization", self._auth_headers()["Authorization"])
                elif upload_url.startswith(self.config.server_url):
                    upload_headers.setdefault("Authorization", self._auth_headers()["Authorization"])

                upload_file_chunked(
                    upload_url,
                    result.out_local,
                    method=upload_info.get("method", "PUT"),
                    chunk_size=upload_info.get("chunk_size", 5 * 1024 * 1024),
                    headers=upload_headers,
                )

                # Complete
                print(f"[{task_id}] Completing...")
                resp = self.client.post(
                    f"{self.config.server_url}/v1/tasks/{task_id}/complete",
                    json={
                        "worker_id": self.worker_id,
                        "staging_path": staging_path,
                        "action": action,
                        "out_size": out_size,
                        "metrics": {},
                    },
                    headers=self._auth_headers(),
                )
                resp.raise_for_status()
                complete_data = resp.json()
                if not complete_data.get("ok"):
                    raise RuntimeError(complete_data.get("message", "Complete failed"))

                print(f"[{task_id}] Completed ({action}): {result.msg}")

        except Exception as e:
            print(f"[{task_id}] Failed: {e}")
            try:
                resp = self.client.post(
                    f"{self.config.server_url}/v1/tasks/{task_id}/fail",
                    json={
                        "worker_id": self.worker_id,
                        "err": str(e),
                        "retryable": True,
                    },
                    headers=self._auth_headers(),
                )
                resp.raise_for_status()
            except Exception as e2:
                print(f"[{task_id}] Failed to report failure: {e2}")
        finally:
            with self.active_tasks_lock:
                self.active_tasks.pop(task_id, None)

    def run(self) -> None:
        """Main worker loop."""
        # Register
        self.register()

        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

        # Main loop
        print("\nWorker started, waiting for tasks...")
        while self.running:
            try:
                # Lease tasks
                tasks = self.lease_tasks()
                if not tasks:
                    time.sleep(5)
                    continue

                # Process tasks
                for task in tasks:
                    if not self.running:
                        break
                    self.process_task(task)

            except KeyboardInterrupt:
                print("\nShutting down...")
                self.running = False
                break
            except Exception as e:
                print(f"Error in main loop: {e}")
                time.sleep(5)

        print("Worker stopped")

    def shutdown(self) -> None:
        """Graceful shutdown."""
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)
        self.client.close()


def main() -> None:
    """Main entry point."""
    config = WorkerConfig.from_env()
    worker = Worker(config)

    # Handle signals
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, shutting down...")
        worker.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        worker.run()
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
