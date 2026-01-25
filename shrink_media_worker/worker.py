"""Worker main loop for shrink_media C/S architecture."""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import logging
import os
import signal
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import httpx

from .caps import detect_capabilities
from .transport import download_file, upload_file_chunked

logger = logging.getLogger(__name__)

# NOTE: This is intentionally loaded lazily (and patchable for unit tests).
process_one_local = None


def _ensure_process_one_local():
    global process_one_local
    if process_one_local is None:
        from shrink_media.processor import process_one_local as pol

        process_one_local = pol
    return process_one_local


@dataclass
class WorkerConfig:
    """Worker configuration from environment."""

    server_url: str
    bootstrap_token: Optional[str] = None
    worker_token: Optional[str] = None
    worker_name: str = "worker"
    lease_batch_size: int = 1
    heartbeat_interval: int = 60  # seconds
    lease_poll_interval: int = 5  # seconds (base, exponential backoff)
    lease_poll_max_interval: int = 60  # seconds (cap, exponential backoff)
    max_inflight_tasks: int = 2
    download_concurrency: int = 2
    transcode_concurrency: int = 1
    upload_concurrency: int = 2

    @classmethod
    def from_env(cls) -> WorkerConfig:
        """Load configuration from environment variables."""
        def _pos_int(name: str, default: int) -> int:
            raw = os.getenv(name)
            if raw is None:
                return default
            try:
                value = int(raw)
            except ValueError:
                return default
            return max(1, value)

        server_url = os.getenv("WORKER_SERVER_URL", "http://localhost:8000")
        bootstrap_token = os.getenv("WORKER_BOOTSTRAP_TOKEN")
        worker_token = os.getenv("WORKER_TOKEN")
        worker_name = os.getenv("WORKER_NAME", f"worker-{os.getpid()}")
        lease_batch_size = _pos_int("WORKER_LEASE_BATCH_SIZE", 1)
        heartbeat_interval = _pos_int("WORKER_HEARTBEAT_INTERVAL", 60)
        lease_poll_interval = _pos_int("WORKER_LEASE_POLL_INTERVAL_SECONDS", 5)
        lease_poll_max_interval = _pos_int("WORKER_LEASE_POLL_MAX_INTERVAL_SECONDS", 60)
        max_inflight_tasks = _pos_int("WORKER_MAX_INFLIGHT_TASKS", 2)
        download_concurrency = _pos_int("WORKER_DOWNLOAD_CONCURRENCY", 2)
        transcode_concurrency = _pos_int("WORKER_TRANSCODE_CONCURRENCY", 1)
        upload_concurrency = _pos_int("WORKER_UPLOAD_CONCURRENCY", 2)

        return cls(
            server_url=server_url.rstrip("/"),
            bootstrap_token=bootstrap_token,
            worker_token=worker_token,
            worker_name=worker_name,
            lease_batch_size=lease_batch_size,
            heartbeat_interval=heartbeat_interval,
            lease_poll_interval=lease_poll_interval,
            lease_poll_max_interval=lease_poll_max_interval,
            max_inflight_tasks=max_inflight_tasks,
            download_concurrency=download_concurrency,
            transcode_concurrency=transcode_concurrency,
            upload_concurrency=upload_concurrency,
        )


class Worker:
    """Worker main loop."""

    def __init__(self, config: WorkerConfig):
        self.config = config
        self.worker_id: Optional[int] = None
        self.worker_token: Optional[str] = config.worker_token
        self.client = httpx.Client(timeout=300.0)
        self.running = True
        self.shutting_down = False
        self.active_tasks_lock = threading.Lock()
        self.active_tasks: dict[str, dict] = {}
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.current_task_id: Optional[str] = None
        self._client_lock = threading.Lock()
        self._download_sem = threading.Semaphore(self.config.download_concurrency)
        self._transcode_sem = threading.Semaphore(self.config.transcode_concurrency)
        self._upload_sem = threading.Semaphore(self.config.upload_concurrency)

    def _client_get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ):  # noqa: ANN201 - response type depends on client
        with self._client_lock:
            return self.client.get(url, headers=headers, timeout=timeout)

    def _client_post(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ):  # noqa: ANN201 - response type depends on client
        with self._client_lock:
            return self.client.post(url, json=json, headers=headers, timeout=timeout)

    def _auth_headers(self) -> dict[str, str]:
        """Get authorization headers."""
        if not self.worker_token:
            raise RuntimeError("Worker not registered")
        return {"Authorization": f"Bearer {self.worker_token}"}

    def request_shutdown(self) -> None:
        """Request graceful shutdown (stop leasing new tasks)."""
        self.shutting_down = True

    def register(self) -> None:
        """Register worker with server."""
        if self.worker_token:
            # Already have token, verify it works and discover worker_id.
            try:
                resp = self._client_get(f"{self.config.server_url}/v1/workers/me", headers=self._auth_headers())
                resp.raise_for_status()
                data = resp.json()
                self.worker_id = int(data["worker_id"])
                allow_kinds = data.get("allow_kinds")
                allow_routes = data.get("allow_routes")
                scope_hint = ""
                if allow_kinds is not None or allow_routes is not None:
                    scope_hint = f", allow_kinds={allow_kinds}, allow_routes={allow_routes}"
                print(f"Using existing worker token (worker_id={self.worker_id}{scope_hint})")
                return
            except Exception:
                print("Existing token invalid, re-registering...")

        if not self.config.bootstrap_token:
            raise RuntimeError("No bootstrap token provided (set WORKER_BOOTSTRAP_TOKEN)")

        caps = detect_capabilities()
        resp = self._client_post(
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
        allow_kinds = data.get("allow_kinds")
        allow_routes = data.get("allow_routes")
        if allow_kinds is not None or allow_routes is not None:
            print(f"Worker scope: allow_kinds={allow_kinds}, allow_routes={allow_routes}")
        print("Save this token to WORKER_TOKEN env var for future runs")

    def lease_tasks(self, *, n: int | None = None) -> list[dict]:
        """Lease tasks from server."""
        n = int(n if n is not None else self.config.lease_batch_size)
        resp = self._client_post(
            f"{self.config.server_url}/v1/tasks/lease",
            json={"worker_id": self.worker_id, "n": max(1, n)},
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("tasks", [])

    def heartbeat_task(self, task_id: str) -> None:
        """Send heartbeat for a task."""
        try:
            resp = self._client_post(
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
        task_start = time.time()
        with self.active_tasks_lock:
            self.active_tasks[task_id] = task
            self.current_task_id = task_id

        try:
            if self.shutting_down:
                raise RuntimeError("Worker shutting down")
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
                dl_start = time.time()
                with self._download_sem:
                    download_file(download_url, src_local, headers=download_headers)
                dl_time = time.time() - dl_start
                logger.info(
                    "Download complete",
                    extra={
                        "task_id": task_id,
                        "download_time_s": round(dl_time, 2),
                        "size_bytes": task["src_size"],
                    },
                )

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
                _ensure_process_one_local()

                transcode_start = time.time()
                with self._transcode_sem:
                    result = process_one_local(
                        src_local=src_in_root,
                        in_root=in_root,
                        out_root=out_root,
                        container=profile.get("container", "mp4"),
                        video_policy=profile.get("video_policy", "transcode"),
                        audio_policy=profile.get("audio_policy", "transcode"),
                        allow_opus_in_mp4=profile.get("allow_opus_in_mp4", False),
                        video_encoder=profile.get("video_encoder", "auto"),
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
                        tolerate_corrupt=profile.get("tolerate_corrupt", False),
                        out_name_mode="suffix",
                        src_size_hint=task["src_size"],
                    )
                transcode_time = time.time() - transcode_start

                logger.info(
                    "Transcode complete",
                    extra={
                        "task_id": task_id,
                        "transcode_time_s": round(transcode_time, 2),
                        "ok": result.ok,
                        "action": result.action,
                        "ffmpeg_rc": getattr(result, "ffmpeg_rc", None),
                        "result_msg": result.msg[:200] if result.msg else None,
                    },
                )

                if not result.ok:
                    raise RuntimeError(result.msg)

                action = result.action
                if action == "dry-run":
                    action = "skip"

                # Handle skip action
                if action == "skip":
                    print(f"[{task_id}] Skipped: {result.msg}")
                    resp = self._client_post(
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
                resp = self._client_post(
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

                upload_start = time.time()
                try:
                    with self._upload_sem:
                        upload_file_chunked(
                            upload_url,
                            result.out_local,
                            method=upload_info.get("method", "PUT"),
                            chunk_size=upload_info.get("chunk_size", 5 * 1024 * 1024),
                            headers=upload_headers,
                        )
                except httpx.HTTPStatusError as e:
                    body = (e.response.text or "").strip()
                    if body:
                        body = body[:2000]
                        raise RuntimeError(f"{e}; response={body}") from e
                    raise
                upload_time = time.time() - upload_start

                src_size = task["src_size"]
                size_change_pct = ((out_size - src_size) / src_size * 100) if src_size > 0 else 0
                logger.info(
                    "Upload complete",
                    extra={
                        "task_id": task_id,
                        "upload_time_s": round(upload_time, 2),
                        "src_size": src_size,
                        "out_size": out_size,
                        "size_change_pct": round(size_change_pct, 1),
                    },
                )

                # Complete
                print(f"[{task_id}] Completing...")
                resp = self._client_post(
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

                task_time = time.time() - task_start
                logger.info(
                    "Task completed successfully",
                    extra={
                        "task_id": task_id,
                        "total_time_s": round(task_time, 2),
                        "action": action,
                        "result_msg": result.msg[:200] if result.msg else None,
                    },
                )
                print(f"[{task_id}] Completed ({action}): {result.msg}")

        except KeyboardInterrupt:
            task_time = time.time() - task_start
            logger.warning(
                "Task interrupted by SIGINT",
                extra={
                    "task_id": task_id,
                    "total_time_s": round(task_time, 2),
                },
            )
            print(f"[{task_id}] Interrupted")
            try:
                resp = self._client_post(
                    f"{self.config.server_url}/v1/tasks/{task_id}/fail",
                    json={
                        "worker_id": self.worker_id,
                        "err": "worker interrupted (SIGINT)",
                        "retryable": True,
                    },
                    headers=self._auth_headers(),
                    timeout=5.0,
                )
                resp.raise_for_status()
            except Exception as e2:
                print(f"[{task_id}] Failed to report interruption: {e2}")
            self.shutting_down = True
            self.running = False
            raise
        except Exception as e:
            task_time = time.time() - task_start
            logger.error(
                "Task failed",
                extra={
                    "task_id": task_id,
                    "total_time_s": round(task_time, 2),
                    "error": str(e)[:200],
                },
            )
            print(f"[{task_id}] Failed: {e}")
            retryable = True
            if isinstance(e, httpx.HTTPStatusError):
                try:
                    status_code = int(e.response.status_code)
                except Exception:
                    status_code = None
                if status_code in {401, 403, 404}:
                    retryable = False
            try:
                resp = self._client_post(
                    f"{self.config.server_url}/v1/tasks/{task_id}/fail",
                    json={
                        "worker_id": self.worker_id,
                        "err": str(e),
                        "retryable": retryable,
                    },
                    headers=self._auth_headers(),
                )
                resp.raise_for_status()
            except Exception as e2:
                print(f"[{task_id}] Failed to report failure: {e2}")
        finally:
            with self.active_tasks_lock:
                self.active_tasks.pop(task_id, None)
                if self.current_task_id == task_id:
                    self.current_task_id = None

    def run(self, once: bool = False) -> None:
        """Main worker loop."""
        # Register
        self.register()

        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

        # Main loop
        print("\nWorker started, waiting for tasks...")
        logger.info(
            "Worker concurrency configured",
            extra={
                "max_inflight_tasks": int(self.config.max_inflight_tasks),
                "download_concurrency": int(self.config.download_concurrency),
                "transcode_concurrency": int(self.config.transcode_concurrency),
                "upload_concurrency": int(self.config.upload_concurrency),
                "lease_poll_base_interval_s": int(self.config.lease_poll_interval),
                "lease_poll_max_interval_s": int(self.config.lease_poll_max_interval),
            },
        )
        in_flight: set[Future[None]] = set()
        leased_once = False
        max_inflight = max(1, int(self.config.max_inflight_tasks))
        lease_poll_base_interval = max(1, int(self.config.lease_poll_interval))
        lease_poll_max_interval = max(lease_poll_base_interval, int(self.config.lease_poll_max_interval))
        lease_backoff = lease_poll_base_interval
        next_lease_at = 0.0  # monotonic seconds

        with ThreadPoolExecutor(max_workers=max_inflight) as executor:
            while self.running:
                try:
                    # Drain completed tasks first so we can lease more.
                    done = {f for f in in_flight if f.done()}
                    if done:
                        # Progress happened; allow leasing immediately (avoid waiting full backoff interval).
                        next_lease_at = 0.0
                        lease_backoff = lease_poll_base_interval
                    for f in done:
                        in_flight.remove(f)
                        try:
                            f.result()
                        except KeyboardInterrupt:
                            raise
                        except Exception:
                            logger.exception("Unhandled exception in worker task")

                    if self.shutting_down:
                        break

                    if once and leased_once and not in_flight:
                        print("Completed one lease cycle (--once mode)")
                        break

                    # Lease up to remaining capacity (bounded).
                    capacity = max_inflight - len(in_flight)
                    now_mono = time.monotonic()
                    if capacity > 0 and (not once or not leased_once) and now_mono >= next_lease_at:
                        lease_n = min(int(self.config.lease_batch_size), capacity)
                        tasks = self.lease_tasks(n=lease_n)
                        if once:
                            leased_once = True

                        if tasks:
                            next_lease_at = 0.0
                            lease_backoff = lease_poll_base_interval
                            for task in tasks:
                                if not self.running or self.shutting_down:
                                    break
                                in_flight.add(executor.submit(self.process_task, task))
                            continue

                        if once:
                            print("No tasks available (--once mode)")
                            break

                        # No tasks available right now; exponential backoff lease polling to avoid spamming server.
                        current_backoff = lease_backoff
                        next_lease_at = time.monotonic() + current_backoff
                        lease_backoff = min(lease_poll_max_interval, lease_backoff * 2)
                        if in_flight:
                            wait(in_flight, timeout=current_backoff, return_when=FIRST_COMPLETED)
                        else:
                            time.sleep(current_backoff)
                        continue

                    # No capacity or (--once and already leased): wait for progress.
                    if in_flight:
                        timeout = None
                        if capacity > 0 and (not once or not leased_once) and next_lease_at > 0.0:
                            timeout = max(0.0, next_lease_at - time.monotonic())
                        wait(in_flight, timeout=timeout, return_when=FIRST_COMPLETED)
                    else:
                        if capacity > 0 and (not once or not leased_once) and next_lease_at > 0.0:
                            time.sleep(max(0.0, next_lease_at - time.monotonic()))
                        else:
                            time.sleep(1)

                except KeyboardInterrupt:
                    print("\nShutting down...")
                    self.running = False
                    break
                except Exception as e:
                    print(f"Error in main loop: {e}")
                    logger.exception("Error in main loop")
                    if once:
                        break
                    time.sleep(5)

            if in_flight:
                print("Waiting for in-flight tasks to finish...")
                wait(in_flight)

        print("Worker stopped")

    def shutdown(self, graceful: bool = True) -> None:
        """Graceful shutdown."""
        _ = graceful
        self.shutting_down = True
        self.running = False

        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)
            self.heartbeat_thread = None
        with self._client_lock:
            self.client.close()


def main() -> None:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="shrink_media worker")
    parser.add_argument("--once", action="store_true", help="Run one lease cycle and exit (for debugging)")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logs (engine ffmpeg candidates, retries, and detailed tracebacks)",
    )
    args = parser.parse_args()

    if args.debug:
        from shrink_media.logging import configure_logging

        configure_logging(None, append=True, debug=True, prefetch_debug=False)
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    config = WorkerConfig.from_env()
    worker = Worker(config)

    # Handle signals
    def signal_handler(signum, frame):
        _ = frame
        if worker.shutting_down:
            os._exit(128 + int(signum))
        worker.request_shutdown()
        if signum == signal.SIGINT:
            raise KeyboardInterrupt
        print(f"\nReceived signal {signum}, requesting shutdown...")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        worker.run(once=args.once)
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
