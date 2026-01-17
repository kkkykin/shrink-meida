from __future__ import annotations

import json
import os
import tempfile
import unittest
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi.testclient import TestClient


@dataclass
class FakeRemoteInfo:
    path: str
    size: int
    name: str = ""
    sign: str = ""
    modified: Optional[str] = None


class FakeOpenListManager:
    """
    Minimal in-memory fake for shrink_media_server.openlist.OpenListManager.
    Only implements methods used by shrink_media_server.api endpoints.
    """

    def __init__(
        self,
        base_url: str = "http://openlist.invalid",
        user: str = "",
        password: str = "",
        otp_key: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.files: dict[str, FakeRemoteInfo] = {}
        self.dirs: set[str] = set()

    def get_download_url(self, path: str, *, base_url: str | None = None) -> dict:
        p = path if path.startswith("/") else f"/{path}"
        base = (base_url or self.base_url).rstrip("/")
        return {"url": f"{base}/d{p}", "expires_at": None}

    def get_direct_upload_info(self, dst_path: str, size: int, *, base_url: str | None = None) -> Optional[dict]:
        _ = dst_path, size, base_url
        return None

    def ensure_dir(self, path: str):
        self.dirs.add(path)

    def info(self, path: str):
        return self.files.get(path)

    def download_to(self, remote_path: str, local_path: Path) -> None:
        info = self.files.get(remote_path)
        if not info:
            raise FileNotFoundError(remote_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(b"x" * int(info.size))

    def upload_file(self, remote_path: str, local_file: Path, *, overwrite: bool) -> None:
        if not overwrite and remote_path in self.files:
            raise FileExistsError(remote_path)
        size = int(local_file.stat().st_size)
        self.files[remote_path] = FakeRemoteInfo(path=remote_path, size=size, name=Path(remote_path).name)

    def rename(self, src: str, dst: str):
        if src not in self.files:
            raise FileNotFoundError(src)
        if dst in self.files:
            raise FileExistsError(dst)
        info = self.files.pop(src)
        info.path = dst
        info.name = Path(dst).name
        self.files[dst] = info

    def remove(self, path: str):
        self.files.pop(path, None)

    def finalize(self, staging_path: str, final_path: str, expected_size: int) -> dict:
        staging = self.info(staging_path)
        if not staging:
            return {"ok": False, "error": "staging file not found", "final_size": 0}

        staging_size = int(staging.size)
        expected_size = int(expected_size)
        if staging_size != expected_size:
            return {
                "ok": False,
                "error": f"size mismatch: expected {expected_size}, got {staging_size}",
                "final_size": staging_size,
            }

        final = self.info(final_path)
        if final:
            final_size = int(final.size)
            if final_size == expected_size:
                self.remove(staging_path)
                return {"ok": True, "error": None, "final_size": final_size}
            return {
                "ok": False,
                "error": f"final path exists with different size: {final_size} != {expected_size}",
                "final_size": final_size,
            }

        self.files[final_path] = FakeRemoteInfo(path=final_path, size=expected_size, name=Path(final_path).name)
        self.remove(staging_path)
        return {"ok": True, "error": None, "final_size": expected_size}

    def close(self):
        return None


class ServerHarness(unittest.TestCase):
    bootstrap_token = "bootstrap-token"
    route_id = "r1"
    in_root = "/in"
    out_root = "/out"

    def setUp(self) -> None:
        super().setUp()
        self._old_cwd = os.getcwd()
        self._old_env = os.environ.copy()
        self._tmp = tempfile.TemporaryDirectory(prefix="shrink_media_server_tests_")
        self.workdir = Path(self._tmp.name)
        os.chdir(self.workdir)

        db_path = self.workdir / "server.sqlite"
        os.environ["SERVER_DB_URL"] = f"sqlite:///{db_path}"
        os.environ["ROUTES_JSON"] = json.dumps(
            [
                {
                    "id": self.route_id,
                    "in_root": self.in_root,
                    "out_root": self.out_root,
                    "profile": {"image_codec": "webp"},
                }
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
        self.openlist: FakeOpenListManager = server_api.openlist  # type: ignore[assignment]

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
        src_path: str = "/in/a.mov",
        src_rel: str = "a.mov",
        src_size: int = 123,
        src_mtime_ns: int = 1,
        status: str = "queued",
        lease_worker_id: int | None = None,
        lease_expires_at: datetime | None = None,
        attempts: int = 0,
        max_attempts: int = 3,
    ) -> str:
        from shrink_media_server.models import Task

        task_id = str(uuid.uuid4())
        session = self.server_api.db.get_session()
        try:
            task = Task(
                id=task_id,
                route_id=self.route_id,
                src_path=src_path,
                src_rel=src_rel,
                src_size=src_size,
                src_mtime_ns=src_mtime_ns,
                status=status,
                lease_worker_id=lease_worker_id,
                lease_expires_at=lease_expires_at,
                attempts=attempts,
                max_attempts=max_attempts,
                profile_json=json.dumps({"image_codec": "webp"}, ensure_ascii=False, separators=(",", ":")),
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
