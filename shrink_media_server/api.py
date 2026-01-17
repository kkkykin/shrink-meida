"""FastAPI server for shrink_media_server."""
from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Literal, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.orm import Session

from .config import ServerConfig
from .models import Attempt, Database, Task, Worker
from .openlist import OpenListManager
from shrink_media.workitem import build_suffixed_target_name, normalize_ext

logger = logging.getLogger(__name__)


Action = Literal["ok", "copy", "skip"]


# Request/Response models
class RegisterRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    caps: dict = Field(default_factory=dict)


class RegisterResponse(BaseModel):
    worker_id: int
    worker_token: str


class WorkerMeResponse(BaseModel):
    worker_id: int
    name: str
    caps: dict = Field(default_factory=dict)


class LeaseRequest(BaseModel):
    worker_id: int = Field(ge=1)
    n: int = Field(default=1, ge=1, le=50)


class TaskInfo(BaseModel):
    task_id: str
    route_id: str
    src_path: str
    src_rel: str
    src_size: int
    src_mtime_ns: int
    profile: dict
    download: dict
    lease_expires_at: str


class LeaseResponse(BaseModel):
    tasks: list[TaskInfo]


class HeartbeatRequest(BaseModel):
    worker_id: int = Field(ge=1)


class HeartbeatResponse(BaseModel):
    lease_expires_at: str


class UploadIntentRequest(BaseModel):
    worker_id: int = Field(ge=1)
    out_size: int = Field(ge=0)
    out_ext: str = Field(default="", max_length=16, pattern=r"^$|^\.?[A-Za-z0-9]{1,10}$")
    action: Action


class UploadIntentResponse(BaseModel):
    staging_path: str
    upload: dict


class CompleteRequest(BaseModel):
    worker_id: int = Field(ge=1)
    staging_path: str = Field(default="", max_length=4096)
    action: Action
    out_size: int = Field(ge=0)
    metrics: Optional[dict] = None

    @model_validator(mode="after")
    def _validate_staging_path(self) -> CompleteRequest:
        if self.action in {"ok", "copy"} and not self.staging_path:
            raise ValueError("staging_path is required for ok/copy")
        return self


class CompleteResponse(BaseModel):
    ok: bool
    message: str


class FailRequest(BaseModel):
    worker_id: int = Field(ge=1)
    err: str = Field(min_length=1, max_length=10000)
    retryable: bool = True


class FailResponse(BaseModel):
    ok: bool
    message: str


class AdminTaskInfo(BaseModel):
    task_id: str
    route_id: str
    src_path: str
    src_rel: str
    status: str
    attempts: int
    max_attempts: int
    lease_worker_id: Optional[int] = None
    lease_expires_at: Optional[str] = None
    action: Optional[str] = None
    out_size: Optional[int] = None
    staging_path: Optional[str] = None
    final_path: Optional[str] = None
    last_error: Optional[str] = None
    created_at: str
    updated_at: str


class AdminListTasksResponse(BaseModel):
    tasks: list[AdminTaskInfo]


class AdminRequeueRequest(BaseModel):
    status_in: list[str] = Field(default_factory=lambda: ["deadletter"])
    route_id: Optional[str] = Field(default=None, max_length=255)
    src_path_prefix: Optional[str] = Field(default=None, max_length=2048)
    reset_attempts: bool = True
    limit: int = Field(default=1000, ge=1, le=10000)


class AdminRequeueResponse(BaseModel):
    ok: bool
    updated: int


# Global state
config: ServerConfig
db: Database
openlist: OpenListManager


def _parse_safe_rel(rel: str) -> PurePosixPath:
    p = PurePosixPath(rel)
    if p.is_absolute():
        raise ValueError("rel path must be relative")
    if ".." in p.parts:
        raise ValueError("rel path must not contain '..'")
    return p


def _parse_safe_abs(p: str) -> PurePosixPath:
    pp = PurePosixPath(p)
    if not pp.is_absolute():
        raise ValueError("path must be absolute")
    if ".." in pp.parts:
        raise ValueError("path must not contain '..'")
    return pp


def _build_final_path(*, out_root: str, src_rel: str, out_ext: str) -> str:
    out_root_p = _parse_safe_abs(out_root.rstrip("/") or "/")
    rel_p = _parse_safe_rel(src_rel)

    target_ext = normalize_ext(out_ext)
    if not target_ext:
        final_rel = rel_p
    elif rel_p.suffix.lower() == target_ext:
        final_rel = rel_p
    else:
        final_rel = rel_p.with_name(build_suffixed_target_name(rel_p.name, target_ext=target_ext))

    return str(out_root_p / final_rel)


def _build_staging_path(*, out_root: str, task_id: str, nonce: str) -> str:
    if not task_id or "/" in task_id or "\\" in task_id or ".." in task_id:
        raise ValueError("invalid task_id")
    if not nonce or "/" in nonce or "\\" in nonce or ".." in nonce:
        raise ValueError("invalid nonce")
    out_root_p = _parse_safe_abs(out_root.rstrip("/") or "/")
    return str(out_root_p / ".shrink_media_staging" / task_id / nonce / "blob")


def _is_task_staging_path(*, path: str, out_root: str, task_id: str) -> bool:
    try:
        p = _parse_safe_abs(path)
        out_root_p = _parse_safe_abs(out_root.rstrip("/") or "/")
        prefix = out_root_p / ".shrink_media_staging" / task_id
        return p.is_relative_to(prefix)
    except Exception:
        return False


def init_app() -> FastAPI:
    """Initialize FastAPI application."""
    global config, db, openlist

    config = ServerConfig.from_env()
    db = Database(config.db_url)
    db.create_tables()
    openlist = OpenListManager(
        base_url=config.openlist_base_url,
        user=config.openlist_user,
        password=config.openlist_password,
        otp_key=config.openlist_otp,
    )

    app = FastAPI(title="shrink_media_server")

    # Dependency: Get DB session
    def get_db():
        session = db.get_session()
        try:
            yield session
        finally:
            session.close()

    def _get_bearer_token(authorization: Optional[str]) -> str:
        if not authorization:
            raise HTTPException(status_code=401, detail="Missing authorization header")
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid authorization header")
        token = authorization[7:].strip()
        if not token:
            raise HTTPException(status_code=401, detail="Invalid authorization header")
        return token

    # Dependency: Bootstrap token (register only)
    def require_bootstrap_token(authorization: Optional[str] = Header(None)) -> str:
        token = _get_bearer_token(authorization)
        if token not in config.bootstrap_tokens:
            raise HTTPException(status_code=401, detail="Invalid token")
        return token

    # Dependency: Worker token (per-worker token, stored hashed in DB)
    def authenticate_worker(
        session: Session = Depends(get_db),
        authorization: Optional[str] = Header(None),
    ) -> Worker:
        token = _get_bearer_token(authorization)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        worker = session.query(Worker).filter(Worker.token_hash == token_hash).first()
        if not worker:
            raise HTTPException(status_code=401, detail="Invalid token")
        worker.last_seen_at = datetime.now(timezone.utc)
        session.commit()
        return worker

    @app.post("/v1/workers/register", response_model=RegisterResponse)
    def register_worker(
        req: RegisterRequest,
        session: Session = Depends(get_db),
        token: str = Depends(require_bootstrap_token),
    ):
        """Register a new worker."""
        # Generate worker token
        worker_token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(worker_token.encode()).hexdigest()

        # Create worker
        worker = Worker(
            name=req.name,
            token_hash=token_hash,
            caps_json=json.dumps(req.caps, ensure_ascii=False, separators=(",", ":")),
            last_seen_at=datetime.now(timezone.utc),
        )
        session.add(worker)
        session.commit()
        session.refresh(worker)

        return RegisterResponse(worker_id=worker.id, worker_token=worker_token)

    @app.get("/v1/workers/me", response_model=WorkerMeResponse)
    def worker_me(worker: Worker = Depends(authenticate_worker)) -> WorkerMeResponse:
        """Return the authenticated worker info (token validation + worker_id discovery)."""
        try:
            caps = json.loads(worker.caps_json) if worker.caps_json else {}
        except Exception:
            caps = {}
        return WorkerMeResponse(worker_id=worker.id, name=worker.name, caps=caps)

    @app.post("/v1/tasks/lease", response_model=LeaseResponse)
    def lease_tasks(
        req: LeaseRequest,
        session: Session = Depends(get_db),
        worker: Worker = Depends(authenticate_worker),
    ):
        """Lease tasks for a worker."""
        if req.worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Worker mismatch")

        # Find available tasks (queued or expired leases)
        now = datetime.now(timezone.utc)
        tasks = (
            session.query(Task)
            .filter(
                (Task.status == "queued")
                | ((Task.status == "leased") & (Task.lease_expires_at < now)),
                Task.attempts < Task.max_attempts,
            )
            .limit(req.n)
            .all()
        )

        # Lease tasks
        lease_expires_at = now + timedelta(minutes=10)
        leased_tasks = []

        for task in tasks:
            task.status = "leased"
            task.lease_worker_id = worker.id
            task.lease_expires_at = lease_expires_at
            task.attempts += 1
            task.staging_path = None
            task.final_path = None
            task.action = None
            task.out_size = None
            task.updated_at = now

            # Get download URL
            download = openlist.get_download_url(task.src_path)

            # Parse profile
            try:
                profile0 = json.loads(task.profile_json) if task.profile_json else {}
            except Exception:
                profile0 = {}
            profile = profile0 if isinstance(profile0, dict) else {}

            logger.info(
                "Task leased",
                extra={
                    "task_id": task.id,
                    "worker_id": worker.id,
                    "route_id": task.route_id,
                    "src_path": task.src_path,
                    "attempt": task.attempts,
                    "status": "leased",
                },
            )

            leased_tasks.append(
                TaskInfo(
                    task_id=task.id,
                    route_id=task.route_id,
                    src_path=task.src_path,
                    src_rel=task.src_rel,
                    src_size=task.src_size,
                    src_mtime_ns=task.src_mtime_ns,
                    profile=profile,
                    download=download,
                    lease_expires_at=lease_expires_at.isoformat(),
                )
            )

        session.commit()

        return LeaseResponse(tasks=leased_tasks)

    @app.post("/v1/tasks/{task_id}/heartbeat", response_model=HeartbeatResponse)
    def heartbeat_task(
        task_id: str,
        req: HeartbeatRequest,
        session: Session = Depends(get_db),
        worker: Worker = Depends(authenticate_worker),
    ):
        """Renew task lease."""
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if req.worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Worker mismatch")

        if task.status == "finalized":
            raise HTTPException(status_code=409, detail="Task already finalized")

        if task.lease_worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Task not leased by this worker")

        # Renew lease
        now = datetime.now(timezone.utc)
        lease_expires_at = now + timedelta(minutes=10)
        task.lease_expires_at = lease_expires_at
        task.updated_at = now

        session.commit()

        return HeartbeatResponse(lease_expires_at=lease_expires_at.isoformat())

    @app.post("/v1/tasks/{task_id}/upload_intent", response_model=UploadIntentResponse)
    def upload_intent(
        task_id: str,
        req: UploadIntentRequest,
        session: Session = Depends(get_db),
        worker: Worker = Depends(authenticate_worker),
    ):
        """Get upload capability for task output."""
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if req.worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Worker mismatch")

        if task.status == "finalized":
            raise HTTPException(status_code=409, detail="Task already finalized")

        if task.lease_worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Task not leased by this worker")

        if req.action == "skip":
            raise HTTPException(status_code=400, detail="skip action does not require upload_intent")

        # Find route
        route = next((r for r in config.routes if r.id == task.route_id), None)
        if not route:
            raise HTTPException(status_code=500, detail="Route not found")

        # Generate staging path
        nonce = secrets.token_hex(8)
        try:
            staging_path = _build_staging_path(out_root=route.out_root, task_id=task_id, nonce=nonce)
            final_path = _build_final_path(out_root=route.out_root, src_rel=task.src_rel, out_ext=req.out_ext)
        except ValueError as e:
            raise HTTPException(status_code=500, detail=str(e))

        # Ensure staging directory exists
        staging_dir = str(PurePosixPath(staging_path).parent)
        openlist.ensure_dir(staging_dir)

        # Get direct upload info
        upload_info = openlist.get_direct_upload_info(staging_path, req.out_size)
        if upload_info:
            upload_info.setdefault("expires_at", None)
        else:
            # Fallback: provide proxy upload URL
            upload_info = {
                "url": f"/v1/tasks/{task_id}/upload_proxy",
                "method": "PUT",
                "chunk_size": 5 * 1024 * 1024,
                "headers": {},
                "expires_at": None,
            }

        # Update task
        task.staging_path = staging_path
        task.final_path = final_path
        task.action = req.action
        task.out_size = req.out_size
        task.updated_at = datetime.now(timezone.utc)
        session.commit()

        return UploadIntentResponse(staging_path=staging_path, upload=upload_info)

    @app.get("/v1/tasks/{task_id}/download_proxy")
    def download_proxy(
        task_id: str,
        background_tasks: BackgroundTasks,
        session: Session = Depends(get_db),
        worker: Worker = Depends(authenticate_worker),
    ):
        """Download task input via server proxy (fallback)."""
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if task.lease_worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Task not leased by this worker")

        tmp_dir = Path(tempfile.mkdtemp(prefix="shrink_media_server_dl_"))
        background_tasks.add_task(shutil.rmtree, tmp_dir, ignore_errors=True)
        tmp_path = tmp_dir / "blob"

        try:
            openlist.download_to(task.src_path, tmp_path)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Source not found")

        filename = PurePosixPath(task.src_rel).name
        return FileResponse(path=str(tmp_path), filename=filename, background=background_tasks)

    @app.put("/v1/tasks/{task_id}/upload_proxy")
    async def upload_proxy(
        task_id: str,
        request: Request,
        session: Session = Depends(get_db),
        worker: Worker = Depends(authenticate_worker),
    ):
        """Upload task output via server proxy (fallback)."""
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if task.lease_worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Task not leased by this worker")

        if not task.staging_path or not task.final_path or task.out_size is None or not task.action:
            raise HTTPException(status_code=409, detail="Call upload_intent before upload_proxy")

        # Enforce staging path safety
        route = next((r for r in config.routes if r.id == task.route_id), None)
        if not route:
            raise HTTPException(status_code=500, detail="Route not found")
        if not _is_task_staging_path(path=task.staging_path, out_root=route.out_root, task_id=task_id):
            raise HTTPException(status_code=500, detail="Invalid staging_path in task")

        expected_size = int(task.out_size)

        content_range = (request.headers.get("content-range") or "").strip()
        if content_range:
            m = re.match(r"^bytes (\\d+)-(\\d+)/(\\d+)$", content_range)
            if not m:
                raise HTTPException(status_code=400, detail="Invalid Content-Range header")
            start, end, total = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if total != expected_size:
                raise HTTPException(status_code=400, detail=f"Total size mismatch: expected {expected_size}, got {total}")
            if end < start:
                raise HTTPException(status_code=400, detail="Invalid Content-Range header")
            expected_chunk_len = end - start + 1

            base_dir = Path(tempfile.gettempdir()) / "shrink_media_server_upload_proxy" / task_id
            base_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = base_dir / "blob.part"

            current = tmp_path.stat().st_size if tmp_path.exists() else 0
            if start == 0 and current != 0:
                tmp_path.unlink(missing_ok=True)
                current = 0
            if start != current:
                raise HTTPException(status_code=409, detail=f"Out-of-order chunk: expected start {current}, got {start}")

            wrote = 0
            with tmp_path.open("ab") as f:
                async for chunk in request.stream():
                    f.write(chunk)
                    wrote += len(chunk)
            if wrote != expected_chunk_len:
                raise HTTPException(status_code=400, detail=f"Chunk size mismatch: expected {expected_chunk_len}, got {wrote}")
            if end + 1 < total:
                return {"ok": True, "received": end + 1, "total": total}

            try:
                openlist.upload_file(task.staging_path, tmp_path, overwrite=False)
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"OpenList upload failed: {e}")
            finally:
                shutil.rmtree(base_dir, ignore_errors=True)

            return {"ok": True, "staging_path": task.staging_path, "size": expected_size}

        tmp_dir = Path(tempfile.mkdtemp(prefix="shrink_media_server_ul_"))
        tmp_path = tmp_dir / "blob"
        try:
            with tmp_path.open("wb") as f:
                async for chunk in request.stream():
                    f.write(chunk)
            size = tmp_path.stat().st_size
            if expected_size != size:
                raise HTTPException(status_code=400, detail=f"Size mismatch: expected {expected_size}, got {size}")
            try:
                openlist.upload_file(task.staging_path, tmp_path, overwrite=False)
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"OpenList upload failed: {e}")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        return {"ok": True, "staging_path": task.staging_path, "size": expected_size}

    @app.post("/v1/tasks/{task_id}/complete", response_model=CompleteResponse)
    def complete_task(
        task_id: str,
        req: CompleteRequest,
        session: Session = Depends(get_db),
        worker: Worker = Depends(authenticate_worker),
    ):
        """Mark task as complete and finalize output."""
        start_time = time.time()
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if req.worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Worker mismatch")

        if task.status == "finalized":
            if task.action == "skip":
                if req.action != "skip":
                    raise HTTPException(status_code=409, detail="action mismatch")
                if task.out_size is not None and int(req.out_size) != int(task.out_size):
                    raise HTTPException(status_code=409, detail="out_size mismatch")
                return CompleteResponse(ok=True, message="Task already finalized")

            if not task.final_path or task.out_size is None or not task.action:
                raise HTTPException(status_code=500, detail="Finalized task missing final_path/out_size")
            if task.staging_path and req.staging_path != task.staging_path:
                raise HTTPException(status_code=409, detail="staging_path mismatch")
            if req.action != task.action:
                raise HTTPException(status_code=409, detail="action mismatch")
            if int(req.out_size) != int(task.out_size):
                raise HTTPException(status_code=409, detail="out_size mismatch")
            final_info = openlist.info(task.final_path)
            if not final_info:
                raise HTTPException(status_code=500, detail="Finalized task missing final file")
            final_size = int(getattr(final_info, "size", 0) or 0)
            if final_size != int(task.out_size):
                raise HTTPException(status_code=500, detail="Finalized task final size mismatch")
            return CompleteResponse(ok=True, message="Task already finalized")

        if task.lease_worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Task not leased by this worker")

        # Find route
        route = next((r for r in config.routes if r.id == task.route_id), None)
        if not route:
            raise HTTPException(status_code=500, detail="Route not found")

        if req.action == "skip":
            now = datetime.now(timezone.utc)
            task.status = "finalized"
            task.action = "skip"
            task.out_size = int(req.out_size)
            task.staging_path = None
            task.final_path = None
            task.last_error = None
            task.lease_worker_id = None
            task.lease_expires_at = None
            task.updated_at = now

            attempt = Attempt(
                task_id=task_id,
                worker_id=worker.id,
                started_at=now,
                finished_at=now,
                ok=1,
                action="skip",
                err=None,
                metrics_json=json.dumps(req.metrics, ensure_ascii=False, separators=(",", ":")) if req.metrics else None,
            )
            session.add(attempt)
            session.commit()

            latency_ms = int((time.time() - start_time) * 1000)
            logger.info(
                "Task completed (skip)",
                extra={
                    "task_id": task_id,
                    "worker_id": worker.id,
                    "route_id": task.route_id,
                    "action": "skip",
                    "attempt": task.attempts,
                    "status": "finalized",
                    "latency_ms": latency_ms,
                },
            )
            return CompleteResponse(ok=True, message="Task skipped")

        if not task.staging_path or not task.final_path or task.out_size is None or not task.action:
            raise HTTPException(status_code=409, detail="Call upload_intent before complete")

        if req.staging_path != task.staging_path:
            raise HTTPException(status_code=409, detail="staging_path mismatch")
        if req.action != task.action:
            raise HTTPException(status_code=409, detail="action mismatch")
        if int(req.out_size) != int(task.out_size):
            raise HTTPException(status_code=409, detail="out_size mismatch")

        if not _is_task_staging_path(path=task.staging_path, out_root=route.out_root, task_id=task_id):
            raise HTTPException(status_code=500, detail="Invalid staging_path in task")

        now = datetime.now(timezone.utc)
        expected_size = int(task.out_size)

        # Idempotency: if final already exists with correct size, treat as success.
        final_info = openlist.info(task.final_path)
        if final_info:
            final_size = int(getattr(final_info, "size", 0) or 0)
            if final_size == expected_size:
                task.status = "finalized"
                task.last_error = None
                task.updated_at = now
                task.lease_worker_id = None
                task.lease_expires_at = None
                session.commit()

                latency_ms = int((time.time() - start_time) * 1000)
                logger.info(
                    "Task completed (already finalized)",
                    extra={
                        "task_id": task_id,
                        "worker_id": worker.id,
                        "route_id": task.route_id,
                        "action": task.action,
                        "attempt": task.attempts,
                        "status": "finalized",
                        "latency_ms": latency_ms,
                    },
                )
                return CompleteResponse(ok=True, message="Task already finalized")

        # Finalize staging to final
        result = openlist.finalize(task.staging_path, task.final_path, expected_size)

        if result["ok"]:
            task.status = "finalized"
            task.last_error = None
            task.lease_worker_id = None
            task.lease_expires_at = None
        else:
            task.status = "failed"
            task.last_error = result["error"]

        task.updated_at = now

        # Log attempt
        attempt = Attempt(
            task_id=task_id,
            worker_id=worker.id,
            started_at=now,
            finished_at=now,
            ok=1 if result["ok"] else 0,
            action=task.action,
            err=result["error"],
            metrics_json=json.dumps(req.metrics, ensure_ascii=False, separators=(",", ":")) if req.metrics else None,
        )
        session.add(attempt)
        session.commit()

        latency_ms = int((time.time() - start_time) * 1000)
        logger.info(
            "Task completed",
            extra={
                "task_id": task_id,
                "worker_id": worker.id,
                "route_id": task.route_id,
                "action": task.action,
                "attempt": task.attempts,
                "status": task.status,
                "ok": result["ok"],
                "latency_ms": latency_ms,
                "src_size": task.src_size,
                "out_size": task.out_size,
            },
        )

        return CompleteResponse(
            ok=result["ok"],
            message=result["error"] or "Task completed successfully",
        )

    @app.post("/v1/tasks/{task_id}/fail", response_model=FailResponse)
    def fail_task(
        task_id: str,
        req: FailRequest,
        session: Session = Depends(get_db),
        worker: Worker = Depends(authenticate_worker),
    ):
        """Mark task as failed."""
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if req.worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Worker mismatch")

        # Idempotency: allow retrying the same fail call after the server released the lease.
        if task.status in {"queued", "deadletter"}:
            if task.last_error == req.err:
                return FailResponse(ok=True, message="Task already marked as failed")
            raise HTTPException(status_code=409, detail="Task already released")
        if task.status == "finalized":
            raise HTTPException(status_code=409, detail="Task already finalized")

        if task.lease_worker_id != worker.id:
            raise HTTPException(status_code=403, detail="Task not leased by this worker")

        # Update task status
        if req.retryable and task.attempts < task.max_attempts:
            task.status = "queued"  # Back to queue for retry
        else:
            task.status = "deadletter"

        task.last_error = req.err
        task.updated_at = datetime.now(timezone.utc)
        task.lease_worker_id = None
        task.lease_expires_at = None
        task.staging_path = None
        task.final_path = None
        task.action = None
        task.out_size = None

        # Log attempt
        now = datetime.now(timezone.utc)
        attempt = Attempt(
            task_id=task_id,
            worker_id=worker.id,
            started_at=now,
            finished_at=now,
            ok=0,
            err=req.err,
        )
        session.add(attempt)
        session.commit()

        logger.warning(
            "Task failed",
            extra={
                "task_id": task_id,
                "worker_id": worker.id,
                "route_id": task.route_id,
                "attempt": task.attempts,
                "status": task.status,
                "retryable": req.retryable,
                "error": req.err[:200],  # Truncate long errors
            },
        )

        return FailResponse(ok=True, message="Task marked as failed")

    @app.get("/health")
    def health():
        """Health check endpoint."""
        return {"status": "ok"}

    @app.get("/v1/admin/tasks", response_model=AdminListTasksResponse)
    def admin_list_tasks(
        status: Optional[str] = Query(default=None, max_length=32),
        route_id: Optional[str] = Query(default=None, max_length=255),
        src_path_prefix: Optional[str] = Query(default=None, max_length=2048),
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        session: Session = Depends(get_db),
        token: str = Depends(require_bootstrap_token),
    ):
        """List tasks for operational debugging (requires bootstrap token)."""
        _ = token
        q = session.query(Task).order_by(Task.updated_at.desc())
        if status:
            q = q.filter(Task.status == status)
        if route_id:
            q = q.filter(Task.route_id == route_id)
        if src_path_prefix:
            q = q.filter(Task.src_path.startswith(src_path_prefix))

        tasks = q.offset(offset).limit(limit).all()

        out: list[AdminTaskInfo] = []
        for t in tasks:
            out.append(
                AdminTaskInfo(
                    task_id=t.id,
                    route_id=t.route_id,
                    src_path=t.src_path,
                    src_rel=t.src_rel,
                    status=t.status,
                    attempts=int(t.attempts or 0),
                    max_attempts=int(t.max_attempts or 0),
                    lease_worker_id=int(t.lease_worker_id) if t.lease_worker_id is not None else None,
                    lease_expires_at=t.lease_expires_at.isoformat() if t.lease_expires_at else None,
                    action=t.action,
                    out_size=int(t.out_size) if t.out_size is not None else None,
                    staging_path=t.staging_path,
                    final_path=t.final_path,
                    last_error=(t.last_error[:200] if t.last_error else None),
                    created_at=t.created_at.isoformat(),
                    updated_at=t.updated_at.isoformat(),
                )
            )

        return AdminListTasksResponse(tasks=out)

    @app.post("/v1/admin/tasks/requeue", response_model=AdminRequeueResponse)
    def admin_requeue_tasks(
        req: AdminRequeueRequest,
        session: Session = Depends(get_db),
        token: str = Depends(require_bootstrap_token),
    ):
        """Requeue failed/deadletter tasks (requires bootstrap token)."""
        _ = token
        if not req.status_in:
            raise HTTPException(status_code=400, detail="status_in must not be empty")

        q = session.query(Task).filter(Task.status.in_(req.status_in))
        if req.route_id:
            q = q.filter(Task.route_id == req.route_id)
        if req.src_path_prefix:
            q = q.filter(Task.src_path.startswith(req.src_path_prefix))

        tasks = q.limit(req.limit).all()
        now = datetime.now(timezone.utc)
        updated = 0
        for t in tasks:
            if t.status == "finalized":
                continue
            t.status = "queued"
            if req.reset_attempts:
                t.attempts = 0
            t.last_error = None
            t.lease_worker_id = None
            t.lease_expires_at = None
            t.staging_path = None
            t.final_path = None
            t.action = None
            t.out_size = None
            t.updated_at = now
            updated += 1

        session.commit()
        return AdminRequeueResponse(ok=True, updated=updated)

    @app.post("/v1/admin/tasks/{task_id}/requeue", response_model=AdminRequeueResponse)
    def admin_requeue_task(
        task_id: str,
        reset_attempts: bool = True,
        session: Session = Depends(get_db),
        token: str = Depends(require_bootstrap_token),
    ):
        """Requeue a single task by id (requires bootstrap token)."""
        _ = token
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        if task.status == "finalized":
            raise HTTPException(status_code=409, detail="Task already finalized")

        now = datetime.now(timezone.utc)
        task.status = "queued"
        if reset_attempts:
            task.attempts = 0
        task.last_error = None
        task.lease_worker_id = None
        task.lease_expires_at = None
        task.staging_path = None
        task.final_path = None
        task.action = None
        task.out_size = None
        task.updated_at = now
        session.commit()

        return AdminRequeueResponse(ok=True, updated=1)

    return app


def main():
    """Main entry point for the server."""
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="shrink_media server")
    parser.add_argument("--scan-once", action="store_true", help="Scan routes once to generate tasks and exit (for debugging)")
    args = parser.parse_args()

    app = init_app()

    if args.scan_once:
        from .scanner import scan_all_routes

        print("Scanning routes once...")
        session = db.get_session()
        try:
            summary = scan_all_routes(config, openlist, session)
            print("\nScan complete:")
            for route_id, counts in summary.items():
                print(f"  {route_id}: created={counts['created']}, skipped={counts['skipped']}")
        finally:
            session.close()
        return

    uvicorn.run(app, host=config.host, port=config.port)


if __name__ == "__main__":
    main()
