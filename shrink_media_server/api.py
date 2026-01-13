"""FastAPI server for shrink_media_server."""
from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Header
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .config import ServerConfig
from .models import Attempt, Database, Task, Worker
from .openlist import OpenListManager


# Request/Response models
class RegisterRequest(BaseModel):
    name: str
    caps: dict


class RegisterResponse(BaseModel):
    worker_id: int
    worker_token: str


class LeaseRequest(BaseModel):
    worker_id: int
    n: int = 1


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
    worker_id: int


class HeartbeatResponse(BaseModel):
    lease_expires_at: str


class UploadIntentRequest(BaseModel):
    worker_id: int
    out_size: int
    out_ext: str
    action: str


class UploadIntentResponse(BaseModel):
    staging_path: str
    upload: dict


class CompleteRequest(BaseModel):
    worker_id: int
    staging_path: str
    action: str
    out_size: int
    metrics: Optional[dict] = None


class CompleteResponse(BaseModel):
    ok: bool
    message: str


class FailRequest(BaseModel):
    worker_id: int
    err: str
    retryable: bool = True


class FailResponse(BaseModel):
    ok: bool
    message: str


# Global state
config: ServerConfig
db: Database
openlist: OpenListManager


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

    # Dependency: Verify worker token
    def verify_token(authorization: Optional[str] = Header(None)) -> str:
        if not authorization:
            raise HTTPException(status_code=401, detail="Missing authorization header")
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid authorization header")
        token = authorization[7:]
        if token not in config.worker_tokens:
            raise HTTPException(status_code=401, detail="Invalid token")
        return token

    @app.post("/v1/workers/register", response_model=RegisterResponse)
    def register_worker(
        req: RegisterRequest,
        session: Session = Depends(get_db),
        token: str = Depends(verify_token),
    ):
        """Register a new worker."""
        # Generate worker token
        worker_token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(worker_token.encode()).hexdigest()

        # Create worker
        worker = Worker(
            name=req.name,
            token_hash=token_hash,
            caps_json=str(req.caps),
            last_seen_at=datetime.now(timezone.utc),
        )
        session.add(worker)
        session.commit()
        session.refresh(worker)

        return RegisterResponse(worker_id=worker.id, worker_token=worker_token)

    @app.post("/v1/tasks/lease", response_model=LeaseResponse)
    def lease_tasks(
        req: LeaseRequest,
        session: Session = Depends(get_db),
        token: str = Depends(verify_token),
    ):
        """Lease tasks for a worker."""
        # Verify worker exists
        worker = session.query(Worker).filter(Worker.id == req.worker_id).first()
        if not worker:
            raise HTTPException(status_code=404, detail="Worker not found")

        # Update worker last_seen_at
        worker.last_seen_at = datetime.now(timezone.utc)

        # Find available tasks (queued or expired leases)
        now = datetime.now(timezone.utc)
        tasks = (
            session.query(Task)
            .filter(
                (Task.status == "queued")
                | ((Task.status == "leased") & (Task.lease_expires_at < now))
            )
            .limit(req.n)
            .all()
        )

        # Lease tasks
        lease_expires_at = now + timedelta(minutes=10)
        leased_tasks = []

        for task in tasks:
            task.status = "leased"
            task.lease_worker_id = req.worker_id
            task.lease_expires_at = lease_expires_at
            task.attempts += 1
            task.updated_at = now

            # Get download URL
            download = openlist.get_download_url(task.src_path)

            # Parse profile
            import json
            profile = json.loads(task.profile_json) if task.profile_json else {}

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
        token: str = Depends(verify_token),
    ):
        """Renew task lease."""
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if task.lease_worker_id != req.worker_id:
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
        token: str = Depends(verify_token),
    ):
        """Get upload capability for task output."""
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if task.lease_worker_id != req.worker_id:
            raise HTTPException(status_code=403, detail="Task not leased by this worker")

        # Find route
        route = next((r for r in config.routes if r.id == task.route_id), None)
        if not route:
            raise HTTPException(status_code=500, detail="Route not found")

        # Generate staging path
        nonce = secrets.token_hex(8)
        staging_path = f"{route.out_root}/.shrink_media_staging/{task_id}/{nonce}/blob"

        # Ensure staging directory exists
        import posixpath
        staging_dir = posixpath.dirname(staging_path)
        openlist.ensure_dir(staging_dir)

        # Get direct upload info
        upload_info = openlist.get_direct_upload_info(staging_path, req.out_size)
        if not upload_info:
            # Fallback: provide proxy upload URL
            upload_info = {
                "url": f"/v1/tasks/{task_id}/upload_proxy",
                "method": "PUT",
                "chunk_size": 5 * 1024 * 1024,
                "headers": {},
            }

        # Update task
        task.staging_path = staging_path
        task.updated_at = datetime.now(timezone.utc)
        session.commit()

        return UploadIntentResponse(staging_path=staging_path, upload=upload_info)

    @app.post("/v1/tasks/{task_id}/complete", response_model=CompleteResponse)
    def complete_task(
        task_id: str,
        req: CompleteRequest,
        session: Session = Depends(get_db),
        token: str = Depends(verify_token),
    ):
        """Mark task as complete and finalize output."""
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if task.lease_worker_id != req.worker_id:
            raise HTTPException(status_code=403, detail="Task not leased by this worker")

        # Find route
        route = next((r for r in config.routes if r.id == task.route_id), None)
        if not route:
            raise HTTPException(status_code=500, detail="Route not found")

        # Calculate final path using suffix naming
        import posixpath
        from pathlib import Path

        src_stem = Path(task.src_rel).stem
        src_ext = "".join(Path(task.src_rel).suffixes)
        final_name = f"{src_stem}__{src_ext}{req.out_ext}"
        final_rel = posixpath.join(posixpath.dirname(task.src_rel), final_name)
        final_path = posixpath.join(route.out_root, final_rel)

        # Finalize staging to final
        result = openlist.finalize(req.staging_path, final_path, req.out_size)

        if result["ok"]:
            task.status = "finalized"
            task.final_path = final_path
            task.action = req.action
            task.out_size = req.out_size
            task.last_error = None
        else:
            task.status = "failed"
            task.last_error = result["error"]

        task.updated_at = datetime.now(timezone.utc)

        # Log attempt
        attempt = Attempt(
            task_id=task_id,
            worker_id=req.worker_id,
            started_at=task.lease_expires_at - timedelta(minutes=10) if task.lease_expires_at else datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            ok=1 if result["ok"] else 0,
            action=req.action,
            err=result["error"],
            metrics_json=str(req.metrics) if req.metrics else None,
        )
        session.add(attempt)
        session.commit()

        return CompleteResponse(
            ok=result["ok"],
            message=result["error"] or "Task completed successfully",
        )

    @app.post("/v1/tasks/{task_id}/fail", response_model=FailResponse)
    def fail_task(
        task_id: str,
        req: FailRequest,
        session: Session = Depends(get_db),
        token: str = Depends(verify_token),
    ):
        """Mark task as failed."""
        task = session.query(Task).filter(Task.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if task.lease_worker_id != req.worker_id:
            raise HTTPException(status_code=403, detail="Task not leased by this worker")

        # Update task status
        if req.retryable and task.attempts < task.max_attempts:
            task.status = "queued"  # Back to queue for retry
        else:
            task.status = "deadletter"

        task.last_error = req.err
        task.updated_at = datetime.now(timezone.utc)

        # Log attempt
        attempt = Attempt(
            task_id=task_id,
            worker_id=req.worker_id,
            started_at=task.lease_expires_at - timedelta(minutes=10) if task.lease_expires_at else datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            ok=0,
            err=req.err,
        )
        session.add(attempt)
        session.commit()

        return FailResponse(ok=True, message="Task marked as failed")

    @app.get("/health")
    def health():
        """Health check endpoint."""
        return {"status": "ok"}

    return app


def main():
    """Main entry point for the server."""
    import uvicorn

    app = init_app()
    uvicorn.run(app, host=config.host, port=config.port)


if __name__ == "__main__":
    main()
