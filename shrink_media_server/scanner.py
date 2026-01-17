"""Scanner for creating tasks from input directories."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import PurePosixPath

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .config import ServerConfig
from .models import Attempt, Task
from .openlist import OpenListManager
from shrink_media.openlist_iter import iter_openlist_recursive


def process_copy_tasks(
    *,
    config: ServerConfig,
    openlist: OpenListManager,
    session: Session,
    limit: int = 1000,
) -> int:
    """
    Process tasks for routes configured with mode=copy.

    Copy tasks are executed on the server (OpenList remote copy), without worker download/upload.
    Returns number of tasks finalized in this run.
    """
    copy_routes = {r.id: r for r in config.routes if (r.mode or "compress").strip().lower() == "copy"}
    if not copy_routes:
        return 0

    now = datetime.now(timezone.utc)
    copy_route_ids = sorted(copy_routes.keys())
    q = (
        session.query(Task)
        .filter(
            Task.route_id.in_(copy_route_ids),
            (
                (Task.status == "queued")
                | (Task.status == "failed")
                | ((Task.status == "leased") & (Task.lease_expires_at < now))
            ),
            Task.attempts < Task.max_attempts,
        )
        .order_by(Task.updated_at.asc())
        .limit(max(1, int(limit)))
    )

    tasks = q.all()
    finalized = 0

    for task in tasks:
        route = copy_routes.get(task.route_id)
        if route is None:
            continue

        # Safety: if it's currently leased (and not expired), don't steal it.
        if task.status == "leased" and task.lease_expires_at is not None and task.lease_expires_at >= now:
            continue

        task.attempts += 1
        task.staging_path = None
        task.lease_worker_id = None
        task.lease_expires_at = None

        out_root_path = PurePosixPath(route.out_root.rstrip("/") or "/")
        final_path = str(out_root_path / PurePosixPath(task.src_rel))
        expected_size = int(task.src_size)

        ok = False
        err: str | None = None

        try:
            final_info = openlist.info(final_path)
            if final_info:
                final_size = int(getattr(final_info, "size", 0) or 0)
                if final_size == expected_size:
                    ok = True
                else:
                    err = f"final path exists with different size: {final_size} != {expected_size}"
            else:
                dst_dir = str(PurePosixPath(final_path).parent)
                openlist.ensure_dir(dst_dir)
                openlist.copy(task.src_path, dst_dir)

                final_info2 = openlist.info(final_path)
                if not final_info2:
                    err = "copied file not found"
                else:
                    final_size2 = int(getattr(final_info2, "size", 0) or 0)
                    if final_size2 != expected_size:
                        err = f"copied size mismatch: {final_size2} != {expected_size}"
                    else:
                        ok = True
        except FileNotFoundError as e:
            err = f"copy failed: {e}"
        except Exception as e:
            err = f"copy failed: {e}"

        task.final_path = final_path
        task.action = "copy"
        task.updated_at = now

        if ok:
            task.status = "finalized"
            task.out_size = expected_size
            task.last_error = None
            finalized += 1
        else:
            err_l = (err or "").lower()
            non_retryable = "final path exists with different size" in err_l
            source_missing = "not found" in err_l
            retryable = bool(err) and (not non_retryable) and (not source_missing)
            if not retryable:
                task.status = "deadletter"
            else:
                task.status = "queued" if task.attempts < int(task.max_attempts) else "deadletter"
            task.out_size = None
            task.last_error = err or "copy failed"

        session.add(
            Attempt(
                task_id=task.id,
                worker_id=None,
                started_at=now,
                finished_at=now,
                ok=1 if ok else 0,
                action="copy",
                err=None if ok else (err or "copy failed"),
                metrics_json=None,
            )
        )

    session.commit()
    return finalized


def scan_route(
    *,
    session: Session,
    openlist: OpenListManager,
    route_id: str,
    in_root: str,
    profile: dict,
) -> tuple[int, int]:
    """Scan a route and create tasks for new files.

    Returns (created, skipped) counts.
    """
    created = 0
    skipped = 0

    in_root_path = PurePosixPath(in_root.rstrip("/") or "/")

    for entry in iter_openlist_recursive(openlist.client, in_root):
        if entry.is_dir:
            continue

        # Calculate relative path
        entry_path = PurePosixPath(entry.path)
        try:
            src_rel = str(entry_path.relative_to(in_root_path))
        except ValueError:
            continue

        # Create task
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            route_id=route_id,
            src_path=entry.path,
            src_rel=src_rel,
            src_size=entry.size,
            src_mtime_ns=entry.mtime_ns,
            status="queued",
            profile_json=json.dumps(profile or {}, ensure_ascii=False, separators=(",", ":")),
        )

        try:
            session.add(task)
            session.commit()
            created += 1
        except IntegrityError:
            session.rollback()
            skipped += 1

    return created, skipped


def scan_all_routes(config: ServerConfig, openlist: OpenListManager, session: Session) -> dict:
    """Scan all routes and create tasks.

    Returns summary dict with counts per route.
    """
    summary = {}

    for route in config.routes:
        created, skipped = scan_route(
            session=session,
            openlist=openlist,
            route_id=route.id,
            in_root=route.in_root,
            profile=route.profile,
        )
        summary[route.id] = {"created": created, "skipped": skipped}

    # Execute copy-mode tasks on server (no worker involved).
    process_copy_tasks(config=config, openlist=openlist, session=session)

    return summary
