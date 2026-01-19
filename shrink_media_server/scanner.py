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
    batch_size: int = 50,
) -> int:
    """
    Process tasks for routes configured with mode=copy.

    Copy tasks are executed on the server (OpenList remote copy), without worker download/upload.
    Returns number of tasks finalized in this run.
    """
    copy_routes = {r.id: r for r in config.routes if (r.mode or "compress").strip().lower() == "copy"}
    if not copy_routes:
        return 0

    batch_size = max(1, int(batch_size))
    copy_route_ids = sorted(copy_routes.keys())
    ensured_dirs: set[str] = set()

    finalized = 0
    run_started_at = datetime.now(timezone.utc)

    while True:
        now = datetime.now(timezone.utc)
        q = (
            session.query(
                Task.id,
                Task.route_id,
                Task.src_path,
                Task.src_rel,
                Task.src_size,
                Task.status,
                Task.lease_expires_at,
            )
            .filter(
                Task.route_id.in_(copy_route_ids),
                (
                    (Task.status == "queued")
                    | (Task.status == "failed")
                    | ((Task.status == "leased") & (Task.lease_expires_at < now))
                ),
                Task.attempts < Task.max_attempts,
                Task.updated_at <= run_started_at,
            )
            .order_by(Task.updated_at.asc())
            .limit(batch_size)
        )

        rows = q.all()
        session.rollback()
        if not rows:
            return finalized

        results: list[
            tuple[str, bool, str | None, Exception | None, str, int, datetime, datetime]
        ] = []

        for task_id, route_id, src_path, src_rel, src_size, status, lease_expires_at in rows:
            route = copy_routes.get(route_id)
            if route is None:
                continue

            # Safety: if it's currently leased (and not expired), don't steal it.
            if status == "leased" and lease_expires_at is not None and lease_expires_at >= now:
                continue

            out_root_path = PurePosixPath(route.out_root.rstrip("/") or "/")
            final_path = str(out_root_path / PurePosixPath(src_rel))
            expected_size = int(src_size)

            ok = False
            err: str | None = None
            exc: Exception | None = None
            started_at = datetime.now(timezone.utc)

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
                    if dst_dir not in ensured_dirs:
                        openlist.ensure_dir(dst_dir)
                        ensured_dirs.add(dst_dir)

                    try:
                        openlist.copy(src_path, dst_dir)
                    except FileExistsError:
                        final_info2 = openlist.info(final_path)
                        if final_info2:
                            final_size2 = int(getattr(final_info2, "size", 0) or 0)
                            if final_size2 == expected_size:
                                ok = True
                            else:
                                err = f"final path exists with different size: {final_size2} != {expected_size}"
                        else:
                            ok = True
                    else:
                        ok = True
            except FileNotFoundError as e:
                exc = e
                err = f"copy submit failed: {e}"
            except Exception as e:
                exc = e
                err = f"copy submit failed: {e}"

            finished_at = datetime.now(timezone.utc)
            results.append((task_id, ok, err, exc, final_path, expected_size, started_at, finished_at))

        if not results:
            continue

        task_ids = [task_id for task_id, *_rest in results]
        tasks = session.query(Task).filter(Task.id.in_(task_ids)).all()
        task_by_id = {t.id: t for t in tasks}
        attempts_to_add: list[Attempt] = []

        for task_id, ok, err, exc, final_path, expected_size, started_at, finished_at in results:
            task = task_by_id.get(task_id)
            if task is None:
                continue

            task.attempts += 1
            task.staging_path = None
            task.lease_worker_id = None
            task.lease_expires_at = None
            task.final_path = final_path
            task.action = "copy"
            task.updated_at = finished_at

            if ok:
                task.status = "finalized"
                task.out_size = expected_size
                task.last_error = None
                finalized += 1
            else:
                err_l = (err or "").lower()
                non_retryable = "final path exists with different size" in err_l
                source_missing = isinstance(exc, FileNotFoundError) or "not found" in err_l
                retryable = bool(err) and (not non_retryable) and (not source_missing)
                if not retryable:
                    task.status = "deadletter"
                else:
                    task.status = "queued" if task.attempts < int(task.max_attempts) else "deadletter"
                task.out_size = None
                task.last_error = err or "copy submit failed"

            attempts_to_add.append(
                Attempt(
                    task_id=task.id,
                    worker_id=None,
                    started_at=started_at,
                    finished_at=finished_at,
                    ok=1 if ok else 0,
                    action="copy",
                    err=None if ok else (err or "copy submit failed"),
                    metrics_json=None,
                ),
            )

        if attempts_to_add:
            session.add_all(attempts_to_add)

        session.commit()


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
    process_copy_tasks(config=config, openlist=openlist, session=session, batch_size=int(getattr(config, "copy_batch_size", 50)))

    return summary
