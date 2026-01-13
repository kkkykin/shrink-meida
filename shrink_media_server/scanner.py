"""Scanner for creating tasks from input directories."""
from __future__ import annotations

import json
import uuid
from pathlib import PurePosixPath

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .config import ServerConfig
from .models import Task
from .openlist import OpenListManager
from shrink_media.openlist_iter import iter_openlist_recursive


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
            profile_json=json.dumps(profile, ensure_ascii=False, separators=(",", ":")),
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
        print(f"Scanning route {route.id}: {route.in_root} -> {route.out_root}")
        created, skipped = scan_route(
            session=session,
            openlist=openlist,
            route_id=route.id,
            in_root=route.in_root,
            profile=route.profile,
        )
        summary[route.id] = {"created": created, "skipped": skipped}
        print(f"  Created: {created}, Skipped: {skipped}")

    return summary
