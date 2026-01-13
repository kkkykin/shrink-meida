"""Generate test tasks for shrink_media_server."""
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from shrink_media_server.config import ServerConfig
from shrink_media_server.models import Database, Task
from shrink_media_server.openlist import OpenListManager


def generate_test_tasks():
    """Generate test tasks from routes."""
    config = ServerConfig.from_env()
    db = Database(config.db_url)
    db.create_tables()

    openlist = OpenListManager(
        base_url=config.openlist_base_url,
        user=config.openlist_user,
        password=config.openlist_password,
        otp_key=config.openlist_otp,
    )

    session = db.get_session()

    try:
        # For each route, scan input directory and create tasks
        for route in config.routes:
            print(f"Scanning route: {route.id} ({route.in_root} -> {route.out_root})")

            try:
                # List files in input directory
                entries = openlist.client.list_recursive(route.in_root)
                print(f"  Found {len(entries)} files")

                for entry in entries:
                    if entry.is_dir:
                        continue

                    # Check if task already exists
                    existing = (
                        session.query(Task)
                        .filter(
                            Task.route_id == route.id,
                            Task.src_path == entry.path,
                            Task.src_size == entry.size,
                            Task.src_mtime_ns == entry.mtime_ns,
                        )
                        .first()
                    )

                    if existing:
                        print(f"  Skip (exists): {entry.rel}")
                        continue

                    # Create new task
                    task = Task(
                        id=str(uuid.uuid4()),
                        route_id=route.id,
                        src_path=entry.path,
                        src_rel=entry.rel,
                        src_size=entry.size,
                        src_mtime_ns=entry.mtime_ns,
                        status="queued",
                        profile_json="{}",
                        created_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                    )
                    session.add(task)
                    print(f"  Created task: {entry.rel}")

            except Exception as e:
                print(f"  Error scanning route: {e}")
                continue

        session.commit()
        print("\nTask generation complete!")

        # Print summary
        total_tasks = session.query(Task).count()
        queued_tasks = session.query(Task).filter(Task.status == "queued").count()
        print(f"Total tasks: {total_tasks}")
        print(f"Queued tasks: {queued_tasks}")

    finally:
        session.close()
        openlist.close()


if __name__ == "__main__":
    generate_test_tasks()
