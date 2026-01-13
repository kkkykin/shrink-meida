"""Manually create test tasks."""
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from shrink_media_server.config import ServerConfig
from shrink_media_server.models import Database, Task


def create_test_tasks():
    """Create test tasks manually."""
    config = ServerConfig.from_env()
    db = Database(config.db_url)
    db.create_tables()

    session = db.get_session()

    try:
        # Create test tasks for route from1
        test_files = [
            {"rel": "sample.mp4", "size": 439614, "path": "/test/from1/sample.mp4"},
            {"rel": "sample.mp3", "size": 17051, "path": "/test/from1/sample.mp3"},
            {"rel": "photo.jpg", "size": 38380, "path": "/test/from1/photo.jpg"},
        ]

        for file_info in test_files:
            task = Task(
                id=str(uuid.uuid4()),
                route_id="from1",
                src_path=file_info["path"],
                src_rel=file_info["rel"],
                src_size=file_info["size"],
                src_mtime_ns=int(datetime.now(timezone.utc).timestamp() * 1_000_000_000),
                status="queued",
                profile_json="{}",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            session.add(task)
            print(f"Created task: {file_info['rel']}")

        session.commit()
        print("\nTask creation complete!")

        # Print summary
        total_tasks = session.query(Task).count()
        queued_tasks = session.query(Task).filter(Task.status == "queued").count()
        print(f"Total tasks: {total_tasks}")
        print(f"Queued tasks: {queued_tasks}")

    finally:
        session.close()


if __name__ == "__main__":
    create_test_tasks()
