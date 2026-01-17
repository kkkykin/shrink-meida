"""Database models for shrink_media_server."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import BigInteger, Column, DateTime, Integer, String, Text, UniqueConstraint, create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class Worker(Base):
    """Worker registration and capability tracking."""
    __tablename__ = "workers"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    token_hash = Column(String(64), nullable=False, unique=True)
    caps_json = Column(Text, nullable=False, default="{}")
    allow_kinds_json = Column(Text, nullable=True)
    allow_routes_json = Column(Text, nullable=True)
    # Optional override for OpenList base URL used in worker capabilities
    # (download `/d?...sign=...` and direct-upload `upload_url`).
    openlist_base_url = Column(String(2048), nullable=True)
    last_seen_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))


class Task(Base):
    """Task represents a single file to be transcoded."""
    __tablename__ = "tasks"
    __table_args__ = (
        UniqueConstraint("route_id", "src_path", "src_size", "src_mtime_ns", name="uq_task_srcver"),
    )

    id = Column(String(36), primary_key=True)  # UUID
    route_id = Column(String(255), nullable=False, index=True)
    src_path = Column(String(2048), nullable=False)
    src_rel = Column(String(2048), nullable=False)
    src_size = Column(BigInteger, nullable=False)
    src_mtime_ns = Column(BigInteger, nullable=False)

    # Status: queued, leased, uploaded_to_staging, finalized, failed, deadletter
    status = Column(String(32), nullable=False, default="queued", index=True)

    # Lease management
    lease_worker_id = Column(Integer, nullable=True)
    lease_expires_at = Column(DateTime, nullable=True)

    # Retry management
    attempts = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=3)

    # Task configuration
    profile_json = Column(Text, nullable=False, default="{}")

    # Output tracking
    staging_path = Column(String(2048), nullable=True)
    final_path = Column(String(2048), nullable=True)
    action = Column(String(16), nullable=True)  # ok, copy, skip
    out_size = Column(BigInteger, nullable=True)

    # Error tracking
    last_error = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class Attempt(Base):
    """Audit log for task attempts."""
    __tablename__ = "attempts"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(36), nullable=False, index=True)
    worker_id = Column(Integer, nullable=True)
    started_at = Column(DateTime, nullable=False)
    finished_at = Column(DateTime, nullable=True)
    ok = Column(Integer, nullable=False, default=0)  # 0=failed, 1=success
    action = Column(String(16), nullable=True)
    err = Column(Text, nullable=True)
    metrics_json = Column(Text, nullable=True)


class Database:
    """Database connection and session management."""

    def __init__(self, db_url: str):
        url = make_url(db_url)
        connect_args = {}
        if url.get_backend_name() == "sqlite":
            connect_args["check_same_thread"] = False
        self.engine = create_engine(db_url, echo=False, connect_args=connect_args)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Create all tables if they don't exist."""
        Base.metadata.create_all(self.engine)
        self._migrate()

    def _migrate(self) -> None:
        """Best-effort migrations for SQLite (add new columns if missing)."""
        if self.engine.dialect.name != "sqlite":
            return
        with self.engine.begin() as conn:
            cols = conn.execute(text("PRAGMA table_info(workers)")).fetchall()
            col_names = {str(r[1]) for r in cols}  # (cid, name, type, notnull, dflt_value, pk)
            if "allow_kinds_json" not in col_names:
                conn.execute(text("ALTER TABLE workers ADD COLUMN allow_kinds_json TEXT"))
            if "allow_routes_json" not in col_names:
                conn.execute(text("ALTER TABLE workers ADD COLUMN allow_routes_json TEXT"))
            if "openlist_base_url" not in col_names:
                conn.execute(text("ALTER TABLE workers ADD COLUMN openlist_base_url TEXT"))

    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()
