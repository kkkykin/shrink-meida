"""Configuration loading for shrink_media_server."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class Route:
    """A route defines input/output roots for task generation."""
    id: str
    in_root: str
    out_root: str
    profile: Optional[dict] = None


@dataclass
class ServerConfig:
    """Server configuration loaded from environment variables."""
    # Database
    db_url: str

    # OpenList credentials (server-only)
    openlist_base_url: str
    openlist_user: str
    openlist_password: str
    openlist_otp: Optional[str]

    # Routes configuration
    routes: List[Route]

    # Worker authentication
    worker_tokens: List[str]

    # Server settings
    host: str
    port: int

    @classmethod
    def from_env(cls) -> ServerConfig:
        """Load configuration from environment variables and files."""
        # Database
        db_url = os.getenv("SERVER_DB_URL", "sqlite:///shrink_media_server.db")

        # OpenList credentials - try pass.txt first, then env vars
        pass_file = Path("pass.txt")
        if pass_file.exists():
            openlist_base_url, openlist_user, openlist_password = cls._load_pass_file(pass_file)
        else:
            openlist_base_url = os.getenv("OPENLIST_BASE_URL", "http://127.0.0.1:15244")
            openlist_user = os.getenv("OPENLIST_USER", "")
            openlist_password = os.getenv("OPENLIST_PASS", "")

        openlist_otp = os.getenv("OPENLIST_OTP")

        # Routes - try routes.json first, then env var
        routes_file = Path("routes.json")
        if routes_file.exists():
            routes = cls._load_routes_file(routes_file)
        else:
            routes_json = os.getenv("ROUTES_JSON", "[]")
            routes = cls._parse_routes(json.loads(routes_json))

        # Worker tokens
        worker_tokens_str = os.getenv("WORKER_TOKENS", "dev-token-001")
        worker_tokens = [t.strip() for t in worker_tokens_str.split(",") if t.strip()]

        # Server settings
        host = os.getenv("SERVER_HOST", "127.0.0.1")
        port = int(os.getenv("SERVER_PORT", "8000"))

        return cls(
            db_url=db_url,
            openlist_base_url=openlist_base_url,
            openlist_user=openlist_user,
            openlist_password=openlist_password,
            openlist_otp=openlist_otp,
            routes=routes,
            worker_tokens=worker_tokens,
            host=host,
            port=port,
        )

    @staticmethod
    def _load_pass_file(path: Path) -> tuple[str, str, str]:
        """Parse pass.txt file for OpenList credentials."""
        content = path.read_text()
        base_url = ""
        user = ""
        password = ""

        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key == "base_url":
                    base_url = value
                elif key == "user":
                    user = value
                elif key == "pass":
                    password = value

        return base_url, user, password

    @staticmethod
    def _load_routes_file(path: Path) -> List[Route]:
        """Load routes from routes.json file."""
        data = json.loads(path.read_text())
        return ServerConfig._parse_routes(data)

    @staticmethod
    def _parse_routes(data: list) -> List[Route]:
        """Parse routes from JSON data."""
        routes = []
        for item in data:
            routes.append(Route(
                id=item["id"],
                in_root=item["in_root"],
                out_root=item["out_root"],
                profile=item.get("profile"),
            ))
        return routes
