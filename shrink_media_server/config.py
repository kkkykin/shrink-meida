"""Configuration loading for shrink_media_server."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import yaml


_MISSING = object()


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
    # NOTE: bootstrap tokens are only used for worker registration (issuing per-worker tokens).
    bootstrap_tokens: List[str]

    # Server settings
    host: str
    port: int

    @classmethod
    def from_env(cls) -> ServerConfig:
        """Backward-compatible alias for load()."""
        return cls.load()

    @classmethod
    def load(cls, *, config_file: Path | None = None) -> ServerConfig:
        """Load configuration from YAML config file + environment overrides.

        Precedence (highest -> lowest):
        - env vars
        - YAML config file (default: ./server.yaml or ./server.yml; override via SERVER_CONFIG_FILE)
        - legacy local files (pass.txt, routes.json)
        - built-in defaults
        """

        config_path, is_explicit = cls._resolve_config_path(config_file=config_file)
        config_yaml: dict[str, Any] = {}
        if config_path is not None:
            if not config_path.exists():
                if is_explicit:
                    raise FileNotFoundError(f"Config file not found: {config_path}")
            else:
                config_yaml = cls._load_yaml_config(config_path)

        # Database
        db_url = os.environ.get("SERVER_DB_URL")
        if db_url is None:
            v = cls._pick_yaml(config_yaml, ("db_url",))
            db_url = cls._coerce_str(v) if v is not _MISSING else "sqlite:///shrink_media_server.db"

        # OpenList credentials: env > yaml > pass.txt > default
        pass_file = Path("pass.txt")
        pass_base_url = pass_user = pass_password = ""
        if pass_file.exists():
            pass_base_url, pass_user, pass_password = cls._load_pass_file(pass_file)

        yaml_openlist_base_url = cls._pick_yaml(config_yaml, ("openlist_base_url",), ("openlist", "base_url"))
        yaml_openlist_user = cls._pick_yaml(config_yaml, ("openlist_user",), ("openlist", "user"))
        yaml_openlist_password = cls._pick_yaml(
            config_yaml,
            ("openlist_password",),
            ("openlist", "password"),
            ("openlist", "pass"),
        )
        yaml_openlist_otp = cls._pick_yaml(
            config_yaml,
            ("openlist_otp",),
            ("openlist", "otp"),
        )

        openlist_base_url = os.environ.get("OPENLIST_BASE_URL")
        if openlist_base_url is None:
            if yaml_openlist_base_url is not _MISSING:
                openlist_base_url = cls._coerce_str(yaml_openlist_base_url)
            elif pass_base_url:
                openlist_base_url = pass_base_url
            else:
                openlist_base_url = "http://127.0.0.1:15244"

        openlist_user = os.environ.get("OPENLIST_USER")
        if openlist_user is None:
            if yaml_openlist_user is not _MISSING:
                openlist_user = cls._coerce_str(yaml_openlist_user)
            else:
                openlist_user = pass_user

        openlist_password = os.environ.get("OPENLIST_PASS")
        if openlist_password is None:
            if yaml_openlist_password is not _MISSING:
                openlist_password = cls._coerce_str(yaml_openlist_password)
            else:
                openlist_password = pass_password

        openlist_otp_env = os.environ.get("OPENLIST_OTP")
        if openlist_otp_env is not None:
            openlist_otp = openlist_otp_env
        elif yaml_openlist_otp is not _MISSING:
            openlist_otp = None if yaml_openlist_otp is None else cls._coerce_str(yaml_openlist_otp)
        else:
            openlist_otp = None

        # Routes: env(ROUTES_JSON) > yaml(routes) > legacy routes.json > default
        routes: List[Route]
        routes_json_env = os.environ.get("ROUTES_JSON")
        if routes_json_env is not None:
            routes = cls._parse_routes(json.loads(routes_json_env))
        else:
            yaml_routes = cls._pick_yaml(config_yaml, ("routes",))
            if yaml_routes is not _MISSING:
                if not isinstance(yaml_routes, list):
                    raise TypeError("server.yaml: routes must be a list")
                routes = cls._parse_routes(yaml_routes)
            else:
                routes_file = Path("routes.json")
                if routes_file.exists():
                    routes = cls._load_routes_file(routes_file)
                else:
                    routes = []

        # Worker tokens: env(WORKER_TOKEN_*/WORKER_TOKENS) > yaml(bootstrap_tokens) > default
        bootstrap_tokens: list[str] = []
        for k, v in sorted(os.environ.items(), key=lambda kv: kv[0]):
            if not k.startswith("WORKER_TOKEN_"):
                continue
            t = (v or "").strip()
            if t:
                bootstrap_tokens.append(t)
        if not bootstrap_tokens:
            worker_tokens_str = os.environ.get("WORKER_TOKENS")
            if worker_tokens_str is not None:
                bootstrap_tokens = [t.strip() for t in worker_tokens_str.split(",") if t.strip()]
            else:
                yaml_tokens = cls._pick_yaml(config_yaml, ("bootstrap_tokens",), ("worker", "bootstrap_tokens"))
                if yaml_tokens is not _MISSING:
                    if isinstance(yaml_tokens, list):
                        bootstrap_tokens = [str(t) for t in yaml_tokens]
                    elif isinstance(yaml_tokens, str):
                        bootstrap_tokens = [t.strip() for t in yaml_tokens.split(",") if t.strip()]
                    else:
                        raise TypeError("server.yaml: bootstrap_tokens must be a list[str] or comma-separated string")
                else:
                    bootstrap_tokens = ["dev-token-001"]

        # Server settings: env > yaml > defaults
        host = os.environ.get("SERVER_HOST")
        if host is None:
            yaml_host = cls._pick_yaml(config_yaml, ("host",), ("server", "host"))
            host = cls._coerce_str(yaml_host) if yaml_host is not _MISSING else "127.0.0.1"

        port_env = os.environ.get("SERVER_PORT")
        if port_env is not None:
            port = int(port_env)
        else:
            yaml_port = cls._pick_yaml(config_yaml, ("port",), ("server", "port"))
            port = cls._coerce_int(yaml_port) if yaml_port is not _MISSING else 8000

        return cls(
            db_url=db_url,
            openlist_base_url=openlist_base_url,
            openlist_user=openlist_user,
            openlist_password=openlist_password,
            openlist_otp=openlist_otp,
            routes=routes,
            bootstrap_tokens=bootstrap_tokens,
            host=host,
            port=port,
        )

    @staticmethod
    def _resolve_config_path(*, config_file: Path | None) -> tuple[Path | None, bool]:
        if config_file is not None:
            return config_file, True

        env_path = os.environ.get("SERVER_CONFIG_FILE")
        if env_path is not None:
            return Path(env_path), True

        for name in ("server.yaml", "server.yml"):
            p = Path(name)
            if p.exists():
                return p, False
        return None, False

    @staticmethod
    def _load_yaml_config(path: Path) -> dict[str, Any]:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if data is None:
            return {}
        if not isinstance(data, dict):
            raise TypeError("server.yaml must be a mapping/object at top level")
        return data

    @staticmethod
    def _coerce_str(v: object) -> str:
        if isinstance(v, str):
            return v
        return str(v)

    @staticmethod
    def _coerce_int(v: object) -> int:
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            return int(v.strip())
        raise TypeError(f"expected int, got {type(v).__name__}")

    @staticmethod
    def _yaml_get(data: dict[str, Any], path: tuple[str, ...]) -> object:
        cur: object = data
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                return _MISSING
            cur = cur[key]
        return cur

    @staticmethod
    def _pick_yaml(data: dict[str, Any], *paths: tuple[str, ...]) -> object:
        for p in paths:
            v = ServerConfig._yaml_get(data, p)
            if v is not _MISSING:
                return v
        return _MISSING

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
