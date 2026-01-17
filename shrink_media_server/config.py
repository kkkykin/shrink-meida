"""Configuration loading for shrink_media_server."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

import yaml


_MISSING = object()

_ALLOWED_TASK_KINDS = {"image", "video", "audio", "comic", "subtitle", "other"}
_ALLOWED_ROUTE_MODES = {"compress", "copy"}


@dataclass
class Route:
    """A route defines input/output roots for task generation."""
    id: str
    in_root: str
    out_root: str
    # Optional per-route scan interval override (seconds). When None, fall back to server.scan_interval_seconds.
    # 0 disables periodic scan for this route.
    scan_interval_seconds: Optional[int] = None
    # compress: worker downloads -> transcodes/copies -> uploads staging -> server finalizes
    # copy: server uses OpenList remote copy (no download/upload)
    mode: str = "compress"
    profile: Optional[dict] = None


@dataclass(frozen=True)
class BootstrapTokenScope:
    """Bootstrap token scope for workers (enforced when leasing tasks)."""

    allow_kinds: Optional[list[str]] = None
    allow_routes: Optional[list[str]] = None
    # Optional override for OpenList base URL used in worker capabilities
    # (download `/d?...sign=...` and direct-upload `upload_url`).
    openlist_base_url: Optional[str] = None


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
    scan_on_startup: bool
    scan_interval_seconds: int
    bootstrap_token_scopes: dict[str, BootstrapTokenScope] = field(default_factory=dict)

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

        # Worker tokens: env(WORKER_TOKEN_*/WORKER_TOKENS[/WORKER_TOKENS_SCOPES_JSON]) > yaml(bootstrap_tokens) > default
        bootstrap_tokens: list[str] = []
        bootstrap_token_scopes: dict[str, BootstrapTokenScope] = {}

        env_scopes_key: str | None = None
        if "WORKER_TOKENS_SCOPES_JSON" in os.environ:
            env_scopes_key = "WORKER_TOKENS_SCOPES_JSON"
        elif "WORKER_TOKEN_SCOPES_JSON" in os.environ:
            # Backward-compat alias (do not start new configs with WORKER_TOKEN_* to avoid collisions).
            env_scopes_key = "WORKER_TOKEN_SCOPES_JSON"
        env_scopes_present = env_scopes_key is not None
        scopes_from_env: dict[str, BootstrapTokenScope] = {}
        tokens_from_scopes: list[str] = []
        if env_scopes_present:
            scopes_from_env, tokens_from_scopes = cls._parse_token_scopes_json(
                os.environ.get(env_scopes_key) or "",
                context=env_scopes_key,
            )

        env_tokens_star: list[str] = []
        for k, v in sorted(os.environ.items(), key=lambda kv: kv[0]):
            if not k.startswith("WORKER_TOKEN_"):
                continue
            if k in {"WORKER_TOKEN_SCOPES_JSON"}:
                continue
            t = (v or "").strip()
            if t:
                env_tokens_star.append(t)

        env_tokens_csv_present = "WORKER_TOKENS" in os.environ
        env_tokens_csv: list[str] = []
        if not env_tokens_star and env_tokens_csv_present:
            env_tokens_csv = [t.strip() for t in (os.environ.get("WORKER_TOKENS") or "").split(",") if t.strip()]

        env_explicit = bool(env_tokens_star) or env_tokens_csv_present or env_scopes_present
        if env_explicit:
            if env_tokens_star:
                bootstrap_tokens = env_tokens_star
                bootstrap_token_scopes = {t: scopes_from_env[t] for t in bootstrap_tokens if t in scopes_from_env}
            elif env_tokens_csv_present:
                bootstrap_tokens = env_tokens_csv
                bootstrap_token_scopes = {t: scopes_from_env[t] for t in bootstrap_tokens if t in scopes_from_env}
            else:
                # scopes-only mode
                bootstrap_tokens = tokens_from_scopes
                bootstrap_token_scopes = scopes_from_env
        else:
            yaml_tokens = cls._pick_yaml(config_yaml, ("bootstrap_tokens",), ("worker", "bootstrap_tokens"))
            if yaml_tokens is not _MISSING:
                tokens_from_yaml, scopes_from_yaml = cls._parse_bootstrap_tokens_yaml(yaml_tokens)
                bootstrap_tokens = tokens_from_yaml
                bootstrap_token_scopes = scopes_from_yaml
            else:
                bootstrap_tokens = ["dev-token-001"]

        # De-dup, preserve order
        seen_tokens: set[str] = set()
        deduped: list[str] = []
        for t in bootstrap_tokens:
            t2 = (t or "").strip()
            if not t2 or t2 in seen_tokens:
                continue
            seen_tokens.add(t2)
            deduped.append(t2)
        bootstrap_tokens = deduped

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

        scan_on_startup_env = os.environ.get("SERVER_SCAN_ON_STARTUP")
        if scan_on_startup_env is not None:
            scan_on_startup = cls._coerce_bool(scan_on_startup_env)
        else:
            yaml_scan_on_startup = cls._pick_yaml(config_yaml, ("scan_on_startup",), ("server", "scan_on_startup"))
            scan_on_startup = cls._coerce_bool(yaml_scan_on_startup) if yaml_scan_on_startup is not _MISSING else True

        scan_interval_env = os.environ.get("SERVER_SCAN_INTERVAL_SECONDS")
        if scan_interval_env is not None:
            scan_interval_seconds = int(scan_interval_env)
        else:
            yaml_scan_interval = cls._pick_yaml(config_yaml, ("scan_interval_seconds",), ("server", "scan_interval_seconds"))
            scan_interval_seconds = cls._coerce_int(yaml_scan_interval) if yaml_scan_interval is not _MISSING else 300
        scan_interval_seconds = max(0, int(scan_interval_seconds))

        return cls(
            db_url=db_url,
            openlist_base_url=openlist_base_url,
            openlist_user=openlist_user,
            openlist_password=openlist_password,
            openlist_otp=openlist_otp,
            routes=routes,
            bootstrap_tokens=bootstrap_tokens,
            bootstrap_token_scopes=bootstrap_token_scopes,
            host=host,
            port=port,
            scan_on_startup=scan_on_startup,
            scan_interval_seconds=scan_interval_seconds,
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
    def _coerce_bool(v: object) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return v != 0
        if isinstance(v, str):
            s = v.strip().lower()
            if s in {"1", "true", "yes", "y", "on"}:
                return True
            if s in {"0", "false", "no", "n", "off", ""}:
                return False
        raise TypeError(f"expected bool, got {type(v).__name__}")

    @staticmethod
    def _coerce_optional_str_list(v: object) -> Optional[list[str]]:
        if v is None or v is _MISSING:
            return None
        if isinstance(v, list):
            out: list[str] = []
            for item in v:
                s = str(item).strip()
                if s:
                    out.append(s)
            return out
        if isinstance(v, str):
            parts = [p.strip() for p in v.split(",")]
            return [p for p in parts if p]
        raise TypeError(f"expected list[str] or comma-separated string, got {type(v).__name__}")

    @staticmethod
    def _validate_task_kinds(kinds: Optional[list[str]], *, context: str) -> None:
        if kinds is None:
            return
        bad = [k for k in kinds if k not in _ALLOWED_TASK_KINDS]
        if bad:
            bad_s = ", ".join(sorted(set(bad)))
            allowed_s = ", ".join(sorted(_ALLOWED_TASK_KINDS))
            raise ValueError(f"{context}: invalid allow_kinds: {bad_s} (allowed: {allowed_s})")

    @staticmethod
    def _parse_bootstrap_tokens_yaml(v: object) -> tuple[list[str], dict[str, BootstrapTokenScope]]:
        if isinstance(v, str):
            tokens = [t.strip() for t in v.split(",") if t.strip()]
            return tokens, {}
        if not isinstance(v, list):
            raise TypeError("server.yaml: bootstrap_tokens must be a list or comma-separated string")

        tokens: list[str] = []
        scopes: dict[str, BootstrapTokenScope] = {}

        for item in v:
            if isinstance(item, str):
                t = item.strip()
                if t:
                    tokens.append(t)
                continue
            if not isinstance(item, dict):
                raise TypeError("server.yaml: bootstrap_tokens items must be str or object")

            token_raw = item.get("token")
            if token_raw is None:
                raise TypeError("server.yaml: bootstrap_tokens object missing 'token'")
            token = str(token_raw).strip()
            if not token:
                raise TypeError("server.yaml: bootstrap_tokens object has empty 'token'")
            tokens.append(token)

            allow_kinds = ServerConfig._coerce_optional_str_list(item.get("allow_kinds", _MISSING))
            allow_routes = ServerConfig._coerce_optional_str_list(
                item.get("allow_routes", item.get("allow_route_ids", item.get("routes", item.get("route_ids", _MISSING))))
            )
            base_url_raw = item.get("base_url", item.get("openlist_base_url", _MISSING))
            if base_url_raw is _MISSING or base_url_raw is None:
                openlist_base_url_str: Optional[str] = None
            elif isinstance(base_url_raw, list):
                raise TypeError(f"server.yaml: bootstrap_tokens[{token!r}]: base_url must be a string, not a list")
            else:
                openlist_base_url_str = str(base_url_raw).strip()
                if not openlist_base_url_str:
                    raise ValueError(f"server.yaml: bootstrap_tokens[{token!r}]: base_url is empty")
                openlist_base_url_str = openlist_base_url_str.rstrip("/")
            ServerConfig._validate_task_kinds(allow_kinds, context=f"server.yaml: bootstrap_tokens[{token!r}]")
            if allow_kinds is None and allow_routes is None and openlist_base_url_str is None:
                continue
            scopes[token] = BootstrapTokenScope(
                allow_kinds=allow_kinds,
                allow_routes=allow_routes,
                openlist_base_url=openlist_base_url_str,
            )

        return tokens, scopes

    @staticmethod
    def _parse_token_scopes_json(raw: str, *, context: str) -> tuple[dict[str, BootstrapTokenScope], list[str]]:
        raw2 = (raw or "").strip()
        if not raw2:
            return {}, []
        data = json.loads(raw2)

        scopes: dict[str, BootstrapTokenScope] = {}
        tokens: list[str] = []

        if isinstance(data, dict):
            for token, spec in data.items():
                t = str(token).strip()
                if not t:
                    continue
                tokens.append(t)
                if spec is None:
                    continue
                if not isinstance(spec, dict):
                    raise TypeError(f"{context}: token spec must be an object")
                allow_kinds = ServerConfig._coerce_optional_str_list(spec.get("allow_kinds", _MISSING))
                allow_routes = ServerConfig._coerce_optional_str_list(
                    spec.get("allow_routes", spec.get("allow_route_ids", spec.get("routes", spec.get("route_ids", _MISSING))))
                )
                base_url_raw = spec.get("base_url", spec.get("openlist_base_url", _MISSING))
                if base_url_raw is _MISSING or base_url_raw is None:
                    openlist_base_url = None
                elif isinstance(base_url_raw, list):
                    raise TypeError(f"{context}[{t!r}]: base_url must be a string, not a list")
                else:
                    openlist_base_url = str(base_url_raw).strip()
                    if not openlist_base_url:
                        raise ValueError(f"{context}[{t!r}]: base_url is empty")
                    openlist_base_url = openlist_base_url.rstrip("/")
                ServerConfig._validate_task_kinds(allow_kinds, context=f"{context}[{t!r}]")
                if allow_kinds is None and allow_routes is None and openlist_base_url is None:
                    continue
                scopes[t] = BootstrapTokenScope(
                    allow_kinds=allow_kinds,
                    allow_routes=allow_routes,
                    openlist_base_url=openlist_base_url,
                )
            return scopes, tokens

        if isinstance(data, list):
            tokens, scopes = ServerConfig._parse_bootstrap_tokens_yaml(data)
            return scopes, tokens

        raise TypeError(f"{context}: expected object or list")

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
            mode_raw = item.get("mode", "compress")
            mode = str(mode_raw).strip().lower() if mode_raw is not None else "compress"
            if mode not in _ALLOWED_ROUTE_MODES:
                allowed = ", ".join(sorted(_ALLOWED_ROUTE_MODES))
                raise ValueError(f"routes: invalid mode {mode!r} (allowed: {allowed})")

            scan_interval_raw = item.get("scan_interval_seconds", _MISSING)
            if scan_interval_raw is _MISSING or scan_interval_raw is None:
                scan_interval_seconds: Optional[int] = None
            else:
                scan_interval_seconds = max(0, int(ServerConfig._coerce_int(scan_interval_raw)))

            routes.append(Route(
                id=item["id"],
                in_root=item["in_root"],
                out_root=item["out_root"],
                scan_interval_seconds=scan_interval_seconds,
                mode=mode,
                profile=item.get("profile"),
            ))
        return routes
