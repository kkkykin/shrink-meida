#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "httpx>=0.27",
#   "openlist",
#   "tenacity>=8.2",
#   "rich>=13.7",
# ]
# [[tool.uv.index]]
# url = "https://mirrors.ustc.edu.cn/pypi/simple/"
# ///

"""
shrink-media: 多媒体“瘦身”工具，支持本地 / OpenList 远端输入输出 + 多设备协同。

特点:
- 视频/音频/图片/漫画压缩包处理，体积不够小则回退复制
- OpenList 递归遍历、分片上传，远端 state/lock，跨设备安全
- 可选多线程；dry-run 预览；NVENC 优先，回退 x265/x264；opus/aac 音频；webp/avif 图片

设计思路：
- 单文件为主，远端使用 openlist 客户端
- 结构化模块：配置、远端 IO、状态锁、分类与转码、漫画处理、执行器
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures as cf
import hashlib
import itertools
import json
import logging
import os
import platform
import posixpath
import queue
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlparse

import httpx
from openlist import Client
from openlist.core.base import BaseService
from openlist.exceptions import AuthenticationFailed, BadResponse, UnexceptedResponseCode
from rich.console import Console
from rich.filesize import decimal as format_bytes
from rich.logging import RichHandler
from tenacity import Retrying, RetryCallState, retry_if_exception, stop_after_attempt

# ------------------------
# 常量/扩展名
# ------------------------

VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv", ".wmv", ".m4v", ".ts", ".m2ts", ".mts", ".3gp", ".3g2"}
AUDIO_EXTS = {".mp3", ".aac", ".m4a", ".wav", ".flac", ".ogg", ".opus", ".wma", ".alac"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff", ".bmp", ".heic", ".heif", ".avif"}
ANIMATED_IMAGE_EXTS = {".gif", ".apng"}
SUB_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub"}

COMIC_EXTS = {".cbz", ".cbr", ".cb7"}
ARCHIVE_EXTS = {".zip", ".7z", ".rar", ".tar", ".gz", ".bz2", ".xz", ".zst", ".tgz", ".tbz2", ".txz"}

LOSSY_AUDIO_CODECS = {"aac", "mp3", "opus", "vorbis", "ac3", "eac3", "dts", "mp2", "wma"}
TEXT_SUB_CODECS = {"subrip", "srt", "ass", "ssa", "mov_text", "webvtt"}
BITMAP_SUB_CODECS = {"hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle", "xsub", "vobsub"}

STATE_DEFAULT_NAME = ".shrink_media_state.jsonl"
STATE_DIR_NAME = ".shrink_media_state"
LOCKS_DIR_NAME = ".shrink_media_locks"

# ------------------------
# 简易日志
# ------------------------

_LOGGER = logging.getLogger("shrink_media")
_DEBUG_ENABLED = False
_PREFETCH_DEBUG_ENABLED = False


class _LevelRangeFilter(logging.Filter):
    def __init__(self, *, min_level: int | None = None, max_level: int | None = None) -> None:
        super().__init__()
        self._min_level = min_level
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        if self._min_level is not None and record.levelno < self._min_level:
            return False
        if self._max_level is not None and record.levelno > self._max_level:
            return False
        return True


def configure_logging(log_file: Optional[str], *, append: bool, debug: bool, prefetch_debug: bool) -> None:
    global _DEBUG_ENABLED, _PREFETCH_DEBUG_ENABLED
    _DEBUG_ENABLED = bool(debug) or bool(log_file)
    _PREFETCH_DEBUG_ENABLED = bool(prefetch_debug)

    any_debug_console = bool(debug) or bool(prefetch_debug)
    _LOGGER.setLevel(logging.DEBUG)
    _LOGGER.propagate = False
    _LOGGER.handlers.clear()

    level = logging.DEBUG if any_debug_console else logging.INFO

    stdout_console = Console(stderr=False)
    stderr_console = Console(stderr=True)

    stdout_handler = RichHandler(
        console=stdout_console,
        show_time=True,
        show_level=True,
        show_path=False,
        rich_tracebacks=any_debug_console,
        markup=False,
    )
    stdout_handler.setLevel(level)
    stdout_handler.addFilter(_LevelRangeFilter(max_level=logging.INFO))
    _LOGGER.addHandler(stdout_handler)

    stderr_handler = RichHandler(
        console=stderr_console,
        show_time=True,
        show_level=True,
        show_path=False,
        rich_tracebacks=any_debug_console,
        markup=False,
    )
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.addFilter(_LevelRangeFilter(min_level=logging.WARNING))
    _LOGGER.addHandler(stderr_handler)

    if log_file:
        p = Path(log_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if append else "w"
        try:
            fh = logging.FileHandler(p, mode=mode, encoding="utf-8")
        except Exception as e:
            print(f"ERROR: cannot open --log-file {p}: {e}", file=sys.stderr, flush=True)
            sys.exit(2)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        _LOGGER.addHandler(fh)


def log(msg: str) -> None:
    _LOGGER.info(msg)


def log_err(msg: str) -> None:
    _LOGGER.error(msg)


# ------------------------
# 杂项工具
# ------------------------

def is_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")


def ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def tail_text(s: str, n_lines: int = 40, max_chars: int = 6000) -> str:
    if not s:
        return ""
    lines = s.splitlines()
    tail = "\n".join(lines[-n_lines:])
    return tail[-max_chars:]


def describe_returncode(rc: int) -> str:
    if rc >= 0:
        return f"exit={rc}"
    sig = -rc
    try:
        name = signal.Signals(sig).name
    except Exception:
        name = f"SIG{sig}"
    return f"killed by signal {sig} ({name})"


def extract_result_from_ffmpeg_err(err: str) -> Optional[str]:
    for line in (err or "").splitlines():
        line = line.strip()
        if not line.startswith("result:"):
            continue
        return line[len("result:") :].strip()
    return None


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def size_of(p: Path) -> int:
    try:
        return p.stat().st_size
    except Exception:
        return -1


def fmt_bytes(n: int) -> str:
    if n < 0:
        return "n/a"
    return format_bytes(n)


def fmt_size_change(src_sz: int, out_sz: int) -> str:
    if src_sz > 0 and out_sz > 0:
        pct = (out_sz - src_sz) * 100.0 / src_sz
        return f"{fmt_bytes(src_sz)} -> {fmt_bytes(out_sz)} ({pct:+.1f}%)"
    return f"{src_sz}->{out_sz}"


def looks_like_archive_name(name: str) -> bool:
    name = name.lower()
    ext = Path(name).suffix.lower()
    if ext in ARCHIVE_EXTS:
        return True
    for multi in (".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst"):
        if name.endswith(multi):
            return True
    return False


def should_ignore_name(name: str) -> bool:
    if name in {STATE_DEFAULT_NAME, STATE_DIR_NAME, LOCKS_DIR_NAME}:
        return True
    if ".__tmp__" in name:
        return True
    return False


def find_7z() -> Optional[str]:
    for n in ("7zz", "7z"):
        p = shutil.which(n)
        if p:
            return n
    return None


# ------------------------
# OpenList 辅助
# ------------------------


def patch_openlist_code_401_as_auth_failed() -> None:
    """
    OpenList 服务端有时会用 HTTP 200 + JSON {"code": 401, ...} 表示未认证。
    openlist 客户端默认会把它当作 BadResponse，从而丢失 code 信息。
    这里把 code=401 统一提升为 AuthenticationFailed，便于上层自动重新登录。
    """
    if getattr(BaseService, "_shrink_media_patched_code401", False):
        return

    async def _request(  # type: ignore[override]
        self: Any,
        method: str,
        endpoint: str,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
        require_auth: bool = True,
        expected_codes: tuple[int, ...] = (200,),
    ) -> dict:
        headers: Dict[str, str] = {}
        if require_auth and getattr(self.context, "auth_token", None):
            headers["Authorization"] = self.context.auth_token

        request_kwargs: Dict[str, Any] = {"headers": headers}
        if json is not None:
            request_kwargs["json"] = json
        if params is not None:
            request_kwargs["params"] = params

        http_method = getattr(self.context.httpx_client, method.lower())
        response: httpx.Response = await http_method(endpoint, **request_kwargs)

        if response.status_code == 401:
            raise AuthenticationFailed("Unauthorized")
        if response.status_code == 403:
            raise AuthenticationFailed(response.json().get("message", "Forbidden"))
        if response.status_code not in expected_codes:
            raise UnexceptedResponseCode(response.status_code, response.json().get("message", "Unknown error"))

        try:
            data = response.json()
        except Exception:
            raise BadResponse("Invalid JSON response")

        # 关键差异：把 code=401 当作“未认证”
        if data.get("code") == 401:
            raise AuthenticationFailed(data.get("message", "Unauthorized"))

        if data.get("code") != 200:
            raise BadResponse(data.get("message", "Unknown error"))

        return data

    BaseService._request = _request  # type: ignore[method-assign]
    setattr(BaseService, "_shrink_media_patched_code401", True)


class FatalAuthError(Exception):
    pass


class HttpStatusError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(f"http {status_code}: {message}")
        self.status_code = status_code


def parse_remote_location(u: str) -> Tuple[str, str]:
    pu = urlparse(u)
    base = f"{pu.scheme}://{pu.netloc}"
    path = pu.path or "/"
    if not path.startswith("/"):
        path = "/" + path
    return base.rstrip("/"), path


def remote_join(root: str, rel: str) -> str:
    root_clean = root.rstrip("/")
    rel_clean = posixpath.normpath(rel).lstrip("/")
    if root_clean == "":
        return "/" + rel_clean if rel_clean else "/"
    return f"{root_clean}/{rel_clean}" if rel_clean else (root_clean + "/")


def _mtime_to_ns(dt: Any) -> int:
    try:
        return int(dt.timestamp() * 1_000_000_000)
    except Exception:
        return 0


@dataclass
class RemoteEntry:
    rel: str
    path: str
    is_dir: bool
    size: int
    mtime_ns: int
    sign: str = ""


class OpenListClientSync:
    """同步包装，便于在现有同步代码里调用 OpenList 异步客户端。"""

    def __init__(
        self,
        base_url: str,
        user: str,
        password: str,
        *,
        timeout: int,
        otp_key: Optional[str] = None,
        retries: int = 3,
        retry_backoff: float = 0.5,
    ) -> None:
        patch_openlist_code_401_as_auth_failed()
        self.base_url = base_url.rstrip("/")
        self._user = user
        self._password = password
        self._otp_key = otp_key
        self._retries = max(0, int(retries))
        self._retry_backoff = max(0.0, float(retry_backoff))
        self._login_mu = threading.Lock()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()
        self._client: Optional[Client] = None
        self._pending_mu = threading.Lock()
        self._pending: set[cf.Future[Any]] = set()
        self._call(self._init_client(timeout))
        assert self._client is not None
        self._login_or_die(reason="initial login")

    def _start_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _call(self, coro: Any) -> Any:
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        with self._pending_mu:
            self._pending.add(fut)
        try:
            return fut.result()
        finally:
            with self._pending_mu:
                self._pending.discard(fut)

    def _retry_wait(self, retry_state: RetryCallState) -> float:
        if self._retry_backoff <= 0:
            return 0.0
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, AuthenticationFailed):
            # token 失效：relogin 后立即重试，不额外退避
            return 0.0
        attempt = int(getattr(retry_state, "attempt_number", 1) or 1)
        attempt = max(1, attempt)
        delay = self._retry_backoff * (2 ** max(0, attempt - 1))
        return min(delay, 30.0)

    def _maybe_status_code(self, e: Exception) -> Optional[int]:
        if isinstance(e, HttpStatusError):
            return e.status_code
        if isinstance(e, UnexceptedResponseCode):
            m = re.search(r"Unexpected response code:\\s*(\\d+)", str(e))
            if m:
                try:
                    return int(m.group(1))
                except ValueError:
                    return None
        return None

    def _is_retryable(self, e: Exception) -> bool:
        if isinstance(e, (httpx.TimeoutException, httpx.TransportError)):
            return True
        code = self._maybe_status_code(e)
        if code is None:
            return False
        return code >= 500 or code in {408, 425, 429}

    def _login_or_die(self, *, reason: str) -> None:
        assert self._client is not None
        attempts = 1 + self._retries

        def _should_retry(e: BaseException) -> bool:
            return isinstance(e, Exception) and (not isinstance(e, AuthenticationFailed)) and self._is_retryable(e)

        def _before_sleep(rs: RetryCallState) -> None:
            if not (_DEBUG_ENABLED or _PREFETCH_DEBUG_ENABLED):
                return
            exc = rs.outcome.exception() if rs.outcome else None
            sleep = float(getattr(getattr(rs, "next_action", None), "sleep", 0.0) or 0.0)
            _LOGGER.debug("retry login attempt=%d/%d sleep=%.2fs err=%r", rs.attempt_number, attempts, sleep, exc)

        try:
            retrying = Retrying(
                stop=stop_after_attempt(attempts),
                retry=retry_if_exception(_should_retry),
                wait=self._retry_wait,
                before_sleep=_before_sleep,
                reraise=True,
            )
            for attempt in retrying:
                with attempt:
                    self._call(self._client.login(self._user, self._password, otp_key=self._otp_key))
                    return
        except AuthenticationFailed as e:
            raise FatalAuthError(f"OpenList login failed ({reason}): {e}") from e
        except Exception as e:
            raise FatalAuthError(f"OpenList login failed ({reason}): {e}") from e

    def _relogin_or_die(self, *, reason: str) -> None:
        with self._login_mu:
            # 最常见场景：token 失效/服务端重启 -> 重新登录恢复
            self._login_or_die(reason=reason)

    def _call_retry(self, make_coro: Callable[[], Any], *, op: str) -> Any:
        attempts = 1 + self._retries
        did_relogin = False

        def _should_retry(e: BaseException) -> bool:
            nonlocal did_relogin
            if isinstance(e, AuthenticationFailed):
                if did_relogin:
                    return False
                did_relogin = True
                self._relogin_or_die(reason=f"{op}: {e}")
                return True
            return isinstance(e, Exception) and self._is_retryable(e)

        def _before_sleep(rs: RetryCallState) -> None:
            if not (_DEBUG_ENABLED or _PREFETCH_DEBUG_ENABLED):
                return
            exc = rs.outcome.exception() if rs.outcome else None
            sleep = float(getattr(getattr(rs, "next_action", None), "sleep", 0.0) or 0.0)
            _LOGGER.debug("retry %s attempt=%d/%d sleep=%.2fs err=%r", op, rs.attempt_number, attempts, sleep, exc)

        retrying = Retrying(
            stop=stop_after_attempt(attempts),
            retry=retry_if_exception(_should_retry),
            wait=self._retry_wait,
            before_sleep=_before_sleep,
            reraise=True,
        )
        for attempt in retrying:
            with attempt:
                return self._call(make_coro())
        raise AssertionError("unreachable")

    async def _init_client(self, timeout: int) -> None:
        self._client = Client(self.base_url, auto_refresh=True)
        self._client.context.httpx_client = httpx.AsyncClient(
            base_url=self.base_url, follow_redirects=True, timeout=timeout
        )

    def cancel_pending(self) -> None:
        # Ctrl-C 场景：让阻塞在 _call().result() 的线程尽快退出，避免非 daemon 线程阻止进程退出。
        with self._pending_mu:
            futs = list(self._pending)
        for f in futs:
            try:
                f.cancel()
            except Exception:
                pass

    def close(self) -> None:
        try:
            self.cancel_pending()
        except Exception:
            pass
        try:
            if self._client is not None:
                self._call(self._client.close())
        except Exception:
            pass
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=1)
        except Exception:
            pass

    # ---- 枚举 ----
    def list_recursive(self, root_path: str) -> List[RemoteEntry]:
        return self._call_retry(lambda: self._list_recursive(root_path), op="list_recursive")

    def listdir(self, path: str, *, refresh: bool = False, per_page: int = 30, page: int = 1) -> Any:
        assert self._client is not None
        return self._call_retry(
            lambda: self._client.fs.listdir(
                path if path.startswith("/") else f"/{path}",
                refresh=refresh,
                page=page,
                per_page=per_page,
            ),
            op="listdir",
        )

    def info(self, path: str) -> Any:
        assert self._client is not None
        return self._call_retry(lambda: self._client.fs.info(path if path.startswith("/") else f"/{path}"), op="info")

    async def _list_recursive(self, root_path: str) -> List[RemoteEntry]:
        root_norm = root_path.rstrip("/") or "/"
        entries: List[RemoteEntry] = []

        try:
            assert self._client is not None
            root_info = await self._client.fs.info(root_norm)
        except Exception as e:
            raise RuntimeError(f"remote info failed: {e}")

        if not root_info.is_dir:
            entries.append(
                RemoteEntry(
                    rel=posixpath.basename(root_norm),
                    path=root_norm,
                    is_dir=False,
                    size=root_info.size,
                    mtime_ns=_mtime_to_ns(root_info.modified),
                    sign=getattr(root_info, "sign", "") or "",
                )
            )
            return entries

        async def walk(cur: str) -> None:
            assert self._client is not None
            page = 1
            got = 0
            while True:
                res = await self._client.fs.listdir(cur, refresh=True, per_page=100, page=page)
                for obj in res.content:
                    child_path = posixpath.join(cur, obj.name)
                    if obj.is_dir:
                        await walk(child_path)
                        continue
                    rel = posixpath.relpath(child_path, root_norm).replace("\\", "/")
                    entries.append(
                        RemoteEntry(
                            rel=rel,
                            path=child_path,
                            is_dir=False,
                            size=obj.size,
                            mtime_ns=_mtime_to_ns(obj.modified),
                            sign=getattr(obj, "sign", "") or "",
                        )
                    )
                got += len(res.content)
                if got >= res.total or not res.content:
                    break
                page += 1

        await walk(root_norm)
        return entries

    # ---- 下载 / 读取 ----
    def download_to(self, remote_path: str, local_path: Path) -> None:
        self._call_retry(lambda: self._download_to(remote_path, local_path), op="download_to")

    def read_bytes(self, remote_path: str) -> bytes:
        return self._call_retry(lambda: self._read_bytes(remote_path), op="read_bytes")

    def read_text(self, remote_path: str) -> str:
        b = self.read_bytes(remote_path)
        return b.decode("utf-8", errors="replace")

    async def _download_to(self, remote_path: str, local_path: Path) -> None:
        url_path, headers = await self._download_request(remote_path)
        assert self._client is not None
        async with self._client.context.httpx_client.stream("GET", url_path, headers=headers) as r:
            if r.status_code == 401:
                raise AuthenticationFailed("Unauthorized")
            if r.status_code == 404:
                raise FileNotFoundError(remote_path)
            if r.status_code >= 400:
                raise HttpStatusError(r.status_code, r.reason_phrase)
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                body = await r.aread()
                try:
                    data = json.loads(body.decode("utf-8", errors="replace"))
                except Exception:
                    data = None
                if isinstance(data, dict) and isinstance(data.get("code"), int) and "message" in data:
                    code = int(data.get("code") or 0)
                    msg = str(data.get("message") or "")
                    if code == 401:
                        raise AuthenticationFailed(msg or "Unauthorized")
                    if code != 200:
                        raise BadResponse(msg or f"code={code}")
                # 兼容下载 JSON 文件：直接写入 body（不走流式）
                ensure_parent(local_path)
                local_path.write_bytes(body)
                return
            ensure_parent(local_path)
            with local_path.open("wb") as f:
                async for chunk in r.aiter_bytes():
                    f.write(chunk)

    async def _read_bytes(self, remote_path: str) -> bytes:
        url_path, headers = await self._download_request(remote_path)
        assert self._client is not None
        r = await self._client.context.httpx_client.get(url_path, headers=headers)
        if r.status_code == 404:
            raise FileNotFoundError(remote_path)
        if r.status_code == 401:
            raise AuthenticationFailed("Unauthorized")
        if r.status_code >= 400:
            raise HttpStatusError(r.status_code, r.reason_phrase)
        ct = (r.headers.get("content-type") or "").lower()
        if "application/json" in ct:
            try:
                data = r.json()
            except Exception:
                data = None
            if isinstance(data, dict) and isinstance(data.get("code"), int) and "message" in data:
                code = int(data.get("code") or 0)
                msg = str(data.get("message") or "")
                if code == 401:
                    raise AuthenticationFailed(msg or "Unauthorized")
                if code != 200:
                    raise BadResponse(msg or f"code={code}")
        return r.content

    async def _download_request(self, remote_path: str) -> Tuple[str, Dict[str, str]]:
        p = remote_path if remote_path.startswith("/") else f"/{remote_path}"
        headers: Dict[str, str] = {}
        assert self._client is not None
        token = self._client.get_token()
        if token:
            headers["Authorization"] = token

        # OpenList 的 /d 下载接口在部分场景下需要 sign，否则会返回 401（如 expire missing）。
        # 同时，OpenList 的 fs.info 在“对象不存在”时可能不会抛异常，而是返回“空对象”（取决于 openlist 客户端版本）。
        # 这里统一用 fs.info 的返回来判断存在性，并拿到 sign，避免依赖下载时报错来区分不存在。
        try:
            info = await self._client.fs.info(p)
        except Exception as e:
            msg = str(e).lower()
            if "not found" in msg or "object not found" in msg:
                raise FileNotFoundError(p)
            raise

        if info is None:
            raise FileNotFoundError(p)
        # 兼容“空对象”表示不存在：data=null 时常见字段可能为空/None。
        name = getattr(info, "name", None)
        path = getattr(info, "path", None)
        sign0 = getattr(info, "sign", None)
        size0 = getattr(info, "size", None)
        modified0 = getattr(info, "modified", None)
        if (
            name in (None, "")
            and path in (None, "")
            and sign0 in (None, "")
            and (size0 in (None, 0))
            and modified0 is None
        ):
            raise FileNotFoundError(p)

        sign = str(sign0 or "")
        url_path = f"/d{quote(p, safe='/')}"
        if sign:
            sep = "&" if "?" in url_path else "?"
            url_path += f"{sep}sign={sign}"
        return url_path, headers

    # ---- 上传 / 写入 ----
    def upload_file(self, remote_path: str, local_file: Path, *, overwrite: bool) -> None:
        assert self._client is not None
        self._call_retry(
            lambda: self._client.fs.upload_file(
                remote_path if remote_path.startswith("/") else f"/{remote_path}",
                str(local_file),
                overwrite=overwrite,
            ),
            op="upload_file",
        )

    def direct_upload_if_available(self, remote_path: str, local_file: Path, *, overwrite: bool) -> bool:
        assert self._client is not None
        size = local_file.stat().st_size

        async def _get_info() -> Optional[Dict[str, Any]]:
            payload = {
                "path": str(Path(remote_path).parent).replace("\\", "/"),
                "file_name": Path(remote_path).name,
                "file_size": size,
                "tool": "HttpDirect",
            }
            token = self._client.get_token()
            headers = {"Content-Type": "application/json"}
            if token:
                headers["Authorization"] = token
            r = await self._client.context.httpx_client.post("/api/fs/get_direct_upload_info", json=payload, headers=headers)
            if r.status_code == 401:
                raise AuthenticationFailed("Unauthorized")
            if r.status_code != 200:
                raise HttpStatusError(r.status_code, r.reason_phrase)
            try:
                data = r.json()
            except Exception:
                raise BadResponse("Invalid JSON response")
            if data.get("code") == 401:
                raise AuthenticationFailed(str(data.get("message") or "Unauthorized"))
            if data.get("code") != 200:
                return None
            return data.get("data")

        info = self._call_retry(_get_info, op="direct_upload:get_info")
        if not info:
            return False

        upload_url = info.get("upload_url")
        chunk_size = int(info.get("chunk_size") or 0) or 5 * 1024 * 1024
        method = (info.get("method") or "PUT").upper()
        if not upload_url:
            return False

        async def _direct_upload() -> None:
            assert self._client is not None

            async def _upload_chunk(start: int, chunk: bytes) -> None:
                end = start + len(chunk) - 1
                headers = {
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{size}",
                }
                r = await self._client.context.httpx_client.request(method, upload_url, content=chunk, headers=headers)
                if r.status_code == 401:
                    raise AuthenticationFailed("Unauthorized")
                if r.status_code >= 400:
                    raise HttpStatusError(r.status_code, r.reason_phrase)

            with local_file.open("rb") as f:
                offset = 0
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    await _upload_chunk(offset, chunk)
                    offset += len(chunk)

        self._call_retry(_direct_upload, op="direct_upload:upload")
        return True

    def upload_text(self, remote_path: str, text: str, *, overwrite: bool) -> None:
        data = text.encode("utf-8")
        self.upload_bytes(remote_path, data, overwrite=overwrite)

    def upload_bytes(self, remote_path: str, data: bytes, *, overwrite: bool) -> None:
        assert self._client is not None
        self._call_retry(
            lambda: self._client.fs.upload(
                remote_path if remote_path.startswith("/") else f"/{remote_path}",
                data,
                overwrite=overwrite,
                last_modified=int(time.time()),
            ),
            op="upload_bytes",
        )

    def rename(self, src: str, dst: str) -> None:
        assert self._client is not None
        self._call_retry(
            lambda: self._client.fs.rename(src if src.startswith("/") else f"/{src}", dst),
            op="rename",
        )

    # ---- 其他 ----
    def ensure_dir(self, path: str) -> None:
        assert self._client is not None
        self._call_retry(
            lambda: self._client.fs.makedirs(path if path.startswith("/") else f"/{path}", exist_ok=True),
            op="ensure_dir",
        )

    def remove(self, path: str) -> None:
        try:
            assert self._client is not None
            self._call(self._client.fs.remove(path if path.startswith("/") else f"/{path}"))
        except FatalAuthError:
            raise
        except Exception:
            pass


# ------------------------
# OpenList 目录遍历
# ------------------------


def list_openlist_recursive(client: OpenListClientSync, root_path: str) -> List[RemoteEntry]:
    return client.list_recursive(root_path)


def iter_openlist_recursive(client: OpenListClientSync, root_path: str) -> Iterator[RemoteEntry]:
    root_norm = root_path.rstrip("/") or "/"

    try:
        root_info = client.info(root_norm)
    except FatalAuthError:
        raise
    except Exception as e:
        raise RuntimeError(f"remote info failed: {e}") from e

    if not getattr(root_info, "is_dir", False):
        yield RemoteEntry(
            rel=posixpath.basename(root_norm),
            path=root_norm,
            is_dir=False,
            size=int(getattr(root_info, "size", 0) or 0),
            mtime_ns=_mtime_to_ns(getattr(root_info, "modified", None)),
            sign=str(getattr(root_info, "sign", "") or ""),
        )
        return

    def walk(cur: str) -> Iterator[RemoteEntry]:
        page = 1
        got = 0
        while True:
            res = client.listdir(cur, refresh=True, per_page=100, page=page)
            for obj in getattr(res, "content", []) or []:
                name = str(getattr(obj, "name", "") or "")
                if not name:
                    continue
                child_path = posixpath.join(cur, name)
                if getattr(obj, "is_dir", False):
                    yield from walk(child_path)
                    continue
                rel = posixpath.relpath(child_path, root_norm).replace("\\", "/")
                yield RemoteEntry(
                    rel=rel,
                    path=child_path,
                    is_dir=False,
                    size=int(getattr(obj, "size", 0) or 0),
                    mtime_ns=_mtime_to_ns(getattr(obj, "modified", None)),
                    sign=str(getattr(obj, "sign", "") or ""),
                )
            got += len(getattr(res, "content", []) or [])
            total = int(getattr(res, "total", 0) or 0)
            if not getattr(res, "content", None) or (total > 0 and got >= total):
                break
            page += 1

    yield from walk(root_norm)


# ------------------------
# WorkItem & 输入枚举
# ------------------------

@dataclass
class WorkItem:
    rel: str
    is_remote: bool
    src_local: Optional[Path] = None
    remote_path: Optional[str] = None
    src_size: int = 0
    src_mtime_ns: int = 0
    out_rel_override: Optional[str] = None


def normalize_ext(ext: str) -> str:
    ext = (ext or "").strip().lower()
    if not ext:
        return ""
    return ext if ext.startswith(".") else "." + ext


def build_suffixed_target_name(src_name: str, *, target_ext: str) -> str:
    p = Path(src_name)
    stem = p.stem
    src_ext = p.suffix.lower().lstrip(".") or "src"
    return f"{stem}__{src_ext}{normalize_ext(target_ext)}"


def apply_out_name_mode(p: Path, *, src_rel: str, target_ext: str, out_name_mode: str) -> Path:
    """
    根据 out_name_mode 改写输出路径的“文件名”：
    - collision：默认保持 <stem><target_ext>（靠外层的 compute_output_name_overrides 仅在撞名时用 out_rel_override 修正）
    - suffix：当 target_ext != src_ext 时，输出名改为 <stem>__<src_ext><target_ext>
    """
    if out_name_mode != "suffix":
        return p
    target_ext2 = normalize_ext(target_ext)
    if not target_ext2:
        return p
    src_p = Path(src_rel)
    if src_p.suffix.lower() == target_ext2:
        return p
    return p.with_name(build_suffixed_target_name(src_p.name, target_ext=target_ext2))


def _unique_collision_name(stem: str, src_ext: str, target_ext: str, reserved: set[str]) -> str:
    base = f"{stem}__{src_ext}{target_ext}"
    cand = base
    if cand in reserved:
        for i in range(2, 10000):
            cand2 = f"{stem}__{src_ext}__{i}{target_ext}"
            if cand2 not in reserved:
                cand = cand2
                break
    reserved.add(cand)
    return cand


def compute_image_out_name_overrides(
    names: Sequence[str],
    *,
    image_target_ext: str,
    out_name_mode: str,
) -> Dict[str, str]:
    """
    给同一目录下的图片文件名做“仅在撞名时”的输出名改写。

    规则：
    - collision：1.jpg/1.png -> 1.webp（或 1.avif）
    - suffix：1.jpg/1.png -> 1__jpg.webp / 1__png.webp（或 .avif）
    - 如果同目录内多个源文件会映射到同一个目标名（例如 1.jpg + 1.png -> 1.webp），
      则对这些需要转码的源文件输出名改为：1__jpg.webp / 1__png.webp（必要时再加 __2/__3...）
    - 若同目录已经存在目标扩展（例如已有 1.webp），保留该文件名不改，对其他冲突源改名。

    返回：{src_name: out_name}，仅包含“需要改名”的源文件。
    """
    target_ext = normalize_ext(image_target_ext)
    if not target_ext:
        return {}

    img_names = [n for n in names if Path(n).suffix.lower() in IMAGE_EXTS]
    if len(img_names) <= 1:
        return {}

    def _default_out_name(n: str) -> str:
        ext = Path(n).suffix.lower()
        if ext == target_ext:
            return n
        if out_name_mode == "suffix":
            return build_suffixed_target_name(n, target_ext=target_ext)
        return Path(n).with_suffix(target_ext).name

    # default target name (after conversion/copy semantics for images)
    groups: Dict[str, List[str]] = {}
    for n in img_names:
        out = _default_out_name(n)
        groups.setdefault(out, []).append(n)

    need: set[str] = set()
    for out, srcs in groups.items():
        if len(srcs) <= 1:
            continue
        keep = out if out in srcs else None
        for s in srcs:
            if keep and s == keep:
                continue
            need.add(s)

    if not need:
        return {}

    reserved: set[str] = set()
    for n in sorted(img_names):
        if n in need:
            continue
        reserved.add(_default_out_name(n))

    overrides: Dict[str, str] = {}
    for n in sorted(need):
        p = Path(n)
        overrides[n] = _unique_collision_name(p.stem, p.suffix.lower().lstrip(".") or "src", target_ext, reserved)

    return overrides


def compute_output_name_overrides(
    names: Sequence[str],
    *,
    out_name_of: Callable[[str], Optional[str]],
) -> Dict[str, str]:
    """
    通用“仅在撞名时”的输出名改写（按目录内文件名）。

    - out_name_of(name) 返回该源文件的默认输出文件名（仅文件名，不含路径）；返回 None 表示不参与撞名检测。
    - 若多个源文件会产生相同 out_name，则保留其中一个不改名（优先保留“源文件名就等于 out_name”的那个），
      其余改为：<stem>__<src_ext><target_ext>（必要时追加 __2/__3...）。

    返回：{src_name: out_name}，仅包含“需要改名”的源文件。
    """
    out_by_name: Dict[str, str] = {}
    groups: Dict[str, List[str]] = {}
    for n in names:
        out = out_name_of(n)
        if not out:
            continue
        out_by_name[n] = out
        groups.setdefault(out, []).append(n)

    if not groups:
        return {}

    need: set[str] = set()
    for out, srcs in groups.items():
        if len(srcs) <= 1:
            continue
        keep = out if out in srcs else sorted(srcs)[0]
        for s in srcs:
            if s == keep:
                continue
            need.add(s)

    if not need:
        return {}

    reserved: set[str] = set()
    for s, out in out_by_name.items():
        if s in need:
            continue
        reserved.add(out)

    overrides: Dict[str, str] = {}
    for s in sorted(need):
        out = out_by_name.get(s)
        if not out:
            continue
        target_ext = Path(out).suffix.lower() or ""
        if not target_ext.startswith("."):
            target_ext = "." + target_ext if target_ext else ""
        src_p = Path(s)
        overrides[s] = _unique_collision_name(src_p.stem, src_p.suffix.lower().lstrip(".") or "src", target_ext, reserved)

    return overrides


def make_default_out_name_of(
    *,
    image_target_ext: str,
    audio_target_ext: str,
    video_target_ext_by_name: Dict[str, str],
    out_name_mode: str,
) -> Callable[[str], Optional[str]]:
    def _out_name_of(n: str) -> Optional[str]:
        ext = Path(n).suffix.lower()
        if ext in IMAGE_EXTS:
            if ext == image_target_ext:
                return n
            if out_name_mode == "suffix":
                return build_suffixed_target_name(n, target_ext=image_target_ext)
            return Path(n).with_suffix(image_target_ext).name
        if ext in AUDIO_EXTS and audio_target_ext:
            if ext == audio_target_ext:
                return n
            if out_name_mode == "suffix":
                return build_suffixed_target_name(n, target_ext=audio_target_ext)
            return Path(n).with_suffix(audio_target_ext).name
        if ext in VIDEO_EXTS or ext in ANIMATED_IMAGE_EXTS:
            vext = video_target_ext_by_name.get(n)
            if not vext:
                return None
            if ext == vext:
                return n
            if out_name_mode == "suffix":
                return build_suffixed_target_name(n, target_ext=vext)
            return Path(n).with_suffix(vext).name
        if ext in COMIC_EXTS or looks_like_archive_name(n):
            if ext == ".cbz":
                return n
            if out_name_mode == "suffix":
                return build_suffixed_target_name(n, target_ext=".cbz")
            return Path(n).with_suffix(".cbz").name
        return None

    return _out_name_of


def iter_local_inputs(
    input_path: Path,
    *,
    image_codec: str = "webp",
    container: str = "auto",
    audio_policy: str = "copy_if_lossy",
    out_name_mode: str = "suffix",
) -> Tuple[Path, Iterator[WorkItem]]:
    if input_path.is_file():
        st = input_path.stat()
        root = input_path.parent

        def _one() -> Iterator[WorkItem]:
            yield WorkItem(
                rel=input_path.name,
                is_remote=False,
                src_local=input_path,
                src_size=int(st.st_size),
                src_mtime_ns=int(st.st_mtime_ns),
            )

        return root, _one()

    root = input_path

    def _walk_dir(d: Path) -> Iterator[WorkItem]:
        try:
            children = sorted(d.iterdir(), key=lambda p: p.name)
        except Exception:
            return
        image_target_ext = ".webp" if image_codec == "webp" else ".avif"
        audio_target_ext = ".opus" if audio_policy != "always_copy" else ""
        baseline_video_ext = ".mkv" if container == "mkv" else ".mp4"
        file_names: List[str] = []
        rel_by_name: Dict[str, str] = {}
        file_by_name: Dict[str, Path] = {}
        for p in children:
            name = p.name
            if should_ignore_name(name):
                continue
            if p.is_file():
                rel = p.relative_to(root).as_posix()
                if rel.startswith(LOCKS_DIR_NAME + "/") or rel.startswith(STATE_DIR_NAME + "/"):
                    continue
                file_names.append(name)
                rel_by_name[name] = rel
                file_by_name[name] = p

        video_target_ext_by_name: Dict[str, str] = {}
        video_names = [
            n
            for n in file_names
            if (Path(n).suffix.lower() in VIDEO_EXTS) or (Path(n).suffix.lower() in ANIMATED_IMAGE_EXTS)
        ]
        for n in video_names:
            video_target_ext_by_name[n] = baseline_video_ext

        # container=auto/mp4 时，部分视频可能因为字幕兼容性而从 mp4 自动切到 mkv；仅在“潜在撞名”的 stem 组里探测，避免无谓 ffprobe。
        if out_name_mode == "collision" and baseline_video_ext == ".mp4" and video_names:
            tmp_groups: Dict[str, List[str]] = {}
            for n in video_names:
                ext = Path(n).suffix.lower()
                out = n if ext == ".mp4" else Path(n).with_suffix(".mp4").name
                tmp_groups.setdefault(out, []).append(n)
            need_probe: set[str] = set()
            for _out, srcs in tmp_groups.items():
                if len(srcs) > 1:
                    need_probe.update(srcs)
            for n in sorted(need_probe):
                p = file_by_name.get(n)
                if not p:
                    continue
                probe = ffprobe_json(p, dry_run=False)
                info = classify(p, probe)
                has_subs, mp4_sub_ok = detect_subtitle_compat(info.streams)
                if has_subs and not mp4_sub_ok:
                    video_target_ext_by_name[n] = ".mkv"

        out_overrides = compute_output_name_overrides(
            file_names,
            out_name_of=make_default_out_name_of(
                image_target_ext=image_target_ext,
                audio_target_ext=audio_target_ext,
                video_target_ext_by_name=video_target_ext_by_name,
                out_name_mode=out_name_mode,
            ),
        )
        for p in children:
            name = p.name
            if should_ignore_name(name):
                continue
            if p.is_dir():
                if p.is_symlink():
                    continue
                yield from _walk_dir(p)
                continue
            if not p.is_file():
                continue
            rel = rel_by_name.get(name)
            if not rel:
                continue
            try:
                st = p.stat()
            except Exception:
                continue
            out_rel_override = None
            ov = out_overrides.get(name)
            if ov:
                out_rel_override = Path(rel).with_name(ov).as_posix()
            yield WorkItem(
                rel=rel,
                is_remote=False,
                src_local=p,
                src_size=int(st.st_size),
                src_mtime_ns=int(st.st_mtime_ns),
                out_rel_override=out_rel_override,
            )

    return root, _walk_dir(root)


def iter_remote_inputs(
    client: OpenListClientSync,
    root_path: str,
    *,
    image_codec: str = "webp",
    container: str = "auto",
    audio_policy: str = "copy_if_lossy",
    out_name_mode: str = "suffix",
) -> Tuple[str, Iterator[WorkItem]]:
    root_norm = root_path.rstrip("/") or "/"
    image_target_ext = ".webp" if image_codec == "webp" else ".avif"
    audio_target_ext = ".opus" if audio_policy != "always_copy" else ""
    baseline_video_ext = ".mkv" if container == "mkv" else ".mp4"

    def _gen() -> Iterator[WorkItem]:
        try:
            root_info = client.info(root_norm)
        except FatalAuthError:
            raise
        except Exception as e:
            raise RuntimeError(f"remote info failed: {e}") from e

        if not getattr(root_info, "is_dir", False):
            rel0 = posixpath.basename(root_norm)
            if should_ignore_name(rel0):
                return
            yield WorkItem(
                rel=rel0,
                is_remote=True,
                remote_path=root_norm,
                src_size=int(getattr(root_info, "size", 0) or 0),
                src_mtime_ns=_mtime_to_ns(getattr(root_info, "modified", None)),
            )
            return

        def walk(cur: str) -> Iterator[WorkItem]:
            dirs: List[str] = []
            files: List[Tuple[str, str, int, int, str]] = []  # (name, child_path, size, mtime_ns, sign)
            page = 1
            got = 0
            while True:
                res = client.listdir(cur, refresh=True, per_page=100, page=page)
                content = getattr(res, "content", []) or []
                for obj in content:
                    name = str(getattr(obj, "name", "") or "")
                    if not name or should_ignore_name(name):
                        continue
                    child_path = posixpath.join(cur, name)
                    is_dir = bool(getattr(obj, "is_dir", False))
                    if is_dir:
                        if name in {LOCKS_DIR_NAME, STATE_DIR_NAME}:
                            continue
                        dirs.append(child_path)
                        continue
                    rel = posixpath.relpath(child_path, root_norm).replace("\\", "/")
                    if rel.startswith(LOCKS_DIR_NAME + "/") or rel.startswith(STATE_DIR_NAME + "/"):
                        continue
                    files.append(
                        (
                            name,
                            child_path,
                            int(getattr(obj, "size", 0) or 0),
                            _mtime_to_ns(getattr(obj, "modified", None)),
                            str(getattr(obj, "sign", "") or ""),
                        )
                    )
                got += len(content)
                total = int(getattr(res, "total", 0) or 0)
                if not content or (total > 0 and got >= total):
                    break
                page += 1

            names = [n for (n, *_rest) in files]
            video_target_ext_by_name: Dict[str, str] = {}
            for n in names:
                ext = Path(n).suffix.lower()
                if ext in VIDEO_EXTS or ext in ANIMATED_IMAGE_EXTS:
                    video_target_ext_by_name[n] = baseline_video_ext

            out_overrides = compute_output_name_overrides(
                names,
                out_name_of=make_default_out_name_of(
                    image_target_ext=image_target_ext,
                    audio_target_ext=audio_target_ext,
                    video_target_ext_by_name=video_target_ext_by_name,
                    out_name_mode=out_name_mode,
                ),
            )

            for name, child_path, size, mtime_ns, sign in sorted(files, key=lambda x: x[0]):
                rel = posixpath.relpath(child_path, root_norm).replace("\\", "/")
                out_rel_override = None
                ov = out_overrides.get(name)
                if ov:
                    out_rel_override = Path(rel).with_name(ov).as_posix()
                yield WorkItem(
                    rel=rel,
                    is_remote=True,
                    remote_path=child_path,
                    src_size=size,
                    src_mtime_ns=mtime_ns,
                    out_rel_override=out_rel_override,
                )

            for d2 in sorted(dirs):
                yield from walk(d2)

        yield from walk(root_norm)

    return root_norm, _gen()


# ------------------------
# 状态/锁
# ------------------------

@dataclass
class StateEntry:
    ts: float
    status: str
    device_id: str
    src_rel: str
    src_size: int
    src_mtime_ns: int
    dst_rel: Optional[str] = None


class StateBackendJsonl:
    """
    - 本地：单文件 JSONL，append 写入（历史保留）。
    - OpenList：单文件 JSONL 只能通过“读旧文本 + 覆盖上传”模拟 append，存在并发丢行风险（不建议）。
    """

    def __init__(self, *, local_path: Optional[Path], remote_path: Optional[str], client: Optional[OpenListClientSync]) -> None:
        self.local_path = local_path
        self.remote_path = remote_path
        self.client = client
        self._append_mu = threading.Lock()
        if (local_path is None) == (remote_path is None):
            raise ValueError("StateBackendJsonl needs exactly one of local_path or remote_path")

    def read_all(self) -> str:
        if self.local_path is not None:
            if not self.local_path.exists():
                return ""
            return self.local_path.read_text("utf-8", errors="replace")

        assert self.client is not None and self.remote_path is not None
        try:
            return self.client.read_text(self.remote_path)
        except FileNotFoundError:
            return ""
        except FatalAuthError:
            raise
        except Exception:
            return ""

    def append_line(self, line: str) -> None:
        # OpenList 的“读旧文本 + 覆盖上传”不是原子 append：并发写会丢行。
        # 这里至少保证“单进程内”串行 append，降低丢失 done 的概率。
        with self._append_mu:
            if self.local_path is not None:
                ensure_parent(self.local_path)
                with self.local_path.open("a", encoding="utf-8") as f:
                    f.write(line + "\n")
                    f.flush()
                return

            assert self.client is not None and self.remote_path is not None
            # ensure remote parent directory exists
            parent = posixpath.dirname(self.remote_path.rstrip("/")) or "/"
            try:
                self.client.ensure_dir(parent)
            except FatalAuthError:
                raise
            except Exception:
                pass
            # Remote append 采用“读旧文本 + 覆盖上传”。read_all() 的“吞异常返回空串”适合容错读取，
            # 但用于 append 时会导致网络抖动/鉴权异常时把历史 state 当作空串覆盖，破坏多设备协同。
            # 因此这里显式读取并在失败时抛错，让上层 safe_append 记录 WARN 而不是清空 state。
            created_new = False
            try:
                old = self.client.read_text(self.remote_path)
            except FileNotFoundError:
                old = ""
                created_new = True
            except FatalAuthError:
                raise
            except Exception as e:
                raise RuntimeError(f"state read failed: {e}")
            new = old
            if new and not new.endswith("\n"):
                new += "\n"
            new += line + "\n"
            self.client.upload_text(self.remote_path, new, overwrite=True)
            if created_new:
                # OpenList 服务端偶尔不会立即刷新“新创建文件”的状态；触发一次 listdir(refresh=True) 提示它更新。
                try:
                    self.client.listdir(parent, refresh=True, per_page=1)
                except FatalAuthError:
                    raise
                except Exception:
                    pass


class StateBackendPerFile:
    """
    OpenList：每个 src_rel 单独一个 state 文件，避免单文件 JSONL 的“读旧+覆盖”并发丢行。

    Layout:
    - <state_dir>/<prefix>/<sha1(src_rel)>.json
    """

    def __init__(self, *, remote_dir_path: str, client: OpenListClientSync, legacy_jsonl_path: Optional[str] = None) -> None:
        self.remote_dir_path = remote_dir_path.rstrip("/") or "/"
        self.client = client
        self.legacy_jsonl_path = legacy_jsonl_path
        self._ensured_dirs: set[str] = set()
        self._mu = threading.Lock()
        self._legacy_cache: Dict[str, StateEntry] = {}
        self._legacy_cache_at = 0.0
        self._ensure_dir(self.remote_dir_path)

    def _ensure_dir(self, path: str) -> None:
        with self._mu:
            if path in self._ensured_dirs:
                return
            self._ensured_dirs.add(path)
        self.client.ensure_dir(path)

    def _path_for_rel(self, rel: str) -> Tuple[str, str]:
        token = sha1_hex(rel)
        prefix = token[:2]
        parent = remote_join(self.remote_dir_path, prefix)
        p = remote_join(self.remote_dir_path, f"{prefix}/{token}.json")
        return parent, p

    def _load_legacy_latest(self, *, force: bool = False) -> Dict[str, StateEntry]:
        if not self.legacy_jsonl_path:
            return {}
        with self._mu:
            if not force and self._legacy_cache and (time.time() - self._legacy_cache_at) < 10:
                return self._legacy_cache
        try:
            txt = self.client.read_text(self.legacy_jsonl_path)
        except FileNotFoundError:
            txt = ""
        except FatalAuthError:
            raise
        except Exception:
            txt = ""
        latest: Dict[str, StateEntry] = {}
        for line in txt.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            rel = obj.get("src_rel")
            st = obj.get("status")
            if not isinstance(rel, str) or not isinstance(st, str):
                continue
            try:
                ent = StateEntry(
                    ts=float(obj.get("ts", 0)),
                    status=st,
                    device_id=str(obj.get("device_id", "")),
                    src_rel=rel,
                    src_size=int(obj.get("src_size", 0)),
                    src_mtime_ns=int(obj.get("src_mtime_ns", 0)),
                    dst_rel=obj.get("dst_rel"),
                )
            except Exception:
                continue
            prev = latest.get(rel)
            if prev is None or ent.ts >= prev.ts:
                latest[rel] = ent
        with self._mu:
            self._legacy_cache = latest
            self._legacy_cache_at = time.time()
        return latest

    def read_latest(self, rel: str) -> Optional[StateEntry]:
        _parent, p = self._path_for_rel(rel)
        try:
            txt = self.client.read_text(p)
        except FileNotFoundError:
            ent = self._load_legacy_latest().get(rel)
            # 懒迁移：把 legacy 单文件 JSONL 的最新记录写入 per-file state，降低后续对 legacy 的依赖。
            if ent is not None and ent.status == "done":
                try:
                    self.write_latest(ent)
                except FatalAuthError:
                    raise
                except Exception:
                    pass
            return ent
        except FatalAuthError:
            raise
        except Exception:
            return None
        try:
            obj = json.loads(txt or "{}")
        except Exception:
            return None
        if not isinstance(obj, dict):
            return None
        st = obj.get("status")
        if not isinstance(st, str):
            return None
        try:
            ent = StateEntry(
                ts=float(obj.get("ts", 0)),
                status=st,
                device_id=str(obj.get("device_id", "")),
                src_rel=str(obj.get("src_rel", rel)),
                src_size=int(obj.get("src_size", 0)),
                src_mtime_ns=int(obj.get("src_mtime_ns", 0)),
                dst_rel=obj.get("dst_rel"),
            )
        except Exception:
            return None
        if ent.src_rel != rel:
            return None
        return ent

    def write_latest(self, ent: StateEntry) -> None:
        parent, p = self._path_for_rel(ent.src_rel)
        self._ensure_dir(parent)
        payload = json.dumps(
            {
                "ts": ent.ts,
                "status": ent.status,
                "device_id": ent.device_id,
                "src_rel": ent.src_rel,
                "src_size": ent.src_size,
                "src_mtime_ns": ent.src_mtime_ns,
                "dst_rel": ent.dst_rel,
            },
            ensure_ascii=False,
        )
        self.client.upload_text(p, payload, overwrite=True)
        # OpenList 服务端偶尔不会立即刷新文件变更；触发一次 listdir(refresh=True) 提示它更新。
        try:
            self.client.listdir(parent, refresh=True, per_page=1)
        except FatalAuthError:
            raise
        except Exception:
            pass


class LockBackend:
    def __init__(
        self,
        *,
        local_dir: Optional[Path],
        remote_dir_path: Optional[str],
        client: Optional[OpenListClientSync],
        device_id: str,
        ttl_sec: int,
        steal_stale: bool,
    ) -> None:
        self.local_dir = local_dir
        self.remote_dir_path = remote_dir_path
        self.client = client
        self.device_id = device_id
        self.ttl_sec = ttl_sec
        self.steal_stale = steal_stale

        if (local_dir is None) == (remote_dir_path is None):
            raise ValueError("LockBackend needs exactly one of local_dir or remote_dir_path")

        if self.local_dir is not None:
            self.local_dir.mkdir(parents=True, exist_ok=True)
        else:
            assert self.client is not None and self.remote_dir_path is not None
            base = self.remote_dir_path.rstrip("/") or "/"
            self.remote_dir_path = base
            self.client.ensure_dir(base)

    def _now(self) -> float:
        return time.time()

    def is_active(self, key: str) -> bool:
        """
        Best-effort 判断锁是否“仍然有效”（存在且未超过 TTL）。
        - 仅用于提升 liveness：如果无法确认（网络/解析错误），返回 False 让任务继续跑。
        """
        token = sha1_hex(key)
        now = self._now()

        if self.local_dir is not None:
            lock_path = self.local_dir / f"{token}.lock"
            if not lock_path.exists():
                return False
            try:
                obj = json.loads(lock_path.read_text("utf-8", errors="replace") or "{}")
                ts = float(obj.get("ts", 0) or 0)
            except Exception:
                return False
            return ts > 0 and (now - ts) <= self.ttl_sec

        assert self.client is not None and self.remote_dir_path is not None
        lock_file = remote_join(self.remote_dir_path, f"{token}.lock")
        try:
            txt = self.client.read_text(lock_file)
            obj = json.loads(txt or "{}")
            ts = float(obj.get("ts", 0) or 0)
        except FileNotFoundError:
            return False
        except FatalAuthError:
            raise
        except Exception:
            return False
        return ts > 0 and (now - ts) <= self.ttl_sec

    def try_acquire(self, key: str) -> Tuple[bool, str]:
        token = sha1_hex(key)
        now = self._now()

        if self.local_dir is not None:
            lock_path = self.local_dir / f"{token}.lock"

            if lock_path.exists():
                try:
                    obj = json.loads(lock_path.read_text("utf-8", errors="replace") or "{}")
                    ts = float(obj.get("ts", 0) or 0)
                    owner = str(obj.get("device_id", ""))
                    stale = now - ts > self.ttl_sec
                    same = owner == self.device_id
                    if same or (self.steal_stale and stale):
                        lock_path.unlink(missing_ok=True)  # type: ignore
                    else:
                        return False, f"locked by {owner or 'unknown'}"
                except Exception:
                    # metadata broken：默认按“活跃锁”处理；只有在允许 steal 时才尝试清理
                    if not self.steal_stale:
                        return False, "local lock present (bad metadata)"
                    try:
                        lock_path.unlink(missing_ok=True)  # type: ignore
                    except Exception:
                        return False, "local lock present (cannot remove)"

            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(json.dumps({"ts": now, "device_id": self.device_id}, ensure_ascii=False))
                return True, "ok"
            except FileExistsError:
                return False, "local lock active"

        assert self.client is not None and self.remote_dir_path is not None
        lock_file = remote_join(self.remote_dir_path, f"{token}.lock")

        stale = True
        same = False
        owner = ""

        try:
            txt = self.client.read_text(lock_file)
            obj = json.loads(txt or "{}")
            ts = float(obj.get("ts", 0) or 0)
            owner = str(obj.get("device_id", ""))
            stale = now - ts > self.ttl_sec if ts > 0 else True
            same = owner == self.device_id
            if not (same or (self.steal_stale and stale)):
                return False, f"locked by {owner or 'unknown'}"
        except FileNotFoundError:
            stale = True
        except FatalAuthError:
            raise
        except Exception as e:
            # 读锁失败：为了不断跑（默认 steal_stale=ON）可以继续尝试接管；
            # 但当明确关闭 steal 时，应该把它当作“有人在跑/未知状态”来避免误抢。
            if not self.steal_stale:
                return False, f"lock read failed: {e}"
            stale = True

        try:
            self.client.upload_text(
                lock_file,
                json.dumps({"ts": now, "device_id": self.device_id}, ensure_ascii=False),
                overwrite=True,
            )
            # OpenList 服务端偶尔不会立即刷新“新创建/更新 lock 文件”的状态；触发一次 listdir(refresh=True) 提示它更新。
            try:
                self.client.listdir(self.remote_dir_path, refresh=True, per_page=1)
            except FatalAuthError:
                raise
            except Exception:
                pass
            return True, "stolen" if (stale and owner) else "ok"
        except FatalAuthError:
            raise
        except Exception as e:
            return False, f"lock write failed: {e}"

    def release(self, key: str) -> None:
        token = sha1_hex(key)
        if self.local_dir is not None:
            p = self.local_dir / f"{token}.lock"
            try:
                p.unlink()
            except Exception:
                pass
            return

        assert self.client is not None and self.remote_dir_path is not None
        lock_file = remote_join(self.remote_dir_path, f"{token}.lock")
        try:
            self.client.remove(lock_file)
        except FatalAuthError:
            raise
        except Exception:
            pass
        # 删除锁后也刷新一下目录，避免服务端缓存导致其他设备仍“看见”旧锁。
        try:
            self.client.listdir(self.remote_dir_path, refresh=True, per_page=1)
        except FatalAuthError:
            raise
        except Exception:
            pass


class Coordinator:
    def __init__(self, state: StateBackendJsonl | StateBackendPerFile, locks: LockBackend, *, device_id: str, ttl_sec: int) -> None:
        self.state = state
        self.locks = locks
        self.device_id = device_id
        self.ttl_sec = ttl_sec
        self._cache_map: Dict[str, StateEntry] = {}
        self._cache_map_at = 0.0
        self._cache_one: Dict[str, Optional[StateEntry]] = {}
        self._cache_one_at: Dict[str, float] = {}
        self._mu = threading.Lock()

    def _now(self) -> float:
        return time.time()

    def _load_latest_map(self, *, force: bool = False) -> Dict[str, StateEntry]:
        if not isinstance(self.state, StateBackendJsonl):
            return {}
        with self._mu:
            if not force and self._cache_map and (self._now() - self._cache_map_at < 10):
                return self._cache_map

            txt = self.state.read_all()
            latest: Dict[str, StateEntry] = {}
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                rel = obj.get("src_rel")
                st = obj.get("status")
                if not isinstance(rel, str) or not isinstance(st, str):
                    continue
                try:
                    ent = StateEntry(
                        ts=float(obj.get("ts", 0)),
                        status=st,
                        device_id=str(obj.get("device_id", "")),
                        src_rel=rel,
                        src_size=int(obj.get("src_size", 0)),
                        src_mtime_ns=int(obj.get("src_mtime_ns", 0)),
                        dst_rel=obj.get("dst_rel"),
                    )
                except Exception:
                    continue
                prev = latest.get(rel)
                if prev is None or ent.ts >= prev.ts:
                    latest[rel] = ent

            self._cache_map = latest
            self._cache_map_at = self._now()
            return latest

    def get_latest(self, rel: str, *, force: bool = False) -> Optional[StateEntry]:
        if isinstance(self.state, StateBackendJsonl):
            return self._load_latest_map(force=force).get(rel)

        now = self._now()
        with self._mu:
            if not force:
                at = self._cache_one_at.get(rel)
                if at and (now - at) < 10:
                    return self._cache_one.get(rel)
        ent = self.state.read_latest(rel)
        with self._mu:
            self._cache_one[rel] = ent
            self._cache_one_at[rel] = now
        return ent

    def is_done(self, it: WorkItem) -> bool:
        ent = self.get_latest(it.rel)
        if not ent:
            return False
        if ent.status != "done":
            return False
        return ent.src_size == it.src_size and ent.src_mtime_ns == it.src_mtime_ns

    def is_processing(self, it: WorkItem, *, current_device_id: Optional[str] = None) -> bool:
        ent = self.get_latest(it.rel)
        if not ent:
            return False
        if ent.status != "processing":
            return False
        if (self._now() - ent.ts) > self.ttl_sec:
            return False
        # 同设备中断/重启：不要因为旧的 processing 阻塞自己继续跑
        if current_device_id and ent.device_id == current_device_id:
            return False
        # processing 仅作为提示；真实“是否有人在跑”以 lock 为准（读失败则当作不活跃，优先不断跑）
        try:
            return self.locks.is_active(it.rel)
        except FatalAuthError:
            raise
        except Exception:
            return False

    def append(self, status: str, it: WorkItem, *, dst_rel: Optional[str] = None) -> None:
        ts = self._now()
        ent = StateEntry(
            ts=ts,
            status=status,
            device_id=self.device_id,
            src_rel=it.rel,
            src_size=it.src_size,
            src_mtime_ns=it.src_mtime_ns,
            dst_rel=dst_rel,
        )
        if isinstance(self.state, StateBackendJsonl):
            rec = {
                "ts": ts,
                "status": status,
                "device_id": self.device_id,
                "src_rel": it.rel,
                "src_size": it.src_size,
                "src_mtime_ns": it.src_mtime_ns,
                "dst_rel": dst_rel,
            }
            self.state.append_line(json.dumps(rec, ensure_ascii=False))
        else:
            self.state.write_latest(ent)
        with self._mu:
            if isinstance(self.state, StateBackendJsonl):
                self._cache_map[it.rel] = ent
                self._cache_map_at = self._now()
            else:
                self._cache_one[it.rel] = ent
                self._cache_one_at[it.rel] = self._now()


# ------------------------
# ffprobe / encoder cache
# ------------------------

def require_tools() -> None:
    for t in ("ffmpeg", "ffprobe"):
        if shutil.which(t) is None:
            log_err(f"ERROR: missing {t}, please install and ensure it is in PATH.")
            sys.exit(2)


_ENCODER_CACHE: Optional[str] = None


def ffmpeg_encoders() -> str:
    global _ENCODER_CACHE
    if _ENCODER_CACHE is None:
        cp = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # 某些 ffmpeg build 可能把列表输出到 stderr；合并避免误判。
        _ENCODER_CACHE = ((cp.stdout or "") + "\n" + (cp.stderr or "")).strip()
    return _ENCODER_CACHE


def has_encoder(enc: str) -> bool:
    return enc in ffmpeg_encoders()


def ffprobe_json(path: str | Path, *, dry_run: bool) -> Optional[Dict[str, Any]]:
    cp = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(path),
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if cp.returncode != 0:
        return None
    try:
        return json.loads(cp.stdout)
    except Exception:
        return None


def opus_bitrate_for_channels(ch: Optional[int]) -> str:
    if not ch:
        return "128k"
    if ch <= 1:
        return "64k"
    if ch == 2:
        return "128k"
    if ch <= 6:
        return "256k"
    return "450k"


def normalize_pix_fmt(pf: str) -> str:
    pf = (pf or "").strip().lower()
    if pf.startswith("yuvj"):
        pf = "yuv" + pf[4:]
    return pf


def map_pix_fmt_for_video(src_pf: Optional[str]) -> Optional[str]:
    if not src_pf:
        return None
    pf = normalize_pix_fmt(src_pf)
    if pf.startswith(("yuv", "yuva")):
        return pf
    if pf.startswith(("rgb", "bgr", "gbrp")):
        return "yuv444p"
    if pf.startswith("gray"):
        return "yuv420p"
    return None


def map_pix_fmt_for_avif(src_pf: Optional[str]) -> Optional[str]:
    if not src_pf:
        return None
    pf = normalize_pix_fmt(src_pf)
    if pf.startswith(("yuv", "yuva")):
        return pf
    if "rgba" in pf or "bgra" in pf:
        return "yuva420p"
    if pf.startswith(("rgb", "bgr", "gbrp")):
        return "yuv444p"
    if pf.startswith("gray"):
        return "yuv420p"
    return None


# ------------------------
# 分类
# ------------------------

@dataclass
class MediaInfo:
    kind: str
    streams: List[Dict[str, Any]] = field(default_factory=list)
    fmt: Dict[str, Any] = field(default_factory=dict)


def classify(path: Path, probe: Optional[Dict[str, Any]]) -> MediaInfo:
    ext = path.suffix.lower()
    streams = (probe or {}).get("streams") or []
    fmt = (probe or {}).get("format") or {}

    if ext in COMIC_EXTS or looks_like_archive_name(path.name):
        return MediaInfo("comic", streams, fmt)
    if ext in SUB_EXTS:
        return MediaInfo("subtitle", streams, fmt)
    if ext in ANIMATED_IMAGE_EXTS:
        return MediaInfo("video", streams, fmt)
    if ext in IMAGE_EXTS:
        return MediaInfo("image", streams, fmt)

    has_audio = any(s.get("codec_type") == "audio" for s in streams)
    has_video = any(
        s.get("codec_type") == "video" and not (s.get("disposition") or {}).get("attached_pic", 0) for s in streams
    )
    if has_video:
        return MediaInfo("video", streams, fmt)
    if has_audio:
        return MediaInfo("audio", streams, fmt)

    if ext in VIDEO_EXTS:
        return MediaInfo("video", streams, fmt)
    if ext in AUDIO_EXTS:
        return MediaInfo("audio", streams, fmt)

    return MediaInfo("other", streams, fmt)


def detect_subtitle_compat(streams: List[Dict[str, Any]]) -> Tuple[bool, bool]:
    subs = [s for s in streams if s.get("codec_type") == "subtitle"]
    if not subs:
        return (False, True)
    ok = True
    for s in subs:
        c = (s.get("codec_name") or "").lower()
        if c in BITMAP_SUB_CODECS:
            ok = False
        elif c and c not in TEXT_SUB_CODECS:
            ok = False
    return (True, ok)


def get_main_video_stream(info: MediaInfo) -> Optional[Dict[str, Any]]:
    vids = [
        s for s in info.streams if s.get("codec_type") == "video" and not (s.get("disposition") or {}).get("attached_pic", 0)
    ]
    if not vids:
        return None
    return vids[0]


# ------------------------
# ffmpeg 命令构建
# ------------------------

def nvenc_preset_from_generic(p: str) -> str:
    p = (p or "").strip().lower()
    if re.fullmatch(r"p[1-7]", p):
        return p
    mapping = {
        "ultrafast": "p1",
        "superfast": "p2",
        "veryfast": "p3",
        "faster": "p4",
        "fast": "p5",
        "medium": "p5",
        "slow": "p6",
        "slower": "p7",
        "veryslow": "p7",
    }
    return mapping.get(p, "p6")


def replace_arg(cmd: List[str], key: str, new_value: str) -> Optional[List[str]]:
    idx = None
    for i in range(len(cmd) - 1):
        if cmd[i] == key:
            idx = i
    if idx is None:
        return None
    out = cmd.copy()
    out[idx + 1] = new_value
    return out


def insert_after_pair(cmd: List[str], key: str, value: str, insert: List[str]) -> Optional[List[str]]:
    for i in range(len(cmd) - 1):
        if cmd[i] == key and cmd[i + 1] == value:
            out = cmd.copy()
            out[i + 2 : i + 2] = insert
            return out
    return None


def replace_last(cmd: List[str], new_last: str) -> List[str]:
    out = cmd.copy()
    out[-1] = new_last
    return out


def build_video_cmd_single(
    in_path: str | Path,
    out_path: Path,
    info: MediaInfo,
    *,
    container_pref: str,
    video_policy: str,
    audio_policy: str,
    allow_opus_in_mp4: bool,
    encoder: str,  # hevc_nvenc/libx265/libx264
    video_crf: int,
    video_preset: str,
    pix_fmt: str,
    faststart: bool,
) -> Tuple[List[str], Optional[List[str]]]:
    streams = info.streams
    has_subs, mp4_sub_ok = detect_subtitle_compat(streams)

    container = container_pref
    if container == "auto":
        container = "mp4"
    if container == "mp4" and has_subs and not mp4_sub_ok:
        container = "mkv"

    cmd: List[str] = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-y",
        "-i",
        str(in_path),
        "-map",
        "0",
        "-map_metadata",
        "0",
        "-map_chapters",
        "0",
    ]
    if container == "mp4":
        cmd += ["-dn", "-map", "-0:t?"]

    v_stream = get_main_video_stream(info)
    v_codec_in = (v_stream.get("codec_name") or "").lower() if v_stream else ""
    src_pf = str(v_stream.get("pix_fmt")) if (v_stream and v_stream.get("pix_fmt")) else None
    src_range = (v_stream.get("color_range") or "").strip().lower() if v_stream else ""
    src_full_range = bool(src_range == "pc" or (src_pf or "").strip().lower().startswith("yuvj"))

    want_pix_retry = False
    retry_cmd: Optional[List[str]] = None

    # video
    if video_policy == "always_copy" or (video_policy == "copy_if_hevc" and v_codec_in in {"hevc", "h265"}):
        cmd += ["-c:v", "copy"]
    else:
        if encoder == "hevc_nvenc":
            cmd += [
                "-c:v",
                "hevc_nvenc",
                "-preset",
                nvenc_preset_from_generic(video_preset),
                "-tune",
                "hq",
                "-rc",
                "constqp",
                "-qp",
                str(video_crf),
            ]
        elif encoder == "libx264":
            cmd += ["-c:v", "libx264", "-preset", video_preset, "-crf", str(video_crf)]
        else:
            cmd += ["-c:v", "libx265", "-preset", video_preset, "-crf", str(video_crf)]

        if pix_fmt != "auto":
            cmd += ["-pix_fmt", pix_fmt]
            if pix_fmt != "yuv420p":
                want_pix_retry = True
        else:
            mapped = map_pix_fmt_for_video(src_pf)
            if mapped:
                cmd += ["-pix_fmt", mapped]
                if mapped != "yuv420p":
                    want_pix_retry = True

        # yuvj*/pc(full-range) 来源若直接转成 yuv*，需要显式指定 range，否则可能出现亮度/对比度偏移。
        if src_full_range:
            cmd += ["-vf", "scale=in_range=pc:out_range=pc"]

        if container == "mp4":
            cmd += ["-tag:v", "hvc1"]

    # audio
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    lossy_in = False
    if audio_streams:
        c0 = (audio_streams[0].get("codec_name") or "").lower()
        if c0 in LOSSY_AUDIO_CODECS:
            lossy_in = True

    out_audio_codec = "opus"
    if container == "mp4" and not allow_opus_in_mp4:
        out_audio_codec = "aac"

    if audio_policy == "always_copy":
        cmd += ["-c:a", "copy"]
    elif audio_policy == "copy_if_lossy" and lossy_in:
        cmd += ["-c:a", "copy"]
    else:
        if out_audio_codec == "opus" and has_encoder("libopus"):
            cmd += ["-c:a", "libopus", "-vbr", "on", "-compression_level", "10", "-application", "audio"]
            for i, s in enumerate(audio_streams):
                cmd += [f"-b:a:{i}", opus_bitrate_for_channels(safe_int(s.get("channels")))]
        else:
            if has_encoder("aac") or has_encoder("libfdk_aac"):
                cmd += ["-c:a", "aac", "-b:a", "192k"]
            else:
                cmd += ["-c:a", "copy"]

    # subtitles
    sub_streams = [s for s in streams if s.get("codec_type") == "subtitle"]
    if sub_streams:
        if container == "mp4":
            cmd += ["-c:s", "mov_text"]
        else:
            cmd += ["-c:s", "copy"]

    if container == "mp4":
        movflags = ["use_metadata_tags"]
        if faststart:
            movflags.insert(0, "faststart")
        cmd += ["-movflags", "+" + "+".join(movflags)]

    cmd += [str(out_path)]

    if want_pix_retry:
        retry_cmd = replace_arg(cmd, "-pix_fmt", "yuv420p")

    return cmd, retry_cmd


def build_video_candidates(
    in_path: str | Path,
    out_path: Path,
    info: MediaInfo,
    *,
    container_pref: str,
    video_policy: str,
    audio_policy: str,
    allow_opus_in_mp4: bool,
    video_encoder: str,  # auto/hevc_nvenc/libx265/libx264
    video_crf: int,
    video_preset: str,
    pix_fmt: str,
    faststart: bool,
) -> List[List[str]]:
    cands: List[List[str]] = []

    def add(enc: str, *, preset_override: Optional[str] = None) -> None:
        cmd1, retry = build_video_cmd_single(
            in_path,
            out_path,
            info,
            container_pref=container_pref,
            video_policy=video_policy,
            audio_policy=audio_policy,
            allow_opus_in_mp4=allow_opus_in_mp4,
            encoder=enc,
            video_crf=video_crf,
            video_preset=preset_override or video_preset,
            pix_fmt=pix_fmt,
            faststart=faststart,
        )
        cands.append(cmd1)
        if retry:
            cands.append(retry)
        # NVENC 在部分环境（例如 Debian 的 ffmpeg build + mp4）可能会遇到 muxer 报错：
        #   [mp4] pts/dts pair unsupported
        # 这里额外尝试一次禁用 B-frames（避免 pts/dts 不匹配），以提升 NVENC 成功率。
        if enc == "hevc_nvenc":
            bf0 = insert_after_pair(cmd1, "-c:v", "hevc_nvenc", ["-bf", "0"])
            if bf0:
                cands.append(bf0)
                bf0_retry = replace_arg(bf0, "-pix_fmt", "yuv420p")
                if bf0_retry:
                    cands.append(bf0_retry)

    if video_encoder == "auto":
        has_nvenc = has_encoder("hevc_nvenc")
        has_x265 = has_encoder("libx265")
        has_x264 = has_encoder("libx264")

        if has_nvenc:
            add("hevc_nvenc")
        if has_x265:
            add("libx265")
            if video_preset in {"slow", "slower", "veryslow"}:
                add("libx265", preset_override="medium")
        elif has_x264:
            add("libx264")

        # 最后兜底：当 x265/NVENC 失败时仍尝试 x264（除非强制 always_hevc）。
        if video_policy != "always_hevc" and has_x264:
            add("libx264")
    else:
        add(video_encoder)
        if video_encoder == "hevc_nvenc":
            if has_encoder("libx265"):
                add("libx265")
                if video_preset in {"slow", "slower", "veryslow"}:
                    add("libx265", preset_override="medium")
            if video_policy != "always_hevc" and has_encoder("libx264"):
                add("libx264")
        if video_encoder == "libx265":
            if video_preset in {"slow", "slower", "veryslow"}:
                add("libx265", preset_override="medium")
            if video_policy != "always_hevc" and has_encoder("libx264"):
                add("libx264")

    # 去重
    seen = set()
    uniq: List[List[str]] = []
    for c in cands:
        k = "\0".join(c)
        if k not in seen:
            seen.add(k)
            uniq.append(c)
    return uniq


def build_audio_candidates(
    in_path: str | Path,
    out_path: Path,
    info: MediaInfo,
    *,
    audio_policy: str,
) -> List[List[str]]:
    audio_streams = [s for s in info.streams if s.get("codec_type") == "audio"]
    if not audio_streams:
        return []

    lossy_in = False
    c0 = (audio_streams[0].get("codec_name") or "").lower()
    if c0 in LOSSY_AUDIO_CODECS:
        lossy_in = True

    if audio_policy == "always_copy" or (audio_policy == "copy_if_lossy" and lossy_in):
        return [
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-y",
                "-i",
                str(in_path),
                "-map_metadata",
                "0",
                "-map_chapters",
                "0",
                "-vn",
                "-map",
                "0:a",
                "-c:a",
                "copy",
                str(out_path),
            ]
        ]

    if has_encoder("libopus"):
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-y",
            "-i",
            str(in_path),
            "-map_metadata",
            "0",
            "-map_chapters",
            "0",
            "-vn",
            "-map",
            "0:a",
            "-c:a",
            "libopus",
            "-vbr",
            "on",
            "-compression_level",
            "10",
            "-application",
            "audio",
        ]
        for i, s in enumerate(audio_streams):
            cmd += [f"-b:a:{i}", opus_bitrate_for_channels(safe_int(s.get("channels")))]
        cmd += [str(out_path)]
        return [cmd]

    return [
        [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-y",
            "-i",
            str(in_path),
            "-map_metadata",
            "0",
            "-map_chapters",
            "0",
            "-vn",
            "-map",
            "0:a",
            "-c:a",
            "copy",
            str(out_path),
        ]
    ]


def build_image_candidates(
    in_path: str | Path,
    out_path: Path,
    *,
    image_codec: str,  # webp/avif
    webp_quality: int,
    webp_lossless: bool,
    avif_crf: int,
    avif_pix_fmt: str,
    src_pix_fmt: Optional[str],
) -> List[List[str]]:
    if image_codec == "webp":
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-y",
            "-i",
            str(in_path),
            "-frames:v",
            "1",
            "-c:v",
            "libwebp",
            "-compression_level",
            "6",
        ]
        if webp_lossless:
            cmd += ["-lossless", "1"]
        else:
            cmd += ["-q:v", str(webp_quality)]
        cmd += [str(out_path)]
        return [cmd]

    # avif fallback webp
    if not has_encoder("libaom-av1"):
        out2 = out_path.with_suffix(".webp")
        return build_image_candidates(
            in_path,
            out2,
            image_codec="webp",
            webp_quality=webp_quality,
            webp_lossless=webp_lossless,
            avif_crf=avif_crf,
            avif_pix_fmt=avif_pix_fmt,
            src_pix_fmt=src_pix_fmt,
        )

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-y",
        "-i",
        str(in_path),
        "-frames:v",
        "1",
        "-c:v",
        "libaom-av1",
        "-still-picture",
        "1",
        "-crf",
        str(avif_crf),
        "-b:v",
        "0",
    ]

    if avif_pix_fmt != "auto":
        cmd += ["-pix_fmt", avif_pix_fmt]
        cmd += [str(out_path)]
        if avif_pix_fmt != "yuv420p":
            retry = replace_arg(cmd, "-pix_fmt", "yuv420p")
            return [cmd, retry] if retry else [cmd]
        return [cmd]

    mapped = map_pix_fmt_for_avif(src_pix_fmt)
    if mapped:
        cmd += ["-pix_fmt", mapped]
    cmd += [str(out_path)]
    if mapped and mapped != "yuv420p":
        retry = replace_arg(cmd, "-pix_fmt", "yuv420p")
        return [cmd, retry] if retry else [cmd]
    return [cmd]


def run_ffmpeg_with_candidates(
    candidates: List[List[str]],
    out_final: Path,
    *,
    overwrite: bool,
    dry_run: bool,
) -> Tuple[bool, str, bool]:
    if dry_run:
        for c in candidates:
            log("[dry-run] " + " ".join(c))
        return True, "dry-run", False

    if out_final.exists() and not overwrite:
        return True, f"output exists: {out_final}", False

    if _DEBUG_ENABLED:
        _LOGGER.debug("ffmpeg candidates=%d out=%s", len(candidates), out_final)

    # 保留原始扩展名，避免 ffmpeg 因未知扩展无法推断格式
    suffix = out_final.suffix
    tmp_name = out_final.stem + ".__tmp__" + suffix
    out_tmp = out_final.with_name(tmp_name)
    ensure_parent(out_tmp)
    last_err = ""

    for idx, cmd in enumerate(candidates):
        cmd2 = replace_last(cmd, str(out_tmp))
        if _DEBUG_ENABLED:
            _LOGGER.debug("ffmpeg run %d/%d: %s", idx + 1, len(candidates), shlex.join(cmd2))

        try:
            if out_tmp.exists():
                out_tmp.unlink()
        except Exception:
            pass

        # ffmpeg 可能输出非 UTF-8 字符（例如携带非 UTF-8 元数据），避免 decode 失败
        cp = subprocess.run(
            cmd2,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if cp.returncode == 0:
            if _DEBUG_ENABLED:
                _LOGGER.debug("ffmpeg ok %d/%d -> %s", idx + 1, len(candidates), out_tmp)
            try:
                if out_final.exists():
                    if overwrite:
                        out_final.unlink()
                    else:
                        try:
                            out_tmp.unlink()
                        except Exception:
                            pass
                        return True, f"output exists: {out_final}", False
                ensure_parent(out_final)
                out_tmp.replace(out_final)
                return True, "ok", True
            except Exception as e:
                try:
                    if out_tmp.exists():
                        out_tmp.unlink()
                except Exception:
                    pass
                return False, f"rename failed: {e}", False
        else:
            try:
                if out_tmp.exists():
                    out_tmp.unlink()
            except Exception:
                pass
            cmd_s = shlex.join(cmd2)
            rc_s = describe_returncode(cp.returncode)
            stderr_tail = tail_text(cp.stderr, n_lines=60, max_chars=6000)
            last_err = f"cmd: {cmd_s}\nresult: {rc_s}\n{stderr_tail}".strip()
            if _DEBUG_ENABLED:
                _LOGGER.debug("ffmpeg fail %d/%d exit=%d: %s", idx + 1, len(candidates), cp.returncode, last_err)

    return False, (last_err or "ffmpeg failed"), False


# ------------------------
# 漫画/压缩包处理
# ------------------------

def sevenzip_password_args(password: Optional[str]) -> List[str]:
    if not password:
        return []
    return [f"-p{password}"]


def mask_cmd(cmd: List[str], password: Optional[str]) -> List[str]:
    if not password:
        return cmd
    out = []
    for a in cmd:
        if a == f"-p{password}":
            out.append("-p***")
        else:
            out.append(a)
    return out


def list_archive_paths(archiver: str, archive: Path, *, password: Optional[str], dry_run: bool) -> Optional[List[str]]:
    cmd = [archiver, "l", "-slt"] + sevenzip_password_args(password) + [str(archive)]
    if dry_run:
        log("[dry-run] " + " ".join(mask_cmd(cmd, password)))
        return []
    if _DEBUG_ENABLED:
        _LOGGER.debug("[7z] list: %s", " ".join(mask_cmd(cmd, password)))
    cp = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if _DEBUG_ENABLED:
        _LOGGER.debug("[7z] list exit=%d archive=%s", cp.returncode, archive)
    if cp.returncode != 0:
        return None
    paths: List[str] = []
    for line in (cp.stdout or "").splitlines():
        line = line.strip()
        if line.startswith("Path = "):
            paths.append(line[len("Path = ") :].strip())
    return [p for p in paths if p and p not in {".", "/"}]


def extract_archive(archiver: str, archive: Path, out_dir: Path, *, password: Optional[str], overwrite: bool, dry_run: bool) -> bool:
    ensure_parent(out_dir / "x")
    ao = "-aoa" if overwrite else "-aos"
    cmd = [archiver, "x", "-y", ao] + sevenzip_password_args(password) + [f"-o{str(out_dir)}", str(archive)]
    if dry_run:
        log("[dry-run] " + " ".join(mask_cmd(cmd, password)))
        return True
    if _DEBUG_ENABLED:
        _LOGGER.debug("[7z] extract: %s", " ".join(mask_cmd(cmd, password)))
    cp = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if _DEBUG_ENABLED:
        _LOGGER.debug("[7z] extract exit=%d archive=%s", cp.returncode, archive)
    return cp.returncode == 0


def natural_key(s: str) -> List[Any]:
    parts = re.split(r"(\\d+)", s)
    out: List[Any] = []
    for p in parts:
        if p.isdigit():
            out.append(int(p))
        else:
            out.append(p.lower())
    return out


def is_image_file(p: Path) -> bool:
    return p.suffix.lower() in IMAGE_EXTS or p.suffix.lower() in ANIMATED_IMAGE_EXTS


def collect_files(root: Path) -> List[Path]:
    return [p for p in root.rglob("*") if p.is_file()]


def make_cbz(zip_path: Path, files: List[Tuple[Path, str]], *, overwrite: bool) -> None:
    ensure_parent(zip_path)
    if zip_path.exists():
        if overwrite:
            zip_path.unlink()
        else:
            raise FileExistsError(str(zip_path))

    try:
        zf = zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9)  # type: ignore
    except TypeError:
        zf = zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED)

    with zf:
        for src, arcname in files:
            zf.write(src, arcname=arcname.replace(os.sep, "/"))


def comic_smart_skip_already_target(
    archiver: str,
    src: Path,
    *,
    password: Optional[str],
    min_images: int,
    target_ext: str,
    dry_run: bool,
) -> Tuple[bool, str]:
    if src.suffix.lower() != ".cbz":
        return False, "not cbz"
    paths = list_archive_paths(archiver, src, password=password, dry_run=dry_run)
    if paths is None:
        return False, "cannot list (password?)"
    imgs = [p for p in paths if Path(p).suffix.lower() in (IMAGE_EXTS | ANIMATED_IMAGE_EXTS)]
    if len(imgs) < min_images:
        return False, f"too few images ({len(imgs)})"
    non = [p for p in imgs if Path(p).suffix.lower() != target_ext]
    if not non:
        return True, f"all images are {target_ext.lstrip('.')}"
    return False, f"has non-{target_ext.lstrip('.')} images ({len(non)})"


def process_comic_to_cbz(
    src: Path,
    out_cbz: Path,
    *,
    archiver: str,
    password: Optional[str],
    image_codec: str,
    out_name_mode: str,
    webp_quality: int,
    webp_lossless: bool,
    avif_crf: int,
    avif_pix_fmt: str,
    detect_min_images: int,
    keep_non_images: bool,
    overwrite: bool,
    dry_run: bool,
) -> Tuple[bool, str]:
    target_ext = ".webp" if image_codec == "webp" else ".avif"

    with tempfile.TemporaryDirectory(prefix="comic_extract_") as td:
        root = Path(td)
        extracted = root / "extracted"
        converted = root / "converted"
        extracted.mkdir(parents=True, exist_ok=True)
        converted.mkdir(parents=True, exist_ok=True)

        if not extract_archive(archiver, src, extracted, password=password, overwrite=True, dry_run=dry_run):
            return False, "7z extract failed (password?)"

        all_files = collect_files(extracted)
        img_files = [p for p in all_files if is_image_file(p)]

        if len(img_files) < detect_min_images and src.suffix.lower() not in COMIC_EXTS:
            return False, f"not a comic archive (images={len(img_files)})"

        files_to_zip: List[Tuple[Path, str]] = []

        if keep_non_images:
            for p in all_files:
                if not is_image_file(p):
                    files_to_zip.append((p, str(p.relative_to(extracted))))

        img_files_sorted = sorted(img_files, key=lambda p: natural_key(str(p.relative_to(extracted))))

        # 同一目录内若存在 1.jpg + 1.png 这类“转码后同名”的情况，避免覆盖：
        # - collision：1.jpg/1.png -> 1.webp；撞名时：1__jpg.webp / 1__png.webp（必要时加 __2/__3...）
        # - suffix：默认：1.jpg/1.png -> 1__jpg.webp / 1__png.webp；撞名时再追加 __2/__3...
        folder_overrides: Dict[Path, Dict[str, str]] = {}
        folder_names: Dict[Path, List[str]] = {}
        for p in img_files_sorted:
            ext = p.suffix.lower()
            if ext in ANIMATED_IMAGE_EXTS:
                continue
            if ext not in IMAGE_EXTS:
                continue
            rel = p.relative_to(extracted)
            folder_names.setdefault(rel.parent, []).append(rel.name)
        for parent, names in folder_names.items():
            ov = compute_image_out_name_overrides(names, image_target_ext=target_ext, out_name_mode=out_name_mode)
            if ov:
                folder_overrides[parent] = ov

        for p in img_files_sorted:
            rel = p.relative_to(extracted)
            ext = p.suffix.lower()

            if ext in ANIMATED_IMAGE_EXTS:
                files_to_zip.append((p, str(rel)))
                continue

            if ext == target_ext:
                files_to_zip.append((p, str(rel)))
                continue

            if out_name_mode == "suffix":
                out_rel = rel.with_name(build_suffixed_target_name(rel.name, target_ext=target_ext))
            else:
                out_rel = rel.with_suffix(target_ext)
            ov = folder_overrides.get(rel.parent)
            if ov:
                out_name = ov.get(rel.name)
                if out_name:
                    out_rel = rel.with_name(out_name)
            out_img = converted / out_rel
            ensure_parent(out_img)

            candidates = build_image_candidates(
                p,
                out_img,
                image_codec=image_codec,
                webp_quality=webp_quality,
                webp_lossless=webp_lossless,
                avif_crf=avif_crf,
                avif_pix_fmt=avif_pix_fmt,
                src_pix_fmt=None,
            )
            ok, _err, _wrote = run_ffmpeg_with_candidates(candidates, out_img, overwrite=True, dry_run=dry_run)
            if not ok:
                files_to_zip.append((p, str(rel)))
                continue

            files_to_zip.append((out_img, str(out_rel)))

        if dry_run:
            log("[dry-run] would create " + str(out_cbz))
            return True, "dry-run ok"

        make_cbz(out_cbz, files_to_zip, overwrite=overwrite)
        return True, f"cbz created (images={len(img_files_sorted)}, fmt={target_ext.lstrip('.')})"


# ------------------------
# 远端上传
# ------------------------


def _openlist_info_is_missing(info: Any) -> bool:
    # OpenList 的 fs.info 在“对象不存在”时可能不会抛异常，而是返回“空对象”（取决于 openlist 客户端版本）。
    name = getattr(info, "name", None)
    path = getattr(info, "path", None)
    sign0 = getattr(info, "sign", None)
    size0 = getattr(info, "size", None)
    modified0 = getattr(info, "modified", None)
    return (
        name in (None, "")
        and path in (None, "")
        and sign0 in (None, "")
        and (size0 in (None, 0))
        and modified0 is None
    )


def _openlist_get_file_size(client: OpenListClientSync, path: str, *, strict: bool) -> Optional[int]:
    try:
        info = client.info(path)
    except FatalAuthError:
        raise
    except Exception as e:
        msg = str(e).lower()
        if "not found" in msg or "object not found" in msg:
            return None
        if strict:
            raise RuntimeError(f"remote info failed: {e}") from e
        return None
    if info is None or _openlist_info_is_missing(info):
        return None
    if bool(getattr(info, "is_dir", False)):
        raise RuntimeError(f"remote path is a directory: {path}")
    return int(getattr(info, "size", 0) or 0)


def _openlist_autorename_index(name: str, *, stem: str, ext: str) -> Optional[int]:
    # OpenList 常见的撞名改写："<stem> 1<ext>" 或 "<stem> (1)<ext>"
    if ext and not name.endswith(ext):
        return None
    base = name[: -len(ext)] if ext else name
    m = re.fullmatch(re.escape(stem) + r"(?: \((\d+)\)| (\d+))", base)
    if not m:
        return None
    token = m.group(1) or m.group(2)
    if not token:
        return None
    try:
        n = int(token)
    except ValueError:
        return None
    return n if n > 0 else None


def _openlist_find_autorename_candidate(
    client: OpenListClientSync,
    *,
    dst_path: str,
    expected_size: int,
) -> Optional[str]:
    parent = posixpath.dirname(dst_path.rstrip("/")) or "/"
    base = posixpath.basename(dst_path.rstrip("/"))
    stem = Path(base).stem
    ext = Path(base).suffix

    best: Tuple[int, int, str] | None = None  # (mtime_ns, idx, name)

    page = 1
    got = 0
    while True:
        res = client.listdir(parent, refresh=True, per_page=100, page=page)
        content = getattr(res, "content", []) or []
        for obj in content:
            name = str(getattr(obj, "name", "") or "")
            if not name:
                continue
            if bool(getattr(obj, "is_dir", False)):
                continue
            idx = _openlist_autorename_index(name, stem=stem, ext=ext)
            if idx is None:
                continue
            sz = int(getattr(obj, "size", 0) or 0)
            if sz != expected_size:
                continue
            mtime_ns = _mtime_to_ns(getattr(obj, "modified", None))
            cand = (mtime_ns, idx, name)
            if best is None or cand > best:
                best = cand
        got += len(content)
        total = int(getattr(res, "total", 0) or 0)
        if not content or (total > 0 and got >= total):
            break
        page += 1

    if best is None:
        return None
    return remote_join(parent, best[2])


def _openlist_try_fix_autorename_upload(
    client: OpenListClientSync,
    *,
    local_size: int,
    dst_path: str,
) -> Optional[str]:
    # 处理服务端自动改名导致的“实际文件名 != dst_path”。
    cand_path = _openlist_find_autorename_candidate(client, dst_path=dst_path, expected_size=local_size)
    if not cand_path:
        return None

    # 如果 dst_path 其实已经存在且 size 正确（例如 info 缓存/延迟），直接视为成功。
    dst_sz = _openlist_get_file_size(client, dst_path, strict=False)
    if dst_sz == local_size:
        return dst_path

    try:
        client.rename(cand_path, dst_path)
    except FatalAuthError:
        raise
    except Exception:
        # 无法改回预期文件名（OpenList 幽灵占坑等问题）：接受服务端自动改名后的文件名。
        return cand_path if _openlist_get_file_size(client, cand_path, strict=False) == local_size else None

    if _openlist_get_file_size(client, dst_path, strict=False) == local_size:
        return dst_path
    # rename 成功但 info/list 缓存未更新：退一步用 cand_path（至少保证 state 与实际可见文件一致）
    return cand_path if _openlist_get_file_size(client, cand_path, strict=False) == local_size else None


def _rewrite_out_rel_for_uploaded_path(out_rel: str, uploaded_path: str) -> str:
    try:
        uploaded_name = posixpath.basename(uploaded_path.rstrip("/"))
        if not uploaded_name:
            return out_rel
        if uploaded_name == posixpath.basename(out_rel):
            return out_rel
        orig_dir = posixpath.dirname(out_rel)
        return posixpath.join(orig_dir, uploaded_name) if orig_dir else uploaded_name
    except Exception:
        return out_rel


def upload_file_remote(
    client: OpenListClientSync,
    local_file: Path,
    dst_path: str,
    *,
    overwrite: bool,
    cancel_event: Optional[threading.Event] = None,
) -> str:
    if cancel_event is not None and cancel_event.is_set():
        raise cf.CancelledError()
    parent = posixpath.dirname(dst_path.rstrip("/")) or "/"
    client.ensure_dir(parent)
    size = local_file.stat().st_size

    # overwrite=False 时，OpenList 可能会在“目标已存在”时自动改名（例如追加 " 1"），导致 state 记录与实际文件名不一致。
    # 这里做幂等处理：
    # - 目标已存在且 size 相等：视为已上传，跳过。
    # - 目标已存在但 size 更小：视为疑似失败残留，尝试删除；若仍存在则强制 overwrite 上传，避免服务端改名。
    remote_size0 = _openlist_get_file_size(client, dst_path, strict=False)
    dst_exists = remote_size0 is not None
    if not overwrite and remote_size0 is not None:
        if remote_size0 == size:
            if _DEBUG_ENABLED:
                _LOGGER.debug("upload skip (exists, same size) dst=%s size=%s", dst_path, fmt_bytes(size))
            return dst_path
        if remote_size0 > size:
            raise RuntimeError(
                f"upload conflict: dst exists and is larger than local (dst={dst_path} remote={fmt_bytes(remote_size0)} local={fmt_bytes(size)}); "
                "use --overwrite or remove remote file first"
            )
        if _DEBUG_ENABLED:
            _LOGGER.debug(
                "upload conflict (exists, size smaller) dst=%s remote=%s local=%s -> cleanup+overwrite",
                dst_path,
                fmt_bytes(remote_size0),
                fmt_bytes(size),
            )
        try:
            client.remove(dst_path)
        except FatalAuthError:
            raise
        except Exception:
            pass
        # 触发一次 refresh，提示服务端更新缓存
        try:
            client.listdir(parent, refresh=True, per_page=1)
        except FatalAuthError:
            raise
        except Exception:
            pass
        remote_size1 = _openlist_get_file_size(client, dst_path, strict=False)
        dst_exists = remote_size1 is not None
        if dst_exists:
            overwrite = True

    if _DEBUG_ENABLED:
        _LOGGER.debug("upload prepare dst=%s size=%s overwrite=%s", dst_path, fmt_bytes(size), overwrite)

    did_upload = False
    if size > 95 * 1024 * 1024 and not dst_exists:
        try:
            if _DEBUG_ENABLED:
                _LOGGER.debug("upload try direct (>=95MiB)")
            ok = client.direct_upload_if_available(dst_path, local_file, overwrite=overwrite)
            if ok:
                if _DEBUG_ENABLED:
                    _LOGGER.debug("upload direct ok")
                did_upload = True
        except FatalAuthError:
            raise
        except Exception as e:
            if isinstance(e, cf.CancelledError) or (cancel_event is not None and cancel_event.is_set()):
                raise
            log_err(f"[WARN] direct upload failed, fallback normal: {e!r}")
    if not did_upload:
        if cancel_event is not None and cancel_event.is_set():
            raise cf.CancelledError()
        if _DEBUG_ENABLED:
            _LOGGER.debug("upload try normal")
        client.upload_file(dst_path, local_file, overwrite=overwrite)
        if _DEBUG_ENABLED:
            _LOGGER.debug("upload normal ok")

    # best-effort verify：确保最终文件确实落在 dst_path（未被服务端自动改名），且 size 匹配。
    # OpenList 可能有目录缓存，先 refresh 再重试几次。
    last_sz: Optional[int] = None
    for attempt in range(3):
        if cancel_event is not None and cancel_event.is_set():
            raise cf.CancelledError()
        last_sz = _openlist_get_file_size(client, dst_path, strict=True)
        if last_sz == size:
            return dst_path
        if last_sz is None or last_sz < size:
            if _DEBUG_ENABLED:
                _LOGGER.debug(
                    "upload verify mismatch dst=%s size=%s remote=%s -> try rename-fix",
                    dst_path,
                    fmt_bytes(size),
                    fmt_bytes(-1 if last_sz is None else last_sz),
                )
            try:
                fixed = _openlist_try_fix_autorename_upload(client, local_size=size, dst_path=dst_path)
                if fixed:
                    if _DEBUG_ENABLED:
                        _LOGGER.debug("upload rename-fix ok dst=%s actual=%s", dst_path, fixed)
                    return fixed
            except FatalAuthError:
                raise
            except Exception as e:
                if _DEBUG_ENABLED:
                    _LOGGER.debug("upload rename-fix failed dst=%s err=%r", dst_path, e)
        try:
            client.listdir(parent, refresh=True, per_page=1)
        except FatalAuthError:
            raise
        except Exception:
            pass
        time.sleep(0.2 * (attempt + 1))
    if last_sz is None:
        raise RuntimeError(f"upload verify failed: dst not found: {dst_path}")
    raise RuntimeError(
        f"upload verify failed: dst size mismatch: expected={fmt_bytes(size)} got={fmt_bytes(last_sz)} dst={dst_path}"
    )


# ------------------------
# 单文件本地处理
# ------------------------

@dataclass
class JobResult:
    ok: bool
    action: str
    msg: str
    out_local: Optional[Path] = None
    out_rel: Optional[str] = None


def copy_file_local(src: Path, dst: Path, *, overwrite: bool) -> None:
    ensure_parent(dst)
    if dst.exists() and not overwrite:
        return
    shutil.copy2(src, dst)


def process_one_local(
    src_local: Path,
    in_root: Path,
    out_root: Path,
    *,
    container: str,
    video_policy: str,
    audio_policy: str,
    allow_opus_in_mp4: bool,
    video_encoder: str,
    video_crf: int,
    video_preset: str,
    pix_fmt: str,
    image_codec: str,
    webp_quality: int,
    webp_lossless: bool,
    avif_crf: int,
    avif_pix_fmt: str,
    faststart: bool,
    overwrite: bool,
    dry_run: bool,
    min_savings: float,
    try_archives: bool,
    comic_min_images: int,
    comic_keep_non_images: bool,
    comic_accept_bigger: bool,
    archive_password: Optional[str],
    out_name_mode: str,
    rel_override: Optional[str] = None,
    out_rel_override: Optional[str] = None,
    src_size_hint: int = 0,
) -> JobResult:
    rel = rel_override or src_local.relative_to(in_root).as_posix()
    src_arg = str(src_local)
    dst_base = out_root / rel

    def apply_out_override(p: Path) -> Path:
        if not out_rel_override:
            return p
        try:
            ov = Path(out_rel_override)
            if ov.is_absolute() or (".." in ov.parts):
                return p
            if ov.suffix.lower() != p.suffix.lower():
                return p
            return out_root / ov
        except Exception:
            return p

    def ensure_local() -> Optional[str]:
        if src_local.exists():
            return None
        return "no local file available"

    def src_size_val() -> int:
        if src_local.exists():
            return size_of(src_local)
        return src_size_hint

    def _ffmpeg_fail_msg(err: str) -> str:
        rc = extract_result_from_ffmpeg_err(err)
        return f"ffmpeg failed ({rc}) -> copied original" if rc else "ffmpeg failed -> copied original"

    def _transcode_or_copy(candidates: List[List[str]], out_final: Path) -> JobResult:
        ok, err, wrote = run_ffmpeg_with_candidates(candidates, out_final, overwrite=overwrite, dry_run=dry_run)
        if not ok:
            if dry_run:
                return JobResult(True, "dry-run", "ffmpeg failed -> would copy", None, rel)
            try:
                if overwrite and out_final.exists():
                    out_final.unlink()
            except Exception:
                pass
            err2 = ensure_local()
            if err2:
                return JobResult(False, "fail", err2)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", _ffmpeg_fail_msg(err), dst_base, rel)

        if dry_run:
            return JobResult(True, "dry-run", "ok", None, out_final.relative_to(out_root).as_posix())
        if not wrote:
            return JobResult(
                True,
                "copy",
                f"output exists -> kept: {out_final}",
                out_final,
                out_final.relative_to(out_root).as_posix(),
            )

        src_sz = src_size_val()
        out_sz = size_of(out_final)
        if src_sz > 0 and out_sz > 0 and out_sz >= src_sz * (1.0 - min_savings):
            try:
                out_final.unlink()
            except Exception:
                pass
            err3 = ensure_local()
            if err3:
                return JobResult(False, "fail", err3)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(
                True,
                "copy",
                f"not enough savings ({fmt_size_change(src_sz, out_sz)}) -> copied original",
                dst_base,
                rel,
            )

        return JobResult(True, "ok", fmt_size_change(src_sz, out_sz), out_final, out_final.relative_to(out_root).as_posix())

    probe = None
    info0 = classify(src_local, None)
    if info0.kind in {"video", "audio", "image"}:
        probe = ffprobe_json(src_arg, dry_run=dry_run)
    info = classify(src_local, probe)

    # subtitle/other -> copy
    if info.kind in {"subtitle", "other"}:
        if dry_run:
            return JobResult(True, "dry-run", f"{info.kind} would copy", None, rel)
        err = ensure_local()
        if err:
            return JobResult(False, "fail", err)
        copy_file_local(src_local, dst_base, overwrite=overwrite)
        return JobResult(True, "copy", f"{info.kind} copied", dst_base, rel)

    # comic/archive
    if info.kind == "comic":
        if not try_archives:
            if dry_run:
                return JobResult(True, "dry-run", "comic try disabled -> would copy", None, rel)
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", "comic copied (try disabled)", dst_base, rel)

        err = ensure_local()
        if err:
            return JobResult(False, "fail", err)
        archiver = find_7z()
        if not archiver:
            return JobResult(False, "fail", "missing 7z/7zz")

        target_ext = ".webp" if image_codec == "webp" else ".avif"
        dst_cbz = (out_root / rel).with_suffix(".cbz")
        dst_cbz = apply_out_name_mode(dst_cbz, src_rel=rel, target_ext=".cbz", out_name_mode=out_name_mode)
        dst_cbz = apply_out_override(dst_cbz)

        # smart-skip
        try:
            skip, reason = comic_smart_skip_already_target(
                archiver,
                src_local,
                password=archive_password,
                min_images=comic_min_images,
                target_ext=target_ext,
                dry_run=dry_run,
            )
        except Exception:
            skip, reason = (False, "skip-check error")

        if skip:
            if dry_run:
                return JobResult(True, "dry-run", f"comic smart-skip: {reason}", None, dst_cbz.relative_to(out_root).as_posix())
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", f"comic smart-skip -> copied original ({reason})", dst_base, rel)

        ensure_parent(dst_cbz)
        ok, msg = process_comic_to_cbz(
            src_local,
            dst_cbz,
            archiver=archiver,
            password=archive_password,
            image_codec=image_codec,
            out_name_mode=out_name_mode,
            webp_quality=webp_quality,
            webp_lossless=webp_lossless,
            avif_crf=avif_crf,
            avif_pix_fmt=avif_pix_fmt,
            detect_min_images=comic_min_images,
            keep_non_images=comic_keep_non_images,
            overwrite=True,
            dry_run=dry_run,
        )
        if not ok and msg.startswith("not a comic archive"):
            if not try_archives:
                if dry_run:
                    return JobResult(True, "dry-run", "archive try disabled -> would copy", None, rel)
                err = ensure_local()
                if err:
                    return JobResult(False, "fail", err)
                copy_file_local(src_local, dst_base, overwrite=overwrite)
                return JobResult(True, "copy", "archive copied (try disabled)", dst_base, rel)
            if dry_run:
                return JobResult(True, "dry-run", msg, None, rel)
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", msg, dst_base, rel)

        if not ok:
            return JobResult(False, "fail", msg)

        if dry_run:
            return JobResult(True, "dry-run", msg, None, dst_cbz.relative_to(out_root).as_posix())

        src_sz = src_size_val()
        out_sz = size_of(dst_cbz)
        if src_sz > 0 and out_sz > 0 and out_sz >= src_sz * (1.0 - min_savings) and not comic_accept_bigger:
            try:
                dst_cbz.unlink()
            except Exception:
                pass
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", f"{msg}; not smaller -> copied original", dst_base, rel)

        return JobResult(True, "ok", f"{msg}; size {src_sz}->{out_sz}", dst_cbz, dst_cbz.relative_to(out_root).as_posix())

    # image already target -> copy
    image_target_ext = ".webp" if image_codec == "webp" else ".avif"
    if info.kind == "image" and src_local.suffix.lower() == image_target_ext:
        if dry_run:
            return JobResult(True, "dry-run", f"already {image_target_ext}", None, rel)
        err = ensure_local()
        if err:
            return JobResult(False, "fail", err)
        copy_file_local(src_local, dst_base, overwrite=overwrite)
        return JobResult(True, "copy", f"already {image_target_ext.lstrip('.')}, copied", dst_base, rel)

    if info.kind == "video":
        has_subs, mp4_sub_ok = detect_subtitle_compat(info.streams)
        container2 = container
        if container2 == "auto":
            container2 = "mp4"
        if container2 == "mp4" and has_subs and not mp4_sub_ok:
            container2 = "mkv"
        out_final = dst_base.with_suffix("." + container2)
        out_final = apply_out_name_mode(out_final, src_rel=rel, target_ext=out_final.suffix, out_name_mode=out_name_mode)
        out_final = apply_out_override(out_final)

        candidates = build_video_candidates(
            src_arg,
            out_final,
            info,
            container_pref=container,
            video_policy=video_policy,
            audio_policy=audio_policy,
            allow_opus_in_mp4=allow_opus_in_mp4,
            video_encoder=video_encoder,
            video_crf=video_crf,
            video_preset=video_preset,
            pix_fmt=pix_fmt,
            faststart=faststart,
        )
        return _transcode_or_copy(candidates, out_final)

    if info.kind == "audio":
        out_final = dst_base.with_suffix(".opus" if audio_policy != "always_copy" else src_local.suffix)
        out_final = apply_out_name_mode(out_final, src_rel=rel, target_ext=out_final.suffix, out_name_mode=out_name_mode)
        out_final = apply_out_override(out_final)
        cmds = build_audio_candidates(src_arg, out_final, info, audio_policy=audio_policy)
        if not cmds:
            if dry_run:
                return JobResult(True, "dry-run", "no audio stream -> copy", None, rel)
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", "no audio stream -> copied", dst_base, rel)

        return _transcode_or_copy(cmds, out_final)

    if info.kind == "image":
        v = get_main_video_stream(info)
        src_pf = str(v.get("pix_fmt")) if (v and v.get("pix_fmt")) else None
        out_final = dst_base.with_suffix(image_target_ext)
        out_final = apply_out_name_mode(out_final, src_rel=rel, target_ext=out_final.suffix, out_name_mode=out_name_mode)
        out_final = apply_out_override(out_final)

        candidates = build_image_candidates(
            src_arg,
            out_final,
            image_codec=image_codec,
            webp_quality=webp_quality,
            webp_lossless=webp_lossless,
            avif_crf=avif_crf,
            avif_pix_fmt=avif_pix_fmt,
            src_pix_fmt=src_pf,
        )
        return _transcode_or_copy(candidates, out_final)

    if dry_run:
        return JobResult(True, "dry-run", "fallback copy", None, rel)
    err = ensure_local()
    if err:
        return JobResult(False, "fail", err)
    copy_file_local(src_local, dst_base, overwrite=overwrite)
    return JobResult(True, "copy", "fallback copied", dst_base, rel)


# ------------------------
# CLI / main
# ------------------------

def _env_nonempty(key: str) -> Optional[str]:
    v = os.environ.get(key)
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def _env_argv() -> List[str]:
    js = _env_nonempty("SHRINK_MEDIA_ARGV_JSON")
    if js:
        try:
            v = json.loads(js)
        except Exception as e:
            log_err(f"ERROR: invalid JSON in SHRINK_MEDIA_ARGV_JSON: {e}")
            sys.exit(2)
        if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
            log_err("ERROR: SHRINK_MEDIA_ARGV_JSON must be a JSON array of strings.")
            sys.exit(2)
        return [str(x) for x in v]

    s = _env_nonempty("SHRINK_MEDIA_ARGS")
    if not s:
        return []
    try:
        return shlex.split(s)
    except ValueError as e:
        log_err(f"ERROR: invalid SHRINK_MEDIA_ARGS (shell-like string): {e}")
        sys.exit(2)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="shrink_media: local/OpenList + multi-device state+locks")
    ap.add_argument("input", type=str, nargs="?", default=None, help="输入（本地路径或 OpenList URL）")
    ap.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="输出（本地路径或 OpenList URL）。缺省：本地输入用同级 <name>__compressed；远端输入用同级 <name>__compressed 目录。",
    )
    ap.add_argument("--inplace", action="store_true", help="原地替换（不推荐远端多设备并发）")
    ap.add_argument(
        "--out-name-mode",
        choices=["suffix", "collision"],
        default="suffix",
        help="输出文件命名：suffix=默认追加 __<src_ext>（例如 1.jpg->1__jpg.webp）；collision=仅在撞名时追加（旧逻辑）。",
    )

    ap.add_argument("--container", choices=["mp4", "mkv", "auto"], default="auto")
    ap.add_argument("--faststart", action="store_true", default=True)
    ap.add_argument("--no-faststart", dest="faststart", action="store_false")

    ap.add_argument("--video-policy", choices=["copy_if_hevc", "always_hevc", "always_copy"], default="copy_if_hevc")
    ap.add_argument("--audio-policy", choices=["copy_if_lossy", "always_opus", "always_copy"], default="copy_if_lossy")
    ap.add_argument("--allow-opus-in-mp4", action="store_true")

    ap.add_argument(
        "--video-encoder",
        choices=["auto", "hevc_nvenc", "libx265", "libx264"],
        default="auto",
        help="auto 优先 hevc_nvenc，失败自动回退 libx265/libx264",
    )
    ap.add_argument("--video-crf", type=int, default=22, help="x265/x264 为 CRF；NVENC 为 QP（越小越清晰）")
    ap.add_argument("--video-preset", type=str, default="slow")
    ap.add_argument("--pix-fmt", type=str, default="auto", help="auto 尽量保留源 pix_fmt；失败回退 yuv420p")

    ap.add_argument("--image-codec", choices=["webp", "avif"], default="webp", help="图片默认 webp；需要 avif 用此切换")
    ap.add_argument("--webp-quality", type=int, default=80)
    ap.add_argument("--webp-lossless", action="store_true")
    ap.add_argument("--image-crf", type=int, default=30, help="avif crf（仅 image-codec=avif）")
    ap.add_argument("--image-pix-fmt", type=str, default="auto", help="avif pix_fmt（auto/显式）")

    ap.add_argument("--min-savings", type=float, default=0.01)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-j", "--jobs", type=int, default=1)

    ap.add_argument("--try-archives", action="store_true", default=True)
    ap.add_argument("--no-try-archives", dest="try_archives", action="store_false")
    ap.add_argument("--comic-detect-min-images", type=int, default=3)
    ap.add_argument("--comic-keep-non-images", action="store_true", default=True)
    ap.add_argument("--comic-drop-non-images", dest="comic_keep_non_images", action="store_false")
    ap.add_argument("--comic-accept-bigger", action="store_true", default=True)
    ap.add_argument("--comic-no-accept-bigger", dest="comic_accept_bigger", action="store_false")

    ap.add_argument("--archive-password", type=str, default=None)
    ap.add_argument("--debug", action="store_true", help="verbose debug logging for pipeline, locks, IO")
    ap.add_argument("--prefetch-debug", action="store_true", help="log detailed prefetch events")
    ap.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="将所有日志写入文件（替代 stdout/stderr）",
    )
    ap.add_argument("--log-append", action="store_true", help="追加写入 --log-file（默认覆盖）")

    # openlist
    ap.add_argument("--openlist-user", type=str, default=None)
    ap.add_argument("--openlist-pass", type=str, default=None)
    ap.add_argument("--openlist-otp", type=str, default=None)
    ap.add_argument("--openlist-timeout", type=int, default=None)
    ap.add_argument("--retries", type=int, default=3, help="OpenList 网络失败重试次数（0 表示不重试）")
    ap.add_argument("--retry-backoff", type=float, default=0.5, help="OpenList 重试退避基准秒数（指数退避）")
    ap.add_argument(
        "--prefetch",
        type=int,
        default=0,
        help="预取远端输入文件的并发数（仅远端输入；0 表示关闭）",
    )
    ap.add_argument(
        "--upload-jobs",
        type=int,
        default=0,
        help="异步上传远端输出的并发数（仅远端输出；0 表示在 worker 内同步上传）",
    )

    # multi-device
    ap.add_argument("--device-id", type=str, default=None)
    ap.add_argument("--lock-ttl", type=int, default=6 * 3600)
    ap.add_argument("--no-steal-stale-lock", dest="steal_stale_lock", action="store_false", default=True)

    cli_argv = sys.argv[1:] if argv is None else argv
    env_argv = _env_argv()
    args = ap.parse_args(env_argv + cli_argv)

    # 从环境变量补齐常用参数（命令行优先）。
    if args.input is None:
        args.input = _env_nonempty("SHRINK_MEDIA_INPUT")
    if args.output is None:
        args.output = _env_nonempty("SHRINK_MEDIA_OUTPUT")

    if args.openlist_user is None:
        args.openlist_user = _env_nonempty("SHRINK_MEDIA_OPENLIST_USER") or _env_nonempty("OPENLIST_USER")
    if args.openlist_pass is None:
        args.openlist_pass = _env_nonempty("SHRINK_MEDIA_OPENLIST_PASS") or _env_nonempty("OPENLIST_PASS")
    if args.openlist_otp is None:
        args.openlist_otp = _env_nonempty("SHRINK_MEDIA_OPENLIST_OTP") or _env_nonempty("OPENLIST_OTP")
    if args.openlist_timeout is None:
        t = _env_nonempty("SHRINK_MEDIA_OPENLIST_TIMEOUT") or _env_nonempty("OPENLIST_TIMEOUT")
        if t:
            try:
                args.openlist_timeout = int(t)
            except ValueError:
                ap.error("Invalid OPENLIST_TIMEOUT/SHRINK_MEDIA_OPENLIST_TIMEOUT: must be int seconds.")
        else:
            args.openlist_timeout = 60

    if args.archive_password is None:
        args.archive_password = _env_nonempty("SHRINK_MEDIA_ARCHIVE_PASSWORD") or _env_nonempty("ARCHIVE_PASSWORD")

    if args.retries < 0:
        ap.error("--retries must be >= 0")
    if args.retry_backoff < 0:
        ap.error("--retry-backoff must be >= 0")

    if args.device_id is None:
        args.device_id = _env_nonempty("SHRINK_MEDIA_DEVICE_ID") or _env_nonempty("DEVICE_ID")

    if args.input is None:
        ap.error("Missing input: provide positional `input` or set env `SHRINK_MEDIA_INPUT`.")

    return args


def main() -> None:
    args = parse_args()
    configure_logging(
        args.log_file,
        append=args.log_append,
        debug=bool(args.debug),
        prefetch_debug=bool(args.prefetch_debug),
    )
    # --help 已在 parse_args 内提前退出；放在这里确保显示帮助不依赖外部工具
    require_tools()

    if args.inplace and args.output:
        log_err("ERROR: --inplace 与 --output 不能同时使用。")
        sys.exit(2)

    # Windows 无 os.uname；platform.node/COMPUTERNAME 作为回退
    if args.device_id:
        device_id = args.device_id
    else:
        try:
            device_id = os.uname().nodename  # type: ignore[attr-defined]
        except AttributeError:
            device_id = platform.node() or os.environ.get("COMPUTERNAME") or "unknown"

    remote_client: Optional[OpenListClientSync] = None
    remote_base: Optional[str] = None
    if is_url(args.input) or (args.output and is_url(args.output)):
        base_candidate = args.input if is_url(args.input) else args.output
        remote_base, _ = parse_remote_location(base_candidate)
        if not args.openlist_user or not args.openlist_pass:
            log_err("ERROR: 远端操作需要 --openlist-user 与 --openlist-pass。")
            sys.exit(2)
        remote_client = OpenListClientSync(
            remote_base,
            args.openlist_user,
            args.openlist_pass,
            timeout=args.openlist_timeout,
            otp_key=args.openlist_otp,
            retries=args.retries,
            retry_backoff=args.retry_backoff,
        )
    # 同一服务器检查（目前仅支持单服务器）
    if is_url(args.input) and args.output and is_url(args.output):
        base_in, _ = parse_remote_location(args.input)
        base_out, _ = parse_remote_location(args.output)
        if base_in != base_out:
            log_err("ERROR: 当前版本仅支持同一 OpenList 服务器的输入/输出。")
            sys.exit(2)

    # 输入枚举
    input_is_remote = is_url(args.input)
    if input_is_remote:
        if remote_client is None:
            log_err("ERROR: OpenList 输入需要远端客户端。")
            sys.exit(2)
        in_base, in_root_remote_path = parse_remote_location(args.input)
        in_root_remote_path, items_iter = iter_remote_inputs(
            remote_client,
            in_root_remote_path,
            image_codec=args.image_codec,
            container=args.container,
            audio_policy=args.audio_policy,
            out_name_mode=args.out_name_mode,
        )
        in_root_local: Optional[Path] = None
        in_root_display = f"{in_base}{in_root_remote_path}"
    else:
        in_path = Path(args.input).expanduser().resolve()
        if not in_path.exists():
            log_err("ERROR: 输入路径不存在。")
            sys.exit(2)
        in_root_local, items_iter = iter_local_inputs(
            in_path,
            image_codec=args.image_codec,
            container=args.container,
            audio_policy=args.audio_policy,
            out_name_mode=args.out_name_mode,
        )
        in_root_remote_path = None
        in_root_display = str(in_root_local)

    items_iter = iter(items_iter)
    try:
        first_item = next(items_iter)
    except StopIteration:
        log("没有找到可处理的文件。")
        return
    items_iter = itertools.chain([first_item], items_iter)

    # 输出位置
    out_is_remote = False
    out_root_local: Optional[Path] = None
    out_root_remote_path: Optional[str] = None
    out_root_display: Optional[str] = None

    if args.inplace:
        if input_is_remote:
            out_is_remote = True
            out_root_remote_path = in_root_remote_path
            out_root_display = f"{remote_base or ''}{out_root_remote_path or ''}"
        else:
            out_is_remote = False
            out_root_local = in_root_local
            out_root_display = str(out_root_local)
    else:
        if args.output is None:
            if input_is_remote:
                # 默认与本地一致：同级目录加 __compressed
                assert in_root_remote_path is not None
                path = in_root_remote_path.rstrip("/")
                parent, name = posixpath.split(path)
                if name == "":
                    name = "__compressed"
                else:
                    name = name + "__compressed"
                new_path = f"{parent}/{name}" if parent else f"/{name}"
                out_root_remote_path = new_path
                out_is_remote = True
                out_root_display = f"{remote_base or ''}{out_root_remote_path}"
            else:
                assert in_root_local is not None
                out_root_local = in_root_local.parent / (in_root_local.name + "__compressed")
                out_root_local.mkdir(parents=True, exist_ok=True)
                out_is_remote = False
                out_root_display = str(out_root_local)
        else:
            if is_url(args.output):
                out_is_remote = True
                base_out, path_out = parse_remote_location(args.output)
                if remote_client is None:
                    remote_base = base_out
                    remote_client = OpenListClientSync(
                        base_out,
                        args.openlist_user,
                        args.openlist_pass,
                        timeout=args.openlist_timeout,
                        otp_key=args.openlist_otp,
                        retries=args.retries,
                        retry_backoff=args.retry_backoff,
                    )
                out_root_remote_path = path_out if path_out.endswith("/") else path_out
                out_root_display = f"{base_out}{out_root_remote_path}"
            else:
                out_is_remote = False
                out_root_local = Path(args.output).expanduser().resolve()
                out_root_local.mkdir(parents=True, exist_ok=True)
                out_root_display = str(out_root_local)

    # state + locks
    if out_is_remote:
        assert remote_client is not None and out_root_remote_path is not None
        state_backend = StateBackendPerFile(
            remote_dir_path=remote_join(out_root_remote_path, STATE_DIR_NAME),
            client=remote_client,
            legacy_jsonl_path=remote_join(out_root_remote_path, STATE_DEFAULT_NAME),
        )
        lock_backend = LockBackend(
            local_dir=None,
            remote_dir_path=remote_join(out_root_remote_path, LOCKS_DIR_NAME),
            client=remote_client,
            device_id=device_id,
            ttl_sec=args.lock_ttl,
            steal_stale=args.steal_stale_lock,
        )
    else:
        assert out_root_local is not None
        state_backend = StateBackendJsonl(local_path=out_root_local / STATE_DEFAULT_NAME, remote_path=None, client=None)
        lock_backend = LockBackend(
            local_dir=out_root_local / LOCKS_DIR_NAME,
            remote_dir_path=None,
            client=None,
            device_id=device_id,
            ttl_sec=args.lock_ttl,
            steal_stale=args.steal_stale_lock,
        )

    coord = Coordinator(state_backend, lock_backend, device_id=device_id, ttl_sec=args.lock_ttl)

    def dbg(msg: str) -> None:
        if _DEBUG_ENABLED:
            _LOGGER.debug(msg)

    def pdebug(msg: str) -> None:
        if _PREFETCH_DEBUG_ENABLED or _DEBUG_ENABLED:
            _LOGGER.debug(f"[PREFETCH] {msg}")

    stop_event = threading.Event()
    fatal_mu = threading.Lock()
    fatal_error: Optional[FatalAuthError] = None

    def set_fatal(e: FatalAuthError) -> None:
        nonlocal fatal_error
        with fatal_mu:
            if fatal_error is None:
                fatal_error = e
        stop_event.set()

    def raise_if_fatal() -> None:
        with fatal_mu:
            e = fatal_error
        if e is not None:
            raise e

    # 异步上传（让“转码”与“上传”在不同线程池中流水线并行）
    upload_dir: Optional[Path] = None
    upload_executor: Optional[cf.ThreadPoolExecutor] = None
    upload_slots: Optional[threading.BoundedSemaphore] = None

    if out_is_remote and args.upload_jobs > 0 and remote_client is not None and not args.dry_run:
        upload_dir = Path(tempfile.mkdtemp(prefix="shrink_upload_"))
        dbg(f"upload dir: {upload_dir}")
        upload_executor = cf.ThreadPoolExecutor(max_workers=args.upload_jobs)
        # bounded backlog：最多允许 2x 并发数的上传任务“在途”（含排队与执行），避免输出在本地无限堆积
        upload_slots = threading.BoundedSemaphore(max(1, args.upload_jobs) * 2)

    # 远端预取
    prefetch_dir: Optional[Path] = None
    prefetch_futs: Dict[str, cf.Future[Tuple[bool, Optional[Path], str]]] = {}
    prefetch_results: Dict[str, Tuple[bool, Optional[Path], str]] = {}
    # Worker 选择直接 fut.result() 等待该条目时，用于阻止回调把结果塞进 prefetch_results
    # （否则会留下永远不被消费的 cached，导致 pump_prefetch() 因 cached>0 长期停摆）
    prefetch_claimed: set[str] = set()
    prefetch_executor: Optional[cf.ThreadPoolExecutor] = None
    prefetch_mu = threading.Lock()
    prefetch_pump_mu = threading.Lock()
    prefetch_candidates: deque[WorkItem] = deque()
    prefetch_candidates_mu = threading.Lock()

    def offer_prefetch(_it: WorkItem) -> None:
        return

    def pump_prefetch() -> None:
        return

    if input_is_remote and args.prefetch > 0 and remote_client is not None:
        prefetch_dir = Path(tempfile.mkdtemp(prefix="shrink_prefetch_"))
        dbg(f"prefetch dir: {prefetch_dir}")
        prefetch_executor = cf.ThreadPoolExecutor(max_workers=args.prefetch)
        max_cached = max(1, args.prefetch) * 2
        max_candidates = max(1, args.prefetch) * 8

        def offer_prefetch(it: WorkItem) -> None:
            if not it.is_remote or it.remote_path is None:
                return
            with prefetch_candidates_mu:
                if len(prefetch_candidates) >= max_candidates:
                    return
                prefetch_candidates.append(it)
            pump_prefetch()

        def submit_prefetch(it: WorkItem) -> None:
            if not it.is_remote or it.remote_path is None:
                return

            def _task() -> Tuple[bool, Optional[Path], str]:
                try:
                    target = prefetch_dir / it.rel
                    ensure_parent(target)
                    remote_client.download_to(it.remote_path, target)
                    return True, target, ""
                except FatalAuthError:
                    raise
                except Exception as e:
                    return False, None, str(e)

            fut = prefetch_executor.submit(_task)
            with prefetch_mu:
                prefetch_futs[it.rel] = fut
            pdebug(f"queued {it.rel}")

            def _done(f: cf.Future) -> None:
                try:
                    res = f.result()
                except FatalAuthError as e:
                    set_fatal(e)
                    return
                except Exception as e:
                    res = (False, None, str(e))
                with prefetch_mu:
                    prefetch_futs.pop(it.rel, None)
                    claimed = it.rel in prefetch_claimed
                    prefetch_claimed.discard(it.rel)
                    if claimed:
                        # Worker 正在等待/消费该 future；文件由 worker 负责清理，避免提前 unlink 导致 copy2 源文件丢失。
                        pass
                    elif len(prefetch_results) < max_cached:
                        prefetch_results[it.rel] = res
                    elif res[0] and res[1] is not None:
                        try:
                            res[1].unlink(missing_ok=True)
                        except Exception:
                            pass
                ok_dl, path_dl, err = res
                if ok_dl:
                    pdebug(f"done {it.rel} size={path_dl.stat().st_size if path_dl and path_dl.exists() else 'n/a'}")
                else:
                    pdebug(f"fail {it.rel}: {err}")
                pump_prefetch()

            fut.add_done_callback(_done)

        def pump_prefetch() -> None:
            if prefetch_executor is None:
                return
            if stop_event.is_set():
                return
            if not prefetch_pump_mu.acquire(blocking=False):
                return
            try:
                while True:
                    with prefetch_mu:
                        running = len(prefetch_futs)
                        cached = len(prefetch_results)
                    pdebug(f"pump running={running} cached={cached}")
                    if running >= args.prefetch or cached >= max_cached:
                        pdebug(f"window hold (running={running}, cached={cached})")
                        break
                    with prefetch_candidates_mu:
                        nxt = prefetch_candidates.popleft() if prefetch_candidates else None
                    if nxt is None:
                        pdebug("no more to prefetch")
                        break
                    with prefetch_mu:
                        if nxt.rel in prefetch_futs or nxt.rel in prefetch_results:
                            continue
                    if not nxt.is_remote or nxt.remote_path is None:
                        continue
                    pdebug(f"dequeue {nxt.rel}")
                    submit_prefetch(nxt)
            finally:
                prefetch_pump_mu.release()

        pump_prefetch()

    scan_total = 0
    scan_done = 0
    scan_proc = 0
    scan_enqueued = 0
    scan_mu = threading.Lock()
    scan_error: Optional[BaseException] = None

    work_q: queue.Queue[Optional[WorkItem]] = queue.Queue(maxsize=max(1, args.jobs) * 4)

    def scan_producer() -> None:
        nonlocal scan_total, scan_done, scan_proc, scan_enqueued, scan_error
        dbg("scan thread start")
        try:
            for it in items_iter:
                if stop_event.is_set():
                    break
                with scan_mu:
                    scan_total += 1
                if coord.is_done(it):
                    with scan_mu:
                        scan_done += 1
                    continue
                if coord.is_processing(it, current_device_id=device_id):
                    with scan_mu:
                        scan_proc += 1
                    continue
                offer_prefetch(it)
                while not stop_event.is_set():
                    try:
                        work_q.put(it, timeout=0.2)
                        break
                    except queue.Full:
                        continue
                with scan_mu:
                    scan_enqueued += 1
        except BaseException as e:
            scan_error = e
            if isinstance(e, FatalAuthError):
                set_fatal(e)
            stop_event.set()
        finally:
            with scan_mu:
                dbg(f"scan thread end total={scan_total} done={scan_done} processing={scan_proc} enqueued={scan_enqueued}")
            while True:
                try:
                    work_q.put(None, timeout=0.2)
                    break
                except queue.Full:
                    if stop_event.is_set():
                        break

    scan_thread = threading.Thread(target=scan_producer, name="scan_inputs")
    scan_thread.start()

    log(f"Input root: {in_root_display}")
    log(f"Scan: streaming | jobs: {args.jobs}")
    log(f"Output: {'(inplace)' if args.inplace else (out_root_display or '')}")
    log(f"Device: {device_id} | lock ttl: {args.lock_ttl}s | steal stale: {'ON' if args.steal_stale_lock else 'OFF'}")
    log(f"Video encoder: {args.video_encoder} | Image codec: {args.image_codec}")
    if input_is_remote:
        log(f"Remote prefetch: {'ON' if prefetch_executor is not None else 'OFF'} | prefetch: {args.prefetch}")
    if out_is_remote:
        log(f"Remote upload async: {'ON' if upload_executor is not None else 'OFF'} | upload jobs: {args.upload_jobs}")

    interrupted = False

    def _sig(_s: int, _f: Any) -> None:
        nonlocal interrupted
        interrupted = True
        stop_event.set()
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    ok = 0
    skipped = 0
    failed = 0
    failed_items: Dict[str, str] = {}

    def record_fail(it: WorkItem, msg: str) -> None:
        failed_items[it.rel] = msg

    @dataclass
    class StageResult:
        it: WorkItem
        jr: JobResult
        upload_dst_path: Optional[str] = None

    def safe_append(status: str, it: WorkItem, *, dst_rel: Optional[str] = None) -> None:
        try:
            coord.append(status, it, dst_rel=dst_rel)
            dbg(f"state {status} {it.rel} dst={dst_rel or ''}")
        except FatalAuthError:
            raise
        except Exception as e:
            log_err(f"[WARN] state append failed ({status}) for {it.rel}: {e}")

    def upload_worker(it: WorkItem, jr: JobResult, dst_path: str) -> Tuple[WorkItem, JobResult]:
        assert remote_client is not None
        try:
            if jr.out_local is None or jr.out_rel is None:
                return it, JobResult(True, "skip", "no output generated")
            dbg(f"upload {jr.out_local} -> {dst_path}")
            uploaded_path = upload_file_remote(
                remote_client, jr.out_local, dst_path, overwrite=args.overwrite, cancel_event=stop_event
            )
            out_rel = _rewrite_out_rel_for_uploaded_path(jr.out_rel, uploaded_path)
            return it, JobResult(True, jr.action, jr.msg, None, out_rel)
        except FatalAuthError:
            raise
        except Exception as e:
            return it, JobResult(False, "fail", f"upload failed: {e}", None, jr.out_rel)
        finally:
            if jr.out_local is not None:
                try:
                    jr.out_local.unlink(missing_ok=True)
                except Exception:
                    pass
            if not args.dry_run and not stop_event.is_set():
                dbg(f"lock release (upload) {it.rel}")
                coord.locks.release(it.rel)
            if upload_slots is not None:
                try:
                    upload_slots.release()
                except ValueError:
                    pass

    def worker(it: WorkItem) -> StageResult:
        dbg(f"start {it.rel} remote={it.is_remote} size={it.src_size}")
        # lock
        lock_acquired = False
        defer_release = False
        try:
            if not args.dry_run:
                acquired, reason = coord.locks.try_acquire(it.rel)
                if not acquired:
                    dbg(f"lock skip {it.rel}: {reason}")
                    return StageResult(it, JobResult(True, "skip", f"lock failed: {reason}", None, None))
                dbg(f"lock ok {it.rel}")
                lock_acquired = True

                # 重新拉取最新 state（避免 todo 构建时的缓存导致重复处理）
                ent = coord.get_latest(it.rel, force=True)
                if ent and ent.status == "done" and ent.src_size == it.src_size and ent.src_mtime_ns == it.src_mtime_ns:
                    dbg(f"already done elsewhere {it.rel}")
                    return StageResult(it, JobResult(True, "skip", "already done by other device", None, ent.dst_rel))

                safe_append("processing", it)

            with tempfile.TemporaryDirectory(prefix="shrink_in_", ignore_cleanup_errors=True) as td_in, tempfile.TemporaryDirectory(
                prefix="shrink_out_", ignore_cleanup_errors=True
            ) as td_out:
                in_tmp_root = Path(td_in)
                out_tmp_root = Path(td_out)

                local_src = in_tmp_root / it.rel
                ensure_parent(local_src)

                if it.is_remote:
                    assert remote_client is not None and it.remote_path is not None
                    res: Optional[Tuple[bool, Optional[Path], str]] = None
                    fut: Optional[cf.Future[Tuple[bool, Optional[Path], str]]] = None
                    with prefetch_mu:
                        res = prefetch_results.pop(it.rel, None)
                        fut = prefetch_futs.get(it.rel)
                        if res is None and fut is not None:
                            prefetch_claimed.add(it.rel)
                    if res is None and fut is not None:
                        try:
                            res = fut.result()
                        except FatalAuthError:
                            raise
                        except Exception as e:
                            res = (False, None, str(e))
                    # after consuming any cached/active prefetch, allow queue refill
                    pump_prefetch()
                    if res is not None:
                        ok_dl, path_dl, err = res
                        if args.prefetch_debug:
                            pdebug(f"use {'hit' if ok_dl else 'miss'} {it.rel}")
                        if ok_dl and path_dl is not None and path_dl.exists():
                            try:
                                ensure_parent(local_src)
                                dbg(f"prefetch copy {path_dl} -> {local_src}")
                                shutil.copy2(path_dl, local_src)
                                if size_of(local_src) != it.src_size:
                                    dbg(f"size mismatch; re-download {it.remote_path}")
                                    remote_client.download_to(it.remote_path, local_src)
                                try:
                                    path_dl.unlink(missing_ok=True)  # cleanup consumed prefetch
                                except Exception:
                                    pass
                            except FatalAuthError:
                                raise
                            except FileNotFoundError:
                                # 预取文件可能被清理/丢失（例如并发消费）；回退为直接下载而不是失败。
                                try:
                                    dbg(f"prefetch vanished -> download {it.remote_path}")
                                    remote_client.download_to(it.remote_path, local_src)
                                except FatalAuthError:
                                    raise
                                except Exception as e:
                                    return StageResult(it, JobResult(False, "fail", f"prefetch vanished+download failed: {e}"))
                            except Exception as e:
                                return StageResult(it, JobResult(False, "fail", f"prefetch copy failed: {e}"))
                        else:
                            try:
                                dbg(f"prefetch miss/stale -> download {it.remote_path}")
                                remote_client.download_to(it.remote_path, local_src)
                            except FatalAuthError:
                                raise
                            except Exception as e:
                                return StageResult(it, JobResult(False, "fail", f"prefetch+download failed: {err or e}"))
                    else:
                        try:
                            dbg(f"download {it.remote_path} -> {local_src}")
                            remote_client.download_to(it.remote_path, local_src)
                        except FatalAuthError:
                            raise
                        except Exception as e:
                            return StageResult(it, JobResult(False, "fail", f"download failed: {e}"))
                else:
                    assert it.src_local is not None
                    if not args.dry_run:
                        dbg(f"local copy {it.src_local} -> {local_src}")
                        shutil.copy2(it.src_local, local_src)

                if out_is_remote:
                    local_out_root = upload_dir if upload_dir is not None else out_tmp_root
                else:
                    local_out_root = out_root_local  # type: ignore

                dbg(f"process {it.rel} src={local_src} out_root={local_out_root}")
                jr = process_one_local(
                    local_src,
                    in_tmp_root,
                    local_out_root,  # type: ignore
                    container=args.container,
                    video_policy=args.video_policy,
                    audio_policy=args.audio_policy,
                    allow_opus_in_mp4=args.allow_opus_in_mp4,
                    video_encoder=args.video_encoder,
                    video_crf=args.video_crf,
                    video_preset=args.video_preset,
                    pix_fmt=args.pix_fmt,
                    image_codec=args.image_codec,
                    webp_quality=args.webp_quality,
                    webp_lossless=args.webp_lossless,
                    avif_crf=args.image_crf,
                    avif_pix_fmt=args.image_pix_fmt,
                    faststart=args.faststart,
                    overwrite=args.overwrite,
                    dry_run=args.dry_run,
                    min_savings=args.min_savings,
                    try_archives=args.try_archives,
                    comic_min_images=args.comic_detect_min_images,
                    comic_keep_non_images=args.comic_keep_non_images,
                    comic_accept_bigger=args.comic_accept_bigger,
                    archive_password=args.archive_password,
                    out_name_mode=args.out_name_mode,
                    rel_override=it.rel,
                    out_rel_override=it.out_rel_override,
                    src_size_hint=it.src_size,
                )
                dbg(f"process result {it.rel} ok={jr.ok} action={jr.action} msg={jr.msg}")

                if args.dry_run:
                    return StageResult(it, jr)

                if not jr.ok:
                    return StageResult(it, jr)

                if out_is_remote:
                    assert remote_client is not None and out_root_remote_path is not None
                    if jr.out_local is None or jr.out_rel is None:
                        return StageResult(it, JobResult(True, "skip", "no output generated"))
                    dst_path = remote_join(out_root_remote_path, jr.out_rel)
                    if upload_executor is not None and upload_dir is not None and upload_slots is not None:
                        defer_release = True
                        dbg(f"upload enqueue {it.rel} -> {dst_path}")
                        return StageResult(it, jr, upload_dst_path=dst_path)
                    try:
                        dbg(f"upload {jr.out_local} -> {dst_path}")
                        uploaded_path = upload_file_remote(
                            remote_client, jr.out_local, dst_path, overwrite=args.overwrite, cancel_event=stop_event
                        )
                    except FatalAuthError:
                        raise
                    except Exception as e:
                        return StageResult(it, JobResult(False, "fail", f"upload failed: {e}", None, jr.out_rel))
                    out_rel2 = _rewrite_out_rel_for_uploaded_path(jr.out_rel, uploaded_path)
                    return StageResult(it, JobResult(True, jr.action, jr.msg, None, out_rel2))

                return StageResult(it, jr)

        finally:
            if lock_acquired and not defer_release and not args.dry_run and not stop_event.is_set():
                dbg(f"lock release {it.rel}")
                coord.locks.release(it.rel)

    def finalize(it: WorkItem, jr: JobResult) -> None:
        nonlocal ok, skipped, failed

        if jr.ok and jr.action in {"ok", "copy"}:
            if jr.action == "ok":
                ok += 1
            else:
                skipped += 1
            log(f"[{jr.action.upper()}] {it.rel} | {jr.msg}")
            if not args.dry_run:
                safe_append("done", it, dst_rel=jr.out_rel)
            return

        if jr.ok and jr.action == "skip":
            skipped += 1
            log(f"[SKIP] {it.rel} | {jr.msg}")
            return

        if jr.ok and jr.action == "dry-run":
            skipped += 1
            log(f"[DRY] {it.rel} | {jr.msg}")
            return

        failed += 1
        record_fail(it, jr.msg)
        log_err(f"[FAIL] {it.rel} | {jr.msg}")
        if not args.dry_run:
            safe_append("fail", it, dst_rel=jr.out_rel)

    upload_futs: Dict[cf.Future[Tuple[WorkItem, JobResult]], WorkItem] = {}

    def drain_upload_futs(*, block: bool, only_success: bool) -> None:
        nonlocal failed
        while upload_futs:
            timeout = 0.5 if block else 0.0
            done, _ = cf.wait(upload_futs.keys(), timeout=timeout, return_when=cf.FIRST_COMPLETED)
            if not done:
                break
            for fut in done:
                it_ref = upload_futs.pop(fut, None)
                try:
                    it2, jr = fut.result()
                except FatalAuthError:
                    raise
                except Exception as e:
                    if only_success:
                        continue
                    failed += 1
                    if it_ref is not None:
                        record_fail(it_ref, f"upload exception: {e}")
                    log_err(f"[FAIL] upload exception: {e}")
                    if it_ref is not None and not args.dry_run:
                        safe_append("fail", it_ref, dst_rel=None)
                    continue
                if only_success and not (jr.ok and jr.action in {"ok", "copy"}):
                    continue
                finalize(it2, jr)

    try:
        if args.jobs <= 1:
            while True:
                raise_if_fatal()
                if stop_event.is_set():
                    break
                try:
                    it = work_q.get(timeout=0.2)
                except queue.Empty:
                    continue
                if it is None:
                    break
                try:
                    st = worker(it)
                    if st.upload_dst_path is not None:
                        if upload_executor is None or upload_slots is None:
                            finalize(st.it, JobResult(False, "fail", "internal: upload executor not available"))
                            if not args.dry_run:
                                coord.locks.release(st.it.rel)
                            continue
                        upload_slots.acquire()
                        try:
                            uf = upload_executor.submit(upload_worker, st.it, st.jr, st.upload_dst_path)
                        except Exception as e:
                            try:
                                upload_slots.release()
                            except ValueError:
                                pass
                            finalize(st.it, JobResult(False, "fail", f"upload submit failed: {e}"))
                            if not args.dry_run:
                                coord.locks.release(st.it.rel)
                            continue
                        upload_futs[uf] = st.it
                        # jobs=1 时主循环不 wait，因此这里顺手收割已完成的上传，
                        # 让 done 尽快写入 state（避免多设备重复跑）。
                        drain_upload_futs(block=False, only_success=False)
                    else:
                        finalize(st.it, st.jr)
                except FatalAuthError:
                    raise
                except Exception as e:
                    failed += 1
                    record_fail(it, f"worker exception: {e}")
                    log_err(f"[FAIL] worker exception: {e}")
                    if not args.dry_run:
                        safe_append("fail", it, dst_rel=None)

            # stop_event 被设置时，尽量把“已完成上传”的结果写入 done（不阻塞等待未完成的上传）
            drain_upload_futs(block=not stop_event.is_set(), only_success=stop_event.is_set())
        else:
            with cf.ThreadPoolExecutor(max_workers=args.jobs) as ex:
                proc_futs: Dict[cf.Future[StageResult], WorkItem] = {}
                work_done = False

                def try_submit_some(limit: int) -> None:
                    nonlocal work_done
                    for _ in range(limit):
                        if stop_event.is_set() or work_done:
                            return
                        try:
                            it0 = work_q.get_nowait()
                        except queue.Empty:
                            return
                        if it0 is None:
                            work_done = True
                            return
                        proc_futs[ex.submit(worker, it0)] = it0

                while True:
                    raise_if_fatal()
                    try_submit_some(max(0, args.jobs - len(proc_futs)))
                    if not (proc_futs or upload_futs):
                        if stop_event.is_set() or work_done:
                            break
                        try:
                            it0 = work_q.get(timeout=0.5)
                        except queue.Empty:
                            continue
                        if it0 is None:
                            work_done = True
                            continue
                        proc_futs[ex.submit(worker, it0)] = it0
                        continue

                    all_futs: List[cf.Future[Any]] = list(proc_futs.keys()) + list(upload_futs.keys())
                    timeout = 0.5 if not stop_event.is_set() else 0.0
                    done, _ = cf.wait(all_futs, timeout=timeout, return_when=cf.FIRST_COMPLETED)
                    if not done:
                        if stop_event.is_set():
                            break
                        continue
                    for fut in done:
                        it_ref = proc_futs.pop(fut, None)
                        if it_ref is not None:
                            try:
                                st = fut.result()
                            except FatalAuthError:
                                raise
                            except Exception as e:
                                if not stop_event.is_set():
                                    failed += 1
                                    record_fail(it_ref, f"worker exception: {e}")
                                    log_err(f"[FAIL] worker exception: {e}")
                                    if not args.dry_run:
                                        safe_append("fail", it_ref, dst_rel=None)
                                    try_submit_some(1)
                                continue

                            if st.upload_dst_path is not None:
                                if not stop_event.is_set():
                                    if upload_executor is None or upload_slots is None:
                                        finalize(st.it, JobResult(False, "fail", "internal: upload executor not available"))
                                        if not args.dry_run:
                                            coord.locks.release(st.it.rel)
                                    else:
                                        upload_slots.acquire()
                                        try:
                                            uf = upload_executor.submit(upload_worker, st.it, st.jr, st.upload_dst_path)
                                        except Exception as e:
                                            try:
                                                upload_slots.release()
                                            except ValueError:
                                                pass
                                            finalize(st.it, JobResult(False, "fail", f"upload submit failed: {e}"))
                                            if not args.dry_run:
                                                coord.locks.release(st.it.rel)
                                        else:
                                            upload_futs[uf] = st.it
                            else:
                                if stop_event.is_set() and not (st.jr.ok and st.jr.action in {"ok", "copy"}):
                                    # 中断时保持 processing/lock；成功的才记 done
                                    pass
                                else:
                                    finalize(st.it, st.jr)

                            if not stop_event.is_set():
                                try_submit_some(1)
                            continue

                        it_ref2 = upload_futs.pop(fut, None)
                        if it_ref2 is None:
                            continue
                        try:
                            it2, jr = fut.result()
                        except FatalAuthError:
                            raise
                        except Exception as e:
                            if not stop_event.is_set():
                                failed += 1
                                record_fail(it_ref2, f"upload exception: {e}")
                                log_err(f"[FAIL] upload exception: {e}")
                                if not args.dry_run:
                                    safe_append("fail", it_ref2, dst_rel=None)
                            continue
                        if stop_event.is_set() and not (jr.ok and jr.action in {"ok", "copy"}):
                            continue
                        finalize(it2, jr)

                if stop_event.is_set():
                    # best-effort：写入已完成上传的 done（不阻塞等待未完成的上传）
                    drain_upload_futs(block=False, only_success=True)
                    for fut in list(proc_futs.keys()):
                        fut.cancel()
                    for fut in list(upload_futs.keys()):
                        fut.cancel()
                    ex.shutdown(wait=False, cancel_futures=True)

    except KeyboardInterrupt:
        interrupted = True
        stop_event.set()
        # Ctrl+C 可能打断主线程的 wait/循环，导致“已完成上传”没来得及 finalize -> done。
        # 这里 best-effort 把已完成的上传写入 done；其余保持 processing/lock 以便后续接管。
        drain_upload_futs(block=False, only_success=True)
        if remote_client is not None:
            try:
                remote_client.cancel_pending()
            except Exception:
                pass

    try:
        scan_thread.join()
    except Exception:
        pass

    with scan_mu:
        log(f"Scan done: total={scan_total} done={scan_done} processing={scan_proc} enqueued={scan_enqueued}")

    raise_if_fatal()

    if scan_error is not None and not interrupted:
        failed += 1
        log_err(f"ERROR: input scan failed: {scan_error}")

    if interrupted:
        log("\nInterrupted. 已尽力写入已完成任务的 done；processing 会保留并在 TTL 超时后可被接管。")

    if prefetch_executor is not None:
        prefetch_executor.shutdown(wait=False, cancel_futures=True)
    if prefetch_dir is not None:
        try:
            dbg(f"cleanup prefetch dir {prefetch_dir}")
            shutil.rmtree(prefetch_dir, ignore_errors=True)
        except Exception:
            pass

    if upload_executor is not None:
        upload_executor.shutdown(wait=not stop_event.is_set(), cancel_futures=stop_event.is_set())
    if upload_dir is not None:
        try:
            dbg(f"cleanup upload dir {upload_dir}")
            shutil.rmtree(upload_dir, ignore_errors=True)
        except Exception:
            pass

    if remote_client is not None:
        try:
            remote_client.close()
        except Exception:
            pass

    log(f"\nSummary: OK={ok}, SKIP/COPY={skipped}, FAIL={failed}")
    if failed_items:
        log("Failed files:")
        for rel, msg in sorted(failed_items.items()):
            log(f"- {rel} | {msg}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except FatalAuthError as e:
        log_err(f"ERROR: {e}")
        sys.exit(2)
