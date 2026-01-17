# ------------------------
# OpenList 辅助
# ------------------------
from __future__ import annotations

import asyncio
import concurrent.futures as cf
import json
import logging
import posixpath
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

import httpx
from openlist import Client
from openlist.core.base import BaseService
from openlist.exceptions import AuthenticationFailed, BadResponse, UnexceptedResponseCode
from tenacity import Retrying, RetryCallState, retry_if_exception, stop_after_attempt

from .logging import _LOGGER, _DEBUG_ENABLED, _PREFETCH_DEBUG_ENABLED
from .utils import ensure_parent, fmt_bytes

__all__ = [
    "patch_openlist_code_401_as_auth_failed",
    "FatalAuthError",
    "HttpStatusError",
    "parse_remote_location",
    "remote_join",
    "mtime_to_ns",
    "RemoteEntry",
    "OpenListClientSync",
]


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

        # 关键差异：把 code=401 当作"未认证"
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


def mtime_to_ns(dt: Any) -> int:
    try:
        return int(dt.timestamp() * 1_000_000_000)
    except Exception:
        return 0


def _mtime_to_ns(dt: Any) -> int:
    return mtime_to_ns(dt)


@dataclass
class RemoteEntry:
    rel: str
    path: str
    is_dir: bool
    size: int
    mtime_ns: int
    sign: str = ""


@dataclass
class _FsObjectLite:
    """
    Minimal FsObject compatible with how shrink-media uses OpenList objects.

    The upstream `openlist` Python client currently requires `path` in the API
    response, but some OpenList servers omit it (both `/api/fs/get` and
    `/api/fs/list`). We synthesize `path` to avoid hard failures while keeping
    attribute access stable (`getattr(obj, "name")`, etc.).
    """

    path: str
    name: str
    size: int = 0
    is_dir: bool = False
    modified: Optional[datetime] = None
    created: Optional[datetime] = None
    sign: str = ""
    thumb: str = ""
    type: int = 0
    hashinfo: Optional[str] = None
    hash_info: Optional[dict] = None
    provider: str = ""


@dataclass
class _FsListResultLite:
    content: List[_FsObjectLite]
    total: int = 0
    readme: str = ""
    header: str = ""
    write: bool = False
    provider: str = ""


def _parse_openlist_dt(v: Any) -> Optional[datetime]:
    if isinstance(v, datetime):
        return v
    if not isinstance(v, str) or not v:
        return None
    s = v
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _parse_openlist_fs_object(data: Dict[str, Any], *, requested_path: str, parent: Optional[str] = None) -> _FsObjectLite:
    name = str(data.get("name") or "")
    # OpenList server may omit `path`; synthesize it deterministically.
    p = str(data.get("path") or "")
    if not p:
        if parent is not None and name:
            p = posixpath.join(parent, name)
        else:
            p = requested_path
    if not p.startswith("/"):
        p = "/" + p
    return _FsObjectLite(
        path=p,
        name=name,
        size=int(data.get("size") or 0),
        is_dir=bool(data.get("is_dir") or False),
        modified=_parse_openlist_dt(data.get("modified")),
        created=_parse_openlist_dt(data.get("created")),
        sign=str(data.get("sign") or ""),
        thumb=str(data.get("thumb") or ""),
        type=int(data.get("type") or 0),
        hashinfo=(data.get("hashinfo") if isinstance(data.get("hashinfo"), str) else None),
        hash_info=(data.get("hash_info") if isinstance(data.get("hash_info"), dict) else None),
        provider=str(data.get("provider") or ""),
    )


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

    def is_retryable(self, e: Exception) -> bool:
        return self._is_retryable(e)

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
        p = path if path.startswith("/") else f"/{path}"

        async def _do() -> _FsListResultLite:
            return await self._fs_list(p, refresh=refresh, page=page, per_page=per_page)

        return self._call_retry(_do, op="listdir")

    def info(self, path: str) -> Any:
        p = path if path.startswith("/") else f"/{path}"

        async def _do() -> _FsObjectLite:
            return await self._fs_get(p)

        return self._call_retry(_do, op="info")

    async def _post_api(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert self._client is not None
        token = self._client.get_token()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = token
        r = await self._client.context.httpx_client.post(endpoint, json=payload, headers=headers)
        if r.status_code == 401:
            raise AuthenticationFailed("Unauthorized")
        if r.status_code == 403:
            try:
                msg = str((r.json() or {}).get("message") or "Forbidden")
            except Exception:
                msg = "Forbidden"
            raise AuthenticationFailed(msg)
        if r.status_code != 200:
            raise HttpStatusError(r.status_code, r.reason_phrase)
        try:
            j = r.json()
        except Exception:
            raise BadResponse("Invalid JSON response")
        if not isinstance(j, dict):
            raise BadResponse("Invalid JSON response")
        code = j.get("code")
        msg = str(j.get("message") or "")
        if code == 401:
            raise AuthenticationFailed(msg or "Unauthorized")
        if code != 200:
            raise BadResponse(msg or f"code={code}")
        data = j.get("data")
        if isinstance(data, dict):
            return data
        if data is None:
            return {}
        # Some OpenList endpoints may return non-dict `data` – keep best effort.
        return {"value": data}

    async def _fs_get(self, path: str, *, password: Optional[str] = None) -> _FsObjectLite:
        p = path if path.startswith("/") else f"/{path}"
        payload: Dict[str, Any] = {"path": p}
        if password is not None:
            payload["password"] = password
        try:
            data = await self._post_api("/api/fs/get", payload)
        except BadResponse as e:
            # OpenList uses code=500 + "object not found" for missing paths.
            msg = str(e).lower()
            if "not found" in msg or "object not found" in msg:
                raise FileNotFoundError(p) from e
            raise
        if not data:
            raise FileNotFoundError(p)
        return _parse_openlist_fs_object(data, requested_path=p)

    async def _fs_list(
        self,
        path: str,
        *,
        refresh: bool,
        page: int,
        per_page: int,
        password: Optional[str] = None,
    ) -> _FsListResultLite:
        p = path if path.startswith("/") else f"/{path}"
        payload: Dict[str, Any] = {
            "path": p,
            "refresh": bool(refresh),
            "page": int(page),
            "per_page": int(per_page),
        }
        if password is not None:
            payload["password"] = password
        try:
            data = await self._post_api("/api/fs/list", payload)
        except BadResponse as e:
            msg = str(e).lower()
            if "not found" in msg or "object not found" in msg:
                raise FileNotFoundError(p) from e
            raise

        content_raw = data.get("content", [])
        content: List[_FsObjectLite] = []
        if isinstance(content_raw, list):
            for item in content_raw:
                if not isinstance(item, dict):
                    continue
                content.append(_parse_openlist_fs_object(item, requested_path=p, parent=p))

        return _FsListResultLite(
            content=content,
            total=int(data.get("total") or 0),
            readme=str(data.get("readme") or ""),
            header=str(data.get("header") or ""),
            write=bool(data.get("write") or False),
            provider=str(data.get("provider") or ""),
        )

    async def _list_recursive(self, root_path: str) -> List[RemoteEntry]:
        root_norm = root_path.rstrip("/") or "/"
        entries: List[RemoteEntry] = []

        try:
            root_info = await self._fs_get(root_norm)
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
            page = 1
            got = 0
            while True:
                res = await self._fs_list(cur, refresh=True, per_page=100, page=page)
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
        # 同时，OpenList 的 fs.info 在"对象不存在"时可能不会抛异常，而是返回"空对象"（取决于 openlist 客户端版本）。
        # 这里统一用 fs.info 的返回来判断存在性，并拿到 sign，避免依赖下载时报错来区分不存在。
        info = await self._fs_get(p)
        sign = str(getattr(info, "sign", "") or "")
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
