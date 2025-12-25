#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "httpx>=0.27",
# ]
# [[tool.uv.index]]
# url = "https://mirrors.ustc.edu.cn/pypi/simple/"
# ///

"""
shrink-media: 多媒体“瘦身”工具，支持本地/ WebDAV 输入输出 + 多设备协同。

特点（与旧版一致/增强）:
- 视频/音频/图片/漫画压缩包处理，体积不够小则回退复制
- WebDAV 递归遍历、上传 (PUT+MOVE)、远端 state/lock，跨设备安全
- 可选多线程；dry-run 预览；NVENC 优先，回退 x265/x264；opus/aac 音频；webp/avif 图片

设计思路：
- 单文件 + 标准库为主，WebDAV 用 httpx 以简化 TLS/重试
- 结构化模块：配置、WebDAV、状态锁、分类与转码、漫画处理、执行器
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures as cf
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse, urlunparse

import httpx
import xml.etree.ElementTree as ET

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
LOCKS_DIR_NAME = ".shrink_media_locks"

SAFE_PATH = "/%:@-._~!$&'()*+,;="
SAFE_QUERY = "%=&:@/?-._~!$'()*+,;"

# ------------------------
# 简易日志
# ------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


def log_err(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


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
    if name in {STATE_DEFAULT_NAME, LOCKS_DIR_NAME}:
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
# WebDAV 辅助
# ------------------------

def normalize_url(u: str) -> str:
    pu = urlparse(u)
    path = pu.path or "/"
    q = pu.query or ""
    path_enc = quote(path, safe=SAFE_PATH)
    q_enc = quote(q, safe=SAFE_QUERY)
    return urlunparse((pu.scheme, pu.netloc, path_enc, pu.params, q_enc, pu.fragment))


def strip_userinfo(u: str) -> str:
    pu = urlparse(u)
    host = pu.hostname or ""
    netloc = f"{host}:{pu.port}" if pu.port else host
    return urlunparse((pu.scheme, netloc, pu.path, pu.params, pu.query, pu.fragment))


def webdav_join(root: str, rel: str) -> str:
    root = normalize_url(root)
    if not root.endswith("/"):
        root += "/"
    parts = [quote(p, safe="!$&'()*+,;=:@-._~") for p in rel.split("/") if p]
    return normalize_url(root + "/".join(parts))


def webdav_dirname(url: str) -> str:
    url = normalize_url(url)
    pu = urlparse(url)
    path = pu.path or "/"
    if path.endswith("/"):
        path = path[:-1]
    i = path.rfind("/")
    dir_path = "/" if i <= 0 else path[:i + 1]
    return normalize_url(urlunparse((pu.scheme, pu.netloc, dir_path, "", "", "")))


@dataclass
class HttpResp:
    status: int
    reason: str
    headers: Dict[str, str]
    body: bytes | None = None


class WebDAVClient:
    """
    轻量 WebDAV 客户端，使用 httpx。
    - 支持 URL 内嵌或参数传入的 Basic 认证
    - 实现 HEAD / GET / PUT / MOVE / DELETE / MKCOL / PROPFIND
    """

    def __init__(self, user: Optional[str], password: Optional[str], *, insecure: bool, timeout: int) -> None:
        self.user = user
        self.password = password
        self.insecure = insecure
        self.timeout = timeout
        self._mkdir_cache: set[str] = set()
        self._client = httpx.Client(verify=not insecure, timeout=timeout)

    # ---- 内部工具 ----
    def _pick_cred(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        if self.user is not None or self.password is not None:
            return self.user, self.password
        pu = urlparse(url)
        return pu.username, pu.password

    def _headers(self, url: str, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        u, p = self._pick_cred(url)
        h: Dict[str, str] = {}
        if u is not None or p is not None:
            raw = f"{u or ''}:{p or ''}".encode("utf-8")
            token = base64.b64encode(raw).decode("ascii")
            h["Authorization"] = f"Basic {token}"
        if extra:
            h.update(extra)
        return h

    def _req(self, method: str, url: str, *, headers: Optional[Dict[str, str]] = None, data: Any = None, stream_path: Optional[Path] = None) -> HttpResp:
        url = normalize_url(url)
        h = self._headers(url, headers)
        try:
            if stream_path:
                with stream_path.open("rb") as f:
                    r = self._client.request(method, url, headers=h, content=f)
            else:
                r = self._client.request(method, url, headers=h, data=data)
        except httpx.HTTPError as e:
            raise RuntimeError(f"WebDAV request error: {e}") from e
        return HttpResp(status=r.status_code, reason=r.reason_phrase or "", headers=dict(r.headers), body=r.content)

    # ---- 基础方法 ----
    def head(self, url: str) -> HttpResp:
        return self._req("HEAD", url)

    def get_bytes(self, url: str) -> Tuple[HttpResp, bytes]:
        r = self._req("GET", url)
        return r, r.body or b""

    def get_text(self, url: str) -> Tuple[HttpResp, str]:
        r, b = self.get_bytes(url)
        return r, (b or b"").decode("utf-8", errors="replace")

    def put_bytes(self, url: str, body: bytes, *, headers: Optional[Dict[str, str]] = None) -> HttpResp:
        return self._req("PUT", url, headers=headers, data=body)

    def put_text(self, url: str, text: str, *, headers: Optional[Dict[str, str]] = None) -> HttpResp:
        return self.put_bytes(url, text.encode("utf-8"), headers=headers)

    def request_upload(self, url: str, local_file: Path, *, headers: Optional[Dict[str, str]] = None) -> HttpResp:
        return self._req("PUT", url, headers=headers, stream_path=local_file)

    def delete(self, url: str) -> HttpResp:
        return self._req("DELETE", url)

    def move(self, src_url: str, dst_url: str, *, overwrite: bool) -> HttpResp:
        headers = {"Destination": normalize_url(dst_url)}
        if overwrite:
            headers["Overwrite"] = "T"
        return self._req("MOVE", src_url, headers=headers)

    def mkcol(self, url: str) -> HttpResp:
        return self._req("MKCOL", url)

    def propfind(self, url: str, *, depth: str) -> Tuple[HttpResp, bytes]:
        headers = {"Depth": depth}
        r = self._req("PROPFIND", url, headers=headers)
        return r, r.body or b""

    # ---- 便利方法 ----
    def ensure_dir(self, dir_url: str) -> None:
        dir_url = normalize_url(dir_url)
        if not dir_url.endswith("/"):
            dir_url += "/"
        # 递归创建路径各层
        pu = urlparse(dir_url)
        parts = pu.path.split("/")
        cur = ""
        for part in parts:
            if part == "":
                continue
            cur += "/" + part
            u = normalize_url(urlunparse((pu.scheme, pu.netloc, cur + "/", "", "", "")))
            if u in self._mkdir_cache:
                continue
            resp = self.mkcol(u)
            if resp.status in (201, 405):
                self._mkdir_cache.add(u)
                continue
            # 其他错误忽略，后续目录可能已存在
        self._mkdir_cache.add(dir_url)


# ------------------------
# WebDAV 目录遍历
# ------------------------

@dataclass
class RemoteEntry:
    rel: str
    href: str
    is_dir: bool
    size: int
    mtime_ns: int


def _dav_text(el: Optional[ET.Element]) -> str:
    return (el.text or "").strip() if el is not None else ""


def _parse_http_date_to_ns(s: str) -> int:
    try:
        dt = parsedate_to_datetime(s)
        return int(dt.timestamp() * 1_000_000_000)
    except Exception:
        return 0


def list_webdav_recursive(client: WebDAVClient, root_url: str) -> List[RemoteEntry]:
    root_url = normalize_url(root_url)
    if not root_url.endswith("/"):
        root_url += "/"
    root_pu = urlparse(root_url)
    root_path = unquote(root_pu.path)
    ns = {"d": "DAV:"}

    def walk(dir_url: str) -> List[RemoteEntry]:
        dir_url = normalize_url(dir_url)
        if not dir_url.endswith("/"):
            dir_url += "/"

        r, b = client.propfind(dir_url, depth="1")
        if r.status not in (207, 200):
            raise RuntimeError(f"PROPFIND failed {r.status} {r.reason}: {dir_url}")

        xml = ET.fromstring(b)
        entries: List[RemoteEntry] = []

        for resp in xml.findall("d:response", ns):
            href_el = resp.find("d:href", ns)
            href = _dav_text(href_el)
            if not href:
                continue

            if href.startswith("http://") or href.startswith("https://"):
                href_full = normalize_url(href)
                href_path = unquote(urlparse(href_full).path)
            else:
                href_path_raw = href
                href_path = unquote(href_path_raw)
                href_full = normalize_url(urlunparse((root_pu.scheme, root_pu.netloc, href_path_raw, "", "", "")))

            if href_path.rstrip("/") == unquote(urlparse(dir_url).path).rstrip("/"):
                continue

            prop = resp.find(".//d:prop", ns)
            if prop is None:
                continue

            is_collection = prop.find("d:resourcetype/d:collection", ns) is not None
            size_txt = _dav_text(prop.find("d:getcontentlength", ns))
            lm_txt = _dav_text(prop.find("d:getlastmodified", ns))

            size = int(size_txt) if size_txt.isdigit() else 0
            mtime_ns = _parse_http_date_to_ns(lm_txt) if lm_txt else 0

            if href_path.startswith(root_path):
                rel = href_path[len(root_path):].lstrip("/")
            else:
                rel = href_path.lstrip("/")
            rel = rel.rstrip("/") if is_collection else rel
            entries.append(RemoteEntry(rel=rel, href=href_full, is_dir=is_collection, size=size, mtime_ns=mtime_ns))

        out: List[RemoteEntry] = []
        for e in entries:
            if e.is_dir:
                out.extend(walk(e.href))
            else:
                out.append(e)
        return out

    return walk(root_url)


# ------------------------
# WorkItem & 输入枚举
# ------------------------

@dataclass
class WorkItem:
    rel: str
    is_remote: bool
    src_local: Optional[Path] = None
    src_url: Optional[str] = None
    src_size: int = 0
    src_mtime_ns: int = 0


def iter_local_inputs(input_path: Path) -> Tuple[Path, List[WorkItem]]:
    if input_path.is_file():
        st = input_path.stat()
        return input_path.parent, [
            WorkItem(
                rel=input_path.name,
                is_remote=False,
                src_local=input_path,
                src_size=int(st.st_size),
                src_mtime_ns=int(st.st_mtime_ns),
            )
        ]

    root = input_path
    items: List[WorkItem] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if should_ignore_name(p.name):
            continue
        rel = p.relative_to(root).as_posix()
        st = p.stat()
        items.append(
            WorkItem(
                rel=rel,
                is_remote=False,
                src_local=p,
                src_size=int(st.st_size),
                src_mtime_ns=int(st.st_mtime_ns),
            )
        )
    items.sort(key=lambda x: x.rel)
    return root, items


def iter_webdav_inputs(client: WebDAVClient, root_url: str) -> Tuple[str, List[WorkItem]]:
    root_url = normalize_url(root_url)
    if not root_url.endswith("/"):
        root_url += "/"
    entries = list_webdav_recursive(client, root_url)
    items: List[WorkItem] = []
    for e in entries:
        name = Path(e.rel).name
        if should_ignore_name(name):
            continue
        if e.rel.startswith(LOCKS_DIR_NAME + "/"):
            continue
        items.append(
            WorkItem(
                rel=e.rel,
                is_remote=True,
                src_url=e.href,
                src_size=e.size,
                src_mtime_ns=e.mtime_ns,
            )
        )
    items.sort(key=lambda x: x.rel)
    return root_url, items


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


class StateBackend:
    """
    - 本地：直接 append
    - WebDAV：GET 现有 + CAS (If-Match)；403/405/412 回退普通 PUT
    """

    def __init__(self, *, local_path: Optional[Path], remote_url: Optional[str], client: Optional[WebDAVClient]) -> None:
        self.local_path = local_path
        self.remote_url = remote_url
        self.client = client
        if (local_path is None) == (remote_url is None):
            raise ValueError("StateBackend needs exactly one of local_path or remote_url")

    def read_all(self) -> str:
        if self.local_path is not None:
            if not self.local_path.exists():
                return ""
            return self.local_path.read_text("utf-8", errors="replace")

        assert self.client is not None and self.remote_url is not None
        h = self.client.head(self.remote_url)
        if h.status == 404:
            return ""
        r, txt = self.client.get_text(self.remote_url)
        if r.status == 404:
            return ""
        if r.status >= 400:
            raise RuntimeError(f"GET state failed {r.status} {r.reason}")
        return txt

    def append_line(self, line: str) -> None:
        if self.local_path is not None:
            ensure_parent(self.local_path)
            with self.local_path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
            return

        assert self.client is not None and self.remote_url is not None

        for _ in range(10):
            head = self.client.head(self.remote_url)
            etag = head.headers.get("ETag")

            if head.status == 404:
                resp = self.client.put_text(self.remote_url, line + "\n", headers={"If-None-Match": "*"})
                if resp.status in (201, 204):
                    return
                if resp.status in (403, 405, 412):
                    resp2 = self.client.put_text(self.remote_url, line + "\n", headers=None)
                    if resp2.status in (201, 204):
                        return
                    raise RuntimeError(f"PUT state(create) failed {resp2.status} {resp2.reason}")
                if resp.status == 412:
                    continue
                raise RuntimeError(f"PUT state(create) failed {resp.status} {resp.reason}")

            if head.status >= 400:
                raise RuntimeError(f"HEAD state failed {head.status} {head.reason}")

            r, old = self.client.get_text(self.remote_url)
            if r.status >= 400:
                raise RuntimeError(f"GET state failed {r.status} {r.reason}")

            new = old
            if new and not new.endswith("\n"):
                new += "\n"
            new += line + "\n"

            hdr = {"If-Match": etag} if etag else None
            resp = self.client.put_text(self.remote_url, new, headers=hdr)
            if resp.status in (201, 204):
                return

            if resp.status in (403, 405, 412):
                resp2 = self.client.put_text(self.remote_url, new, headers=None)
                if resp2.status in (201, 204):
                    return
                if resp2.status in (412,):
                    continue
                raise RuntimeError(f"PUT state(update) failed {resp2.status} {resp2.reason}")

            if resp.status == 412:
                continue

            raise RuntimeError(f"PUT state(update) failed {resp.status} {resp.reason}")

        raise RuntimeError("append state failed after retries")


class LockBackend:
    def __init__(
        self,
        *,
        local_dir: Optional[Path],
        remote_dir_url: Optional[str],
        client: Optional[WebDAVClient],
        device_id: str,
        ttl_sec: int,
        steal_stale: bool,
    ) -> None:
        self.local_dir = local_dir
        self.remote_dir_url = remote_dir_url
        self.client = client
        self.device_id = device_id
        self.ttl_sec = ttl_sec
        self.steal_stale = steal_stale

        if (local_dir is None) == (remote_dir_url is None):
            raise ValueError("LockBackend needs exactly one of local_dir or remote_dir_url")

        if self.local_dir is not None:
            self.local_dir.mkdir(parents=True, exist_ok=True)
        else:
            assert self.client is not None and self.remote_dir_url is not None
            self.remote_dir_url = normalize_url(self.remote_dir_url)
            if not self.remote_dir_url.endswith("/"):
                self.remote_dir_url += "/"
            self.client.ensure_dir(self.remote_dir_url)

    def _now(self) -> float:
        return time.time()

    def try_acquire(self, key: str) -> bool:
        token = sha1_hex(key)
        now = self._now()

        if self.local_dir is not None:
            lock_path = self.local_dir / f"{token}.lock"

            if lock_path.exists() and self.steal_stale:
                try:
                    obj = json.loads(lock_path.read_text("utf-8", errors="replace") or "{}")
                    ts = float(obj.get("ts", 0))
                    if now - ts > self.ttl_sec:
                        lock_path.unlink(missing_ok=True)  # type: ignore
                except Exception:
                    pass

            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(json.dumps({"ts": now, "device_id": self.device_id}, ensure_ascii=False))
                return True
            except FileExistsError:
                return False

        assert self.client is not None and self.remote_dir_url is not None
        lock_dir = normalize_url(self.remote_dir_url + quote(token) + "/")
        owner_url = normalize_url(lock_dir + "owner.json")

        resp = self.client.mkcol(lock_dir)
        if resp.status == 201:
            self.client.put_text(owner_url, json.dumps({"ts": now, "device_id": self.device_id}, ensure_ascii=False))
            return True

        if resp.status not in (405, 409):
            return False

        if not self.steal_stale:
            return False

        try:
            r, txt = self.client.get_text(owner_url)
            if r.status >= 400:
                return False
            obj = json.loads(txt or "{}")
            ts = float(obj.get("ts", 0))
            if now - ts <= self.ttl_sec:
                return False
        except Exception:
            return False

        try:
            self.client.delete(owner_url)
            self.client.delete(lock_dir)
        except Exception:
            return False

        resp2 = self.client.mkcol(lock_dir)
        if resp2.status == 201:
            self.client.put_text(owner_url, json.dumps({"ts": now, "device_id": self.device_id}, ensure_ascii=False))
            return True
        return False

    def release(self, key: str) -> None:
        token = sha1_hex(key)
        if self.local_dir is not None:
            p = self.local_dir / f"{token}.lock"
            try:
                p.unlink()
            except Exception:
                pass
            return

        assert self.client is not None and self.remote_dir_url is not None
        lock_dir = normalize_url(self.remote_dir_url + quote(token) + "/")
        owner_url = normalize_url(lock_dir + "owner.json")
        try:
            self.client.delete(owner_url)
            self.client.delete(lock_dir)
        except Exception:
            pass


class Coordinator:
    def __init__(self, state: StateBackend, locks: LockBackend, *, device_id: str, ttl_sec: int) -> None:
        self.state = state
        self.locks = locks
        self.device_id = device_id
        self.ttl_sec = ttl_sec
        self._cache: Dict[str, StateEntry] = {}
        self._cache_at = 0.0
        self._mu = threading.Lock()

    def _now(self) -> float:
        return time.time()

    def load_latest(self, *, force: bool = False) -> Dict[str, StateEntry]:
        with self._mu:
            if not force and self._cache and (self._now() - self._cache_at < 10):
                return self._cache

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

            self._cache = latest
            self._cache_at = self._now()
            return latest

    def is_done(self, it: WorkItem) -> bool:
        ent = self.load_latest().get(it.rel)
        if not ent:
            return False
        if ent.status != "done":
            return False
        return ent.src_size == it.src_size and ent.src_mtime_ns == it.src_mtime_ns

    def is_processing(self, it: WorkItem) -> bool:
        ent = self.load_latest().get(it.rel)
        if not ent:
            return False
        if ent.status != "processing":
            return False
        return (self._now() - ent.ts) <= self.ttl_sec

    def append(self, status: str, it: WorkItem, *, dst_rel: Optional[str] = None) -> None:
        rec = {
            "ts": self._now(),
            "status": status,
            "device_id": self.device_id,
            "src_rel": it.rel,
            "src_size": it.src_size,
            "src_mtime_ns": it.src_mtime_ns,
            "dst_rel": dst_rel,
        }
        self.state.append_line(json.dumps(rec, ensure_ascii=False))
        with self._mu:
            self._cache[it.rel] = StateEntry(
                ts=float(rec["ts"]),
                status=status,
                device_id=self.device_id,
                src_rel=it.rel,
                src_size=it.src_size,
                src_mtime_ns=it.src_mtime_ns,
                dst_rel=dst_rel,
            )
            self._cache_at = self._now()


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
        _ENCODER_CACHE = cp.stdout or ""
    return _ENCODER_CACHE


def has_encoder(enc: str) -> bool:
    return enc in ffmpeg_encoders()


def ffprobe_json(path: Path, *, dry_run: bool) -> Optional[Dict[str, Any]]:
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


def replace_last(cmd: List[str], new_last: str) -> List[str]:
    out = cmd.copy()
    out[-1] = new_last
    return out


def build_video_cmd_single(
    in_path: Path,
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
    in_path: Path,
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

    def add(enc: str) -> None:
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
            video_preset=video_preset,
            pix_fmt=pix_fmt,
            faststart=faststart,
        )
        cands.append(cmd1)
        if retry:
            cands.append(retry)

    if video_encoder == "auto":
        if has_encoder("hevc_nvenc"):
            add("hevc_nvenc")
        add("libx265" if has_encoder("libx265") else "libx264")
        if has_encoder("hevc_nvenc"):
            add("libx265" if has_encoder("libx265") else "libx264")
    else:
        add(video_encoder)
        if video_encoder == "hevc_nvenc":
            add("libx265" if has_encoder("libx265") else "libx264")

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
    in_path: Path,
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
    in_path: Path,
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
) -> Tuple[bool, str]:
    if dry_run:
        for c in candidates:
            log("[dry-run] " + " ".join(c))
        return True, "dry-run"

    if out_final.exists() and not overwrite:
        return True, f"output exists: {out_final}"

    # 保留原始扩展名，避免 ffmpeg 因未知扩展无法推断格式
    suffix = out_final.suffix
    tmp_name = out_final.stem + ".__tmp__" + suffix
    out_tmp = out_final.with_name(tmp_name)
    ensure_parent(out_tmp)
    last_err = ""

    for cmd in candidates:
        cmd2 = replace_last(cmd, str(out_tmp))

        try:
            if out_tmp.exists():
                out_tmp.unlink()
        except Exception:
            pass

        cp = subprocess.run(cmd2, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if cp.returncode == 0:
            try:
                if out_final.exists():
                    if overwrite:
                        out_final.unlink()
                    else:
                        try:
                            out_tmp.unlink()
                        except Exception:
                            pass
                        return True, f"output exists: {out_final}"
                ensure_parent(out_final)
                out_tmp.replace(out_final)
                return True, "ok"
            except Exception as e:
                try:
                    if out_tmp.exists():
                        out_tmp.unlink()
                except Exception:
                    pass
                return False, f"rename failed: {e}"
        else:
            try:
                if out_tmp.exists():
                    out_tmp.unlink()
            except Exception:
                pass
            last_err = tail_text(cp.stderr, n_lines=60, max_chars=6000)

    return False, (last_err or "ffmpeg failed")


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
    cp = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
    cp = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

        for p in img_files_sorted:
            rel = p.relative_to(extracted)
            ext = p.suffix.lower()

            if ext in ANIMATED_IMAGE_EXTS:
                files_to_zip.append((p, str(rel)))
                continue

            if ext == target_ext:
                files_to_zip.append((p, str(rel)))
                continue

            out_img = (converted / rel).with_suffix(target_ext)
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
            ok, _err = run_ffmpeg_with_candidates(candidates, out_img, overwrite=True, dry_run=dry_run)
            if not ok:
                files_to_zip.append((p, str(rel)))
                continue

            files_to_zip.append((out_img, str(rel.with_suffix(target_ext))))

        if dry_run:
            log("[dry-run] would create " + str(out_cbz))
            return True, "dry-run ok"

        make_cbz(out_cbz, files_to_zip, overwrite=overwrite)
        return True, f"cbz created (images={len(img_files_sorted)}, fmt={target_ext.lstrip('.')})"


# ------------------------
# WebDAV 上传
# ------------------------

def upload_file_webdav(client: WebDAVClient, local_file: Path, dst_url: str, *, overwrite: bool) -> None:
    dst_url = normalize_url(dst_url)
    dir_url = webdav_dirname(dst_url)
    if not dir_url.endswith("/"):
        dir_url += "/"
    client.ensure_dir(dir_url)

    tmp_url = normalize_url(dst_url + f".__tmp__{int(time.time()*1000)}")
    r1 = client.request_upload(tmp_url, local_file)
    if r1.status >= 400:
        raise RuntimeError(f"PUT failed {r1.status} {r1.reason}: {dst_url}")

    r2 = client.move(tmp_url, dst_url, overwrite=overwrite)
    if r2.status >= 400:
        client.delete(tmp_url)
        raise RuntimeError(f"MOVE failed {r2.status} {r2.reason}: {dst_url}")


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
) -> JobResult:
    rel = src_local.relative_to(in_root).as_posix()

    probe = None
    info0 = classify(src_local, None)
    if info0.kind in {"video", "audio", "image"}:
        probe = ffprobe_json(src_local, dry_run=dry_run)
    info = classify(src_local, probe)

    # subtitle/other -> copy
    if info.kind in {"subtitle", "other"}:
        dst = out_root / rel
        if dry_run:
            return JobResult(True, "dry-run", f"{info.kind} would copy", None, rel)
        copy_file_local(src_local, dst, overwrite=overwrite)
        return JobResult(True, "copy", f"{info.kind} copied", dst, rel)

    # comic/archive
    if info.kind == "comic":
        archiver = find_7z()
        if not archiver:
            return JobResult(False, "fail", "missing 7z/7zz")

        target_ext = ".webp" if image_codec == "webp" else ".avif"
        dst_cbz = (out_root / rel).with_suffix(".cbz")

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
            copy_file_local(src_local, out_root / rel, overwrite=overwrite)
            return JobResult(True, "copy", f"comic smart-skip -> copied original ({reason})", out_root / rel, rel)

        ensure_parent(dst_cbz)
        ok, msg = process_comic_to_cbz(
            src_local,
            dst_cbz,
            archiver=archiver,
            password=archive_password,
            image_codec=image_codec,
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
                copy_file_local(src_local, out_root / rel, overwrite=overwrite)
                return JobResult(True, "copy", "archive copied (try disabled)", out_root / rel, rel)
            if dry_run:
                return JobResult(True, "dry-run", msg, None, rel)
            copy_file_local(src_local, out_root / rel, overwrite=overwrite)
            return JobResult(True, "copy", msg, out_root / rel, rel)

        if not ok:
            return JobResult(False, "fail", msg)

        if dry_run:
            return JobResult(True, "dry-run", msg, None, dst_cbz.relative_to(out_root).as_posix())

        src_sz = size_of(src_local)
        out_sz = size_of(dst_cbz)
        if src_sz > 0 and out_sz > 0 and out_sz >= src_sz * (1.0 - min_savings) and not comic_accept_bigger:
            try:
                dst_cbz.unlink()
            except Exception:
                pass
            copy_file_local(src_local, out_root / rel, overwrite=overwrite)
            return JobResult(True, "copy", f"{msg}; not smaller -> copied original", out_root / rel, rel)

        return JobResult(True, "ok", f"{msg}; size {src_sz}->{out_sz}", dst_cbz, dst_cbz.relative_to(out_root).as_posix())

    # image already target -> copy
    image_target_ext = ".webp" if image_codec == "webp" else ".avif"
    if info.kind == "image" and src_local.suffix.lower() == image_target_ext:
        dst = out_root / rel
        if dry_run:
            return JobResult(True, "dry-run", f"already {image_target_ext}", None, rel)
        copy_file_local(src_local, dst, overwrite=overwrite)
        return JobResult(True, "copy", f"already {image_target_ext.lstrip('.')}, copied", dst, rel)

    dst_base = out_root / rel

    if info.kind == "video":
        has_subs, mp4_sub_ok = detect_subtitle_compat(info.streams)
        container2 = container
        if container2 == "auto":
            container2 = "mp4"
        if container2 == "mp4" and has_subs and not mp4_sub_ok:
            container2 = "mkv"
        out_final = dst_base.with_suffix("." + container2)

        candidates = build_video_candidates(
            src_local,
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
        ok, err = run_ffmpeg_with_candidates(candidates, out_final, overwrite=overwrite, dry_run=dry_run)
        if not ok:
            return JobResult(False, "fail", f"ffmpeg failed:\\n{err}")
        if dry_run:
            return JobResult(True, "dry-run", "ok", None, out_final.relative_to(out_root).as_posix())

        src_sz = size_of(src_local)
        out_sz = size_of(out_final)
        if src_sz > 0 and out_sz > 0 and out_sz >= src_sz * (1.0 - min_savings):
            try:
                out_final.unlink()
            except Exception:
                pass
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", f"not enough savings ({src_sz}->{out_sz}) -> copied original", dst_base, rel)

        return JobResult(True, "ok", f"{src_sz}->{out_sz}", out_final, out_final.relative_to(out_root).as_posix())

    if info.kind == "audio":
        out_final = dst_base.with_suffix(".opus" if audio_policy != "always_copy" else src_local.suffix)
        cmds = build_audio_candidates(src_local, out_final, info, audio_policy=audio_policy)
        if not cmds:
            if dry_run:
                return JobResult(True, "dry-run", "no audio stream -> copy", None, rel)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", "no audio stream -> copied", dst_base, rel)

        ok, err = run_ffmpeg_with_candidates(cmds, out_final, overwrite=overwrite, dry_run=dry_run)
        if not ok:
            return JobResult(False, "fail", f"ffmpeg failed:\\n{err}")
        if dry_run:
            return JobResult(True, "dry-run", "ok", None, out_final.relative_to(out_root).as_posix())

        src_sz = size_of(src_local)
        out_sz = size_of(out_final)
        if src_sz > 0 and out_sz > 0 and out_sz >= src_sz * (1.0 - min_savings):
            try:
                out_final.unlink()
            except Exception:
                pass
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", f"not enough savings ({src_sz}->{out_sz}) -> copied original", dst_base, rel)

        return JobResult(True, "ok", f"{src_sz}->{out_sz}", out_final, out_final.relative_to(out_root).as_posix())

    if info.kind == "image":
        v = get_main_video_stream(info)
        src_pf = str(v.get("pix_fmt")) if (v and v.get("pix_fmt")) else None
        out_final = dst_base.with_suffix(image_target_ext)

        candidates = build_image_candidates(
            src_local,
            out_final,
            image_codec=image_codec,
            webp_quality=webp_quality,
            webp_lossless=webp_lossless,
            avif_crf=avif_crf,
            avif_pix_fmt=avif_pix_fmt,
            src_pix_fmt=src_pf,
        )
        ok, err = run_ffmpeg_with_candidates(candidates, out_final, overwrite=overwrite, dry_run=dry_run)
        if not ok:
            return JobResult(False, "fail", f"ffmpeg failed:\\n{err}")
        if dry_run:
            return JobResult(True, "dry-run", "ok", None, out_final.relative_to(out_root).as_posix())

        src_sz = size_of(src_local)
        out_sz = size_of(out_final)
        if src_sz > 0 and out_sz > 0 and out_sz >= src_sz * (1.0 - min_savings):
            try:
                out_final.unlink()
            except Exception:
                pass
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", f"not enough savings ({src_sz}->{out_sz}) -> copied original", dst_base, rel)

        return JobResult(True, "ok", f"{src_sz}->{out_sz}", out_final, out_final.relative_to(out_root).as_posix())

    dst = out_root / rel
    if dry_run:
        return JobResult(True, "dry-run", "fallback copy", None, rel)
    copy_file_local(src_local, dst, overwrite=overwrite)
    return JobResult(True, "copy", "fallback copied", dst, rel)


# ------------------------
# CLI / main
# ------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="shrink_media: local/WebDAV + multi-device state+locks (rewritten)")
    ap.add_argument("input", type=str, help="输入（本地路径或 WebDAV URL）")
    ap.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="输出（本地路径或 WebDAV URL）。缺省：本地输入用同级 <name>__compressed；WebDAV 输入用同级 <name>__compressed/ 目录。",
    )
    ap.add_argument("--inplace", action="store_true", help="原地替换（不推荐 WebDAV 多设备并发）")

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

    # webdav
    ap.add_argument("--webdav-user", type=str, default=None)
    ap.add_argument("--webdav-pass", type=str, default=None)
    ap.add_argument("--webdav-insecure", action="store_true")
    ap.add_argument("--webdav-timeout", type=int, default=60)

    # multi-device
    ap.add_argument("--device-id", type=str, default=None)
    ap.add_argument("--lock-ttl", type=int, default=6 * 3600)
    ap.add_argument("--no-steal-stale-lock", dest="steal_stale_lock", action="store_false", default=True)

    return ap.parse_args()


def main() -> None:
    args = parse_args()
    # --help 已在 parse_args 内提前退出；放在这里确保显示帮助不依赖外部工具
    require_tools()

    if args.inplace and args.output:
        log_err("ERROR: --inplace 与 --output 不能同时使用。")
        sys.exit(2)

    device_id = args.device_id or os.uname().nodename

    webdav_client: Optional[WebDAVClient] = None
    if is_url(args.input) or (args.output and is_url(args.output)):
        webdav_client = WebDAVClient(
            args.webdav_user,
            args.webdav_pass,
            insecure=args.webdav_insecure,
            timeout=args.webdav_timeout,
        )

    # 输入枚举
    input_is_remote = is_url(args.input)
    if input_is_remote:
        if webdav_client is None:
            log_err("ERROR: WebDAV 输入需要 WebDAV 客户端。")
            sys.exit(2)
        in_root_url, items = iter_webdav_inputs(webdav_client, args.input)
        in_root_local = None
        in_root_display = in_root_url
    else:
        in_path = Path(args.input).expanduser().resolve()
        if not in_path.exists():
            log_err("ERROR: 输入路径不存在。")
            sys.exit(2)
        in_root_local, items = iter_local_inputs(in_path)
        in_root_url = None
        in_root_display = str(in_root_local)

    if not items:
        log("没有找到可处理的文件。")
        return

    # 输出位置
    out_is_remote = False
    out_root_local: Optional[Path] = None
    out_root_url: Optional[str] = None

    if args.inplace:
        if input_is_remote:
            out_is_remote = True
            out_root_url = in_root_url
        else:
            out_is_remote = False
            out_root_local = in_root_local
    else:
        if args.output is None:
            if input_is_remote:
                # 默认与本地一致：同级目录加 __compressed
                assert in_root_url is not None
                pu = urlparse(in_root_url)
                path = pu.path
                if path.endswith("/"):
                    path = path[:-1]
                parent, _, name = path.rpartition("/")
                if parent == "" and path.startswith("/"):
                    parent = ""
                if name == "":
                    name = "__compressed"
                else:
                    name = name + "__compressed"
                new_path = f"{parent}/{name}/" if parent else f"/{name}/"
                out_root_url = urlunparse((pu.scheme, pu.netloc, new_path, "", "", ""))
                out_root_url = normalize_url(out_root_url)
                out_is_remote = True
            else:
                assert in_root_local is not None
                out_root_local = in_root_local.parent / (in_root_local.name + "__compressed")
                out_root_local.mkdir(parents=True, exist_ok=True)
                out_is_remote = False
        else:
            if is_url(args.output):
                out_is_remote = True
                out_root_url = normalize_url(args.output)
                if not out_root_url.endswith("/"):
                    out_root_url += "/"
            else:
                out_is_remote = False
                out_root_local = Path(args.output).expanduser().resolve()
                out_root_local.mkdir(parents=True, exist_ok=True)

    # state + locks
    if out_is_remote:
        assert webdav_client is not None and out_root_url is not None
        state_backend = StateBackend(
            local_path=None, remote_url=webdav_join(out_root_url, STATE_DEFAULT_NAME), client=webdav_client
        )
        lock_backend = LockBackend(
            local_dir=None,
            remote_dir_url=webdav_join(out_root_url, LOCKS_DIR_NAME) + "/",
            client=webdav_client,
            device_id=device_id,
            ttl_sec=args.lock_ttl,
            steal_stale=args.steal_stale_lock,
        )
    else:
        assert out_root_local is not None
        state_backend = StateBackend(local_path=out_root_local / STATE_DEFAULT_NAME, remote_url=None, client=None)
        lock_backend = LockBackend(
            local_dir=out_root_local / LOCKS_DIR_NAME,
            remote_dir_url=None,
            client=None,
            device_id=device_id,
            ttl_sec=args.lock_ttl,
            steal_stale=args.steal_stale_lock,
        )

    coord = Coordinator(state_backend, lock_backend, device_id=device_id, ttl_sec=args.lock_ttl)

    # 初筛
    todo: List[WorkItem] = []
    done_cnt = 0
    proc_cnt = 0
    for it in items:
        if coord.is_done(it):
            done_cnt += 1
            continue
        if coord.is_processing(it):
            proc_cnt += 1
            continue
        todo.append(it)

    log(f"Input root: {in_root_display}")
    log(f"Total: {len(items)} | done: {done_cnt} | processing(active): {proc_cnt} | to-run: {len(todo)}")
    log(f"Output: {'(inplace)' if args.inplace else (out_root_url or str(out_root_local))}")
    log(f"Device: {device_id} | lock ttl: {args.lock_ttl}s | steal stale: {'ON' if args.steal_stale_lock else 'OFF'}")
    log(f"Video encoder: {args.video_encoder} | Image codec: {args.image_codec}")

    stop_event = threading.Event()

    def _sig(_s: int, _f: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    ok = 0
    skipped = 0
    failed = 0

    def safe_append(status: str, it: WorkItem, *, dst_rel: Optional[str] = None) -> None:
        try:
            coord.append(status, it, dst_rel=dst_rel)
        except Exception as e:
            log_err(f"[WARN] state append failed ({status}) for {it.rel}: {e}")

    def worker(it: WorkItem) -> Tuple[WorkItem, JobResult]:
        # lock
        if not args.dry_run:
            if not coord.locks.try_acquire(it.rel):
                return it, JobResult(True, "skip", "locked by other device", None, None)
            safe_append("processing", it)

        try:
            with tempfile.TemporaryDirectory(prefix="shrink_in_") as td_in, tempfile.TemporaryDirectory(prefix="shrink_out_") as td_out:
                in_tmp_root = Path(td_in)
                out_tmp_root = Path(td_out)

                local_src = in_tmp_root / it.rel
                ensure_parent(local_src)

                if it.is_remote:
                    assert webdav_client is not None and it.src_url is not None
                    r = webdav_client.get_bytes(it.src_url)
                    if r[0].status >= 400:
                        return it, JobResult(False, "fail", f"download failed {r[0].status} {r[0].reason}")
                    local_src.write_bytes(r[1])
                else:
                    assert it.src_local is not None
                    if not args.dry_run:
                        shutil.copy2(it.src_local, local_src)

                local_out_root = out_tmp_root if out_is_remote else out_root_local  # type: ignore

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
                )

                if args.dry_run:
                    return it, jr

                if not jr.ok:
                    return it, jr

                if out_is_remote:
                    assert webdav_client is not None and out_root_url is not None
                    if jr.out_local is None or jr.out_rel is None:
                        return it, JobResult(True, "skip", "no output generated")
                    dst_url = webdav_join(out_root_url, jr.out_rel)
                    upload_file_webdav(webdav_client, jr.out_local, dst_url, overwrite=args.overwrite)
                    return it, JobResult(True, jr.action, jr.msg, None, jr.out_rel)

                return it, jr

        finally:
            if not args.dry_run:
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
        log_err(f"[FAIL] {it.rel} | {jr.msg}")
        if not args.dry_run:
            safe_append("fail", it, dst_rel=None)

    try:
        if args.jobs <= 1:
            for it in todo:
                if stop_event.is_set():
                    break
                it2, jr = worker(it)
                finalize(it2, jr)
        else:
            with cf.ThreadPoolExecutor(max_workers=args.jobs) as ex:
                it_iter = iter(todo)
                futures: Dict[cf.Future[Tuple[WorkItem, JobResult]], WorkItem] = {}

                def submit_next() -> bool:
                    if stop_event.is_set():
                        return False
                    try:
                        it0 = next(it_iter)
                    except StopIteration:
                        return False
                    futures[ex.submit(worker, it0)] = it0
                    return True

                for _ in range(args.jobs):
                    if not submit_next():
                        break

                while futures:
                    done, _ = cf.wait(futures.keys(), return_when=cf.FIRST_COMPLETED)
                    for fut in done:
                        futures.pop(fut, None)
                        try:
                            it2, jr = fut.result()
                        except Exception as e:
                            failed += 1
                            log_err(f"[FAIL] worker exception: {e}")
                            continue
                        finalize(it2, jr)
                        if not stop_event.is_set():
                            submit_next()

                    if stop_event.is_set():
                        for fut in list(futures.keys()):
                            fut.cancel()
                        break

    except KeyboardInterrupt:
        stop_event.set()

    if stop_event.is_set():
        log("\nInterrupted. done 已写入 state；processing 会保留并在 TTL 超时后可被接管。")

    log(f"\nSummary: OK={ok}, SKIP/COPY={skipped}, FAIL={failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
