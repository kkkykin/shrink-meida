from __future__ import annotations

import concurrent.futures as cf
import posixpath
import re
import threading
import time
from pathlib import Path
from typing import Any, Optional, Tuple

from .logging import _LOGGER, _DEBUG_ENABLED, log_err
from .openlist_client import OpenListClientSync, FatalAuthError, mtime_to_ns, remote_join
from .utils import fmt_bytes

__all__ = [
    "_openlist_info_is_missing",
    "_openlist_get_file_size",
    "_openlist_autorename_index",
    "_openlist_find_autorename_candidate",
    "_openlist_try_fix_autorename_upload",
    "_rewrite_out_rel_for_uploaded_path",
    "upload_file_remote",
]


def _openlist_info_is_missing(info: Any) -> bool:
    # OpenList 的 fs.info 在"对象不存在"时可能不会抛异常，而是返回"空对象"（取决于 openlist 客户端版本）。
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
            mtime_ns = mtime_to_ns(getattr(obj, "modified", None))
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
    # 处理服务端自动改名导致的"实际文件名 != dst_path"。
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

    # overwrite=False 时，OpenList 可能会在"目标已存在"时自动改名（例如追加 " 1"），导致 state 记录与实际文件名不一致。
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
