from __future__ import annotations

import posixpath
from typing import Iterator, List

from .openlist_client import OpenListClientSync, RemoteEntry, FatalAuthError, _mtime_to_ns

__all__ = ["list_openlist_recursive", "iter_openlist_recursive"]


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
