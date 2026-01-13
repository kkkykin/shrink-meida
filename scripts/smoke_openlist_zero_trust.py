from __future__ import annotations

import argparse
import hashlib
import json
import os
import posixpath
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote

import httpx


@dataclass
class Route:
    id: str
    in_root: str
    out_root: str


def _load_kv_file(p: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not p.exists():
        return out
    for line in p.read_text("utf-8", errors="replace").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" in s:
            k, v = s.split("=", 1)
        elif ":" in s:
            k, v = s.split(":", 1)
        else:
            continue
        out[k.strip()] = v.strip()
    return out


def _load_routes(p: Path) -> list[Route]:
    obj = json.loads(p.read_text("utf-8", errors="replace"))
    if not isinstance(obj, list):
        raise SystemExit("ERROR: routes file must be a JSON array")
    routes: list[Route] = []
    for it in obj:
        if not isinstance(it, dict):
            continue
        rid = str(it.get("id") or "").strip()
        in_root = str(it.get("in_root") or "").strip()
        out_root = str(it.get("out_root") or "").strip()
        if not rid or not in_root or not out_root:
            continue
        routes.append(Route(rid, in_root, out_root))
    if not routes:
        raise SystemExit("ERROR: no valid routes found")
    return routes


def _openlist_download_url(base_url: str, remote_path: str, *, sign: str) -> str:
    p = remote_path if remote_path.startswith("/") else f"/{remote_path}"
    url_path = f"/d{quote(p, safe='/')}"
    if sign:
        sep = "&" if "?" in url_path else "?"
        url_path += f"{sep}sign={quote(sign, safe='')}"
    return base_url.rstrip("/") + url_path


def _alist_hash_password(password: str) -> str:
    # OpenList(AList) 的 /api/auth/login/hash 需要：sha256(f"{pass}-{STATIC_HASH_SALT}")
    static_salt = "https://github.com/alist-org/alist"
    return hashlib.sha256(f"{password}-{static_salt}".encode("utf-8")).hexdigest()


def _api_post(c: httpx.Client, base_url: str, endpoint: str, *, json_body: dict, token: str | None) -> dict:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = token
    r = c.post(base_url.rstrip("/") + endpoint, json=json_body, headers=headers)
    if r.status_code == 401:
        raise SystemExit(f"ERROR: http 401 for {endpoint}")
    if r.status_code >= 400:
        raise SystemExit(f"ERROR: http {r.status_code} for {endpoint}: {r.text[:200]}")
    try:
        data = r.json()
    except Exception:
        raise SystemExit(f"ERROR: invalid JSON response for {endpoint}")
    if isinstance(data, dict) and int(data.get("code") or 0) != 200:
        raise SystemExit(f"ERROR: api code={data.get('code')} for {endpoint}: {data.get('message')}")
    if not isinstance(data, dict):
        raise SystemExit(f"ERROR: unexpected response type for {endpoint}")
    return data


def _login_hash(c: httpx.Client, base_url: str, *, user: str, password: str, otp_code: str | None) -> str:
    payload = {
        "username": user,
        "password": _alist_hash_password(password),
        "otp_code": otp_code,
    }
    data = _api_post(c, base_url, "/api/auth/login/hash", json_body=payload, token=None)
    token = (((data.get("data") or {}) if isinstance(data, dict) else {}) or {}).get("token")
    if not isinstance(token, str) or not token:
        raise SystemExit("ERROR: login returned no token")
    return token


def _fs_list(c: httpx.Client, base_url: str, token: str, path: str) -> list[dict[str, Any]]:
    payload = {"path": path, "refresh": True, "page": 1, "per_page": 100}
    data = _api_post(c, base_url, "/api/fs/list", json_body=payload, token=token)
    d = data.get("data") if isinstance(data, dict) else None
    content = (d or {}).get("content") if isinstance(d, dict) else None
    if not isinstance(content, list):
        return []
    return [x for x in content if isinstance(x, dict)]


def _fs_get(c: httpx.Client, base_url: str, token: str, path: str) -> dict[str, Any]:
    payload = {"path": path}
    data = _api_post(c, base_url, "/api/fs/get", json_body=payload, token=token)
    d = data.get("data") if isinstance(data, dict) else None
    return d if isinstance(d, dict) else {}


def _fs_remove(c: httpx.Client, base_url: str, token: str, full_path: str) -> None:
    dir_path = posixpath.dirname(full_path.rstrip("/")) or "/"
    name = posixpath.basename(full_path.rstrip("/"))
    payload = {"dir": dir_path, "names": [name]}
    _api_post(c, base_url, "/api/fs/remove", json_body=payload, token=token)


def _pick_first_file(c: httpx.Client, base_url: str, token: str, root: str) -> str:
    root_norm = root.rstrip("/") or "/"
    for obj in _fs_list(c, base_url, token, root_norm):
        if bool(obj.get("is_dir")):
            continue
        name = str(obj.get("name") or "").strip()
        if not name:
            continue
        return posixpath.join(root_norm, name)
    raise SystemExit(f"ERROR: no files found under {root_norm!r}. Run `make seed` first.")


def _http_get_size(c: httpx.Client, url: str) -> tuple[int, int]:
    with c.stream("GET", url) as r:
        status = r.status_code
        n = 0
        for chunk in r.iter_bytes():
            n += len(chunk)
    return status, n


def _get_direct_upload_info(
    c: httpx.Client, base_url: str, token: str, *, dst_path: str, size: int
) -> Optional[Dict[str, Any]]:
    payload = {
        "path": str(Path(dst_path).parent).replace("\\", "/"),
        "file_name": Path(dst_path).name,
        "file_size": size,
        "tool": "HttpDirect",
    }
    headers = {"Authorization": token} if token else {}
    r = c.post(base_url.rstrip("/") + "/api/fs/get_direct_upload_info", json=payload, headers=headers)
    if r.status_code == 401:
        raise SystemExit("ERROR: server login token rejected when requesting direct upload info")
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    if int(data.get("code") or 0) != 200:
        return None
    return data.get("data")


def _direct_upload_no_auth(c: httpx.Client, upload_url: str, *, method: str, chunk_size: int, data: bytes) -> bool:
    size = len(data)
    method = (method or "PUT").upper()
    chunk_size = int(chunk_size or 0) or 5 * 1024 * 1024
    offset = 0
    while offset < size:
        chunk = data[offset : offset + chunk_size]
        end = offset + len(chunk) - 1
        headers = {
            "Content-Length": str(len(chunk)),
            "Content-Range": f"bytes {offset}-{end}/{size}",
        }
        r = c.request(method, upload_url, content=chunk, headers=headers)
        if r.status_code >= 400:
            return False
        offset += len(chunk)
    return True


def _proxy_upload_with_auth(c: httpx.Client, base_url: str, token: str, *, dst_path: str, data: bytes) -> None:
    path = dst_path if dst_path.startswith("/") else f"/{dst_path}"
    headers = {
        "Authorization": token,
        "Content-Type": "application/octet-stream",
        # OpenList 上传接口使用 File-Path Header（URL 编码后的完整路径）
        "File-Path": quote(path, safe=""),
        "Overwrite": "true",
        "Last-Modified": str(int(time.time())),
    }
    r = c.put(base_url.rstrip("/") + "/api/fs/put", content=data, headers=headers)
    if r.status_code == 401:
        raise SystemExit("ERROR: server login token rejected when uploading via /api/fs/put")
    if r.status_code != 200:
        raise SystemExit(f"FAIL: proxy upload http {r.status_code}: {r.text[:200]}")
    try:
        obj = r.json()
    except Exception:
        raise SystemExit("FAIL: proxy upload returned non-JSON response")
    if not isinstance(obj, dict) or int(obj.get("code") or 0) != 200:
        raise SystemExit(f"FAIL: proxy upload failed: {obj!r}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Smoke test OpenList zero-trust download/upload capability")
    ap.add_argument("--pass-file", type=Path, default=Path("pass.txt"))
    ap.add_argument("--routes", type=Path, default=Path("routes.json"))
    ap.add_argument("--routes-fallback", type=Path, default=Path("routes.example.json"))
    ap.add_argument("--route-id", type=str, default=None)
    ap.add_argument("--timeout", type=int, default=60)
    args = ap.parse_args()

    kv = _load_kv_file(args.pass_file)
    user = kv.get("user") or kv.get("OPENLIST_USER") or os.environ.get("OPENLIST_USER") or ""
    password = (
        kv.get("pass")
        or kv.get("password")
        or kv.get("OPENLIST_PASS")
        or os.environ.get("OPENLIST_PASS")
        or ""
    )
    base_url = kv.get("base_url") or kv.get("OPENLIST_BASE_URL") or os.environ.get("OPENLIST_BASE_URL") or "http://127.0.0.1:15244"
    otp_code = kv.get("otp_code") or kv.get("OPENLIST_OTP_CODE") or os.environ.get("OPENLIST_OTP_CODE") or None
    if not user or not password:
        raise SystemExit("ERROR: missing user/pass. Create pass.txt from pass.example.txt.")

    routes_path = args.routes if args.routes.exists() else args.routes_fallback
    routes = _load_routes(routes_path)
    route = routes[0]
    if args.route_id:
        for r in routes:
            if r.id == args.route_id:
                route = r
                break
        else:
            raise SystemExit(f"ERROR: route_id {args.route_id!r} not found in {routes_path}")

    with httpx.Client(follow_redirects=True, timeout=float(args.timeout), trust_env=False) as c:
        token = _login_hash(c, base_url, user=user, password=password, otp_code=otp_code)

        # --- download without auth ---
        src_path = _pick_first_file(c, base_url, token, route.in_root)
        info = _fs_get(c, base_url, token, src_path)
        size = int(info.get("size") or 0)
        sign = str(info.get("sign") or "")
        url = _openlist_download_url(base_url, src_path, sign=sign)
        st, n = _http_get_size(c, url)
        if st != 200:
            raise SystemExit(
                f"FAIL: zero-trust download failed (status={st}). "
                "Your OpenList likely requires auth for /d; enable server download_proxy."
            )
        if size > 0 and n != size:
            raise SystemExit(f"FAIL: download size mismatch expected={size} got={n} (enable download_proxy)")

        # --- upload ---
        stamp = int(time.time())
        staging_path = f"{route.out_root.rstrip('/')}/.smoke_{stamp}.bin"
        blob = b"shrink-media smoke test\n" + (b"x" * (256 * 1024))

        info2 = _get_direct_upload_info(c, base_url, token, dst_path=staging_path, size=len(blob))
        if info2 and info2.get("upload_url"):
            ok = _direct_upload_no_auth(
                c,
                str(info2.get("upload_url")),
                method=str(info2.get("method") or "PUT"),
                chunk_size=int(info2.get("chunk_size") or 0),
                data=blob,
            )
            if not ok:
                raise SystemExit("FAIL: direct upload URL rejected without auth. Enable server upload_proxy.")
        else:
            # Local disk storage often has no direct-upload URL; fall back to authenticated upload_proxy.
            _proxy_upload_with_auth(c, base_url, token, dst_path=staging_path, data=blob)

        info3 = _fs_get(c, base_url, token, staging_path)
        sz3 = int(info3.get("size") or 0)
        if sz3 != len(blob):
            raise SystemExit(f"FAIL: uploaded size mismatch expected={len(blob)} got={sz3}")
        try:
            _fs_remove(c, base_url, token, staging_path)
        except Exception:
            pass

        if info2 and info2.get("upload_url"):
            print("OK: zero-trust download (/d?sign=) and direct upload (upload_url) both work.")
        else:
            print("OK: zero-trust download (/d?sign=) works; direct upload unavailable; upload_proxy works.")


if __name__ == "__main__":
    main()
