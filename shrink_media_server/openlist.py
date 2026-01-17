"""OpenList manager for server-side operations."""
from __future__ import annotations

import posixpath
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlsplit, urlunsplit

from shrink_media.openlist_client import OpenListClientSync


class OpenListManager:
    """Server-side OpenList manager with capability generation."""

    def __init__(self, base_url: str, user: str, password: str, otp_key: Optional[str] = None):
        self.client = OpenListClientSync(
            base_url=base_url,
            user=user,
            password=password,
            timeout=300,
            otp_key=otp_key,
            retries=3,
        )
        self.base_url = base_url.rstrip("/")

    @staticmethod
    def _rewrite_url_base(url: str, *, base_url: str) -> str:
        base_url2 = (base_url or "").strip().rstrip("/")
        if not base_url2:
            return url

        b = urlsplit(base_url2)
        if not b.scheme or not b.netloc:
            return url

        u = urlsplit(url)
        path = u.path or "/"
        if not path.startswith("/"):
            path = "/" + path

        base_path = b.path.rstrip("/")
        if base_path and base_path != "/":
            if path != base_path and not path.startswith(base_path + "/"):
                path = base_path + path

        return urlunsplit((b.scheme, b.netloc, path, u.query, u.fragment))

    def get_download_url(self, path: str, *, base_url: str | None = None) -> dict:
        """
        Generate a download URL with sign for the given path.
        Returns: {url: str, expires_at: Optional[str]}
        """
        # Try to get file info to obtain sign
        sign = ""
        try:
            info = self.client.info(path)
            sign = getattr(info, "sign", "") or ""
        except Exception:
            # If info fails (e.g., Pydantic validation error), continue without sign
            pass

        # Build download URL
        url_path = f"/d{quote(path, safe='/')}"
        if sign:
            sep = "&" if "?" in url_path else "?"
            url_path += f"{sep}sign={sign}"

        base = (base_url or self.base_url).rstrip("/")
        full_url = f"{base}{url_path}"
        return {"url": full_url, "expires_at": None}

    def get_direct_upload_info(self, dst_path: str, size: int, *, base_url: str | None = None) -> Optional[dict]:
        """
        Get direct upload info from OpenList.
        Returns: {upload_url: str, method: str, chunk_size: int, headers: dict} or None
        """
        try:
            from pathlib import Path

            payload = {
                "path": str(Path(dst_path).parent).replace("\\", "/"),
                "file_name": Path(dst_path).name,
                "file_size": size,
                "tool": "HttpDirect",
            }

            token = self.client._client.get_token()
            headers = {"Content-Type": "application/json"}
            if token:
                headers["Authorization"] = token

            # Use the client's httpx client
            async def _get_info():
                r = await self.client._client.context.httpx_client.post(
                    "/api/fs/get_direct_upload_info",
                    json=payload,
                    headers=headers
                )
                if r.status_code != 200:
                    return None
                data = r.json()
                if data.get("code") != 200:
                    return None
                return data.get("data")

            info = self.client._call(_get_info())
            if not info:
                return None

            upload_url = info.get("upload_url")
            chunk_size = int(info.get("chunk_size") or 0) or 5 * 1024 * 1024
            method = (info.get("method") or "PUT").upper()

            if not upload_url:
                return None

            if base_url is not None:
                upload_url = self._rewrite_url_base(str(upload_url), base_url=base_url)
            elif isinstance(upload_url, str) and upload_url.startswith("/"):
                # Make sure workers don't treat it as a server-relative URL.
                upload_url = f"{self.base_url}{upload_url}"

            return {
                "url": upload_url,
                "method": method,
                "chunk_size": chunk_size,
                "headers": {},
            }
        except Exception:
            return None

    def ensure_dir(self, path: str):
        """Ensure directory exists."""
        self.client.ensure_dir(path)

    def info(self, path: str):
        """Get file/directory info."""
        try:
            info = self.client.info(path)
        except FileNotFoundError:
            return None
        if info is None:
            return None
        name = getattr(info, "name", None)
        p = getattr(info, "path", None)
        sign0 = getattr(info, "sign", None)
        size0 = getattr(info, "size", None)
        modified0 = getattr(info, "modified", None)
        if (
            name in (None, "")
            and p in (None, "")
            and sign0 in (None, "")
            and (size0 in (None, 0))
            and modified0 is None
        ):
            return None
        return info

    def listdir(self, path: str, refresh: bool = True):
        """List directory contents."""
        return self.client.listdir(path, refresh=refresh)

    def download_to(self, remote_path: str, local_path: Path) -> None:
        """Download a file to local filesystem (proxy fallback)."""
        self.client.download_to(remote_path, local_path)

    def upload_file(self, remote_path: str, local_file: Path, *, overwrite: bool) -> None:
        """Upload a local file to OpenList (proxy fallback)."""
        self.client.upload_file(remote_path, local_file, overwrite=overwrite)

    def rename(self, src: str, dst: str):
        """Rename/move a file."""
        self.client.rename(src, dst)

    def move(self, src: str, dst_dir: str):
        """Move a file into destination directory."""
        self.client.move(src, dst_dir)

    def remove(self, path: str):
        """Remove a file."""
        self.client.remove(path)

    def finalize(self, staging_path: str, final_path: str, expected_size: int) -> dict:
        """
        Finalize a staged file by moving it to the final location.
        Returns: {ok: bool, error: Optional[str], final_size: int}
        """
        try:
            # Verify staging file exists and size matches
            staging_info = self.info(staging_path)
            if not staging_info:
                return {"ok": False, "error": "staging file not found", "final_size": 0}

            staging_size = getattr(staging_info, "size", 0)
            if staging_size != expected_size:
                return {
                    "ok": False,
                    "error": f"size mismatch: expected {expected_size}, got {staging_size}",
                    "final_size": staging_size,
                }

            # Check if final path already exists
            try:
                final_info = self.info(final_path)
                if final_info:
                    final_size = getattr(final_info, "size", 0)
                    if final_size == expected_size:
                        # Already exists with correct size - idempotent success
                        # Remove staging file
                        self.remove(staging_path)
                        return {"ok": True, "error": None, "final_size": final_size}
                    else:
                        # Exists but wrong size - conflict
                        return {
                            "ok": False,
                            "error": f"final path exists with different size: {final_size} != {expected_size}",
                            "final_size": final_size,
                        }
            except FileNotFoundError:
                pass  # Final path doesn't exist, proceed with rename

            # Ensure parent directory exists
            parent_dir = str(posixpath.dirname(final_path))
            if parent_dir and parent_dir != "/":
                self.ensure_dir(parent_dir)

            # Move staging to final directory, then rename to final basename.
            self.move(staging_path, parent_dir)
            moved_path = str(posixpath.join(parent_dir or "/", Path(staging_path).name))
            self.rename(moved_path, final_path)

            # Verify final file
            final_info = self.info(final_path)
            if not final_info:
                return {"ok": False, "error": "final file not found after rename", "final_size": 0}

            final_size = getattr(final_info, "size", 0)
            if final_size != expected_size:
                return {
                    "ok": False,
                    "error": f"final size mismatch after rename: expected {expected_size}, got {final_size}",
                    "final_size": final_size,
                }

            return {"ok": True, "error": None, "final_size": final_size}

        except Exception as e:
            return {"ok": False, "error": str(e), "final_size": 0}

    def close(self):
        """Close the OpenList client."""
        self.client.close()
