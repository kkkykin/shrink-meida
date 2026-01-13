"""OpenList manager for server-side operations."""
from __future__ import annotations

import posixpath
from typing import Optional
from urllib.parse import quote

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

    def get_download_url(self, path: str) -> dict:
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

        full_url = f"{self.base_url}{url_path}"
        return {"url": full_url, "expires_at": None}

    def get_direct_upload_info(self, dst_path: str, size: int) -> Optional[dict]:
        """
        Get direct upload info from OpenList.
        Returns: {upload_url: str, method: str, chunk_size: int, headers: dict} or None
        """
        try:
            from pathlib import Path
            import httpx

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

            return {
                "upload_url": upload_url,
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
        return self.client.info(path)

    def listdir(self, path: str, refresh: bool = True):
        """List directory contents."""
        return self.client.listdir(path, refresh=refresh)

    def rename(self, src: str, dst: str):
        """Rename/move a file."""
        self.client.rename(src, dst)

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

            # Rename staging to final
            self.rename(staging_path, final_path)

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
