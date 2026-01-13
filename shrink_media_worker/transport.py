"""HTTP transport for worker (download/upload)."""
from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import httpx


def download_file(url: str, dest: Path, *, headers: Optional[dict] = None, timeout: int = 300) -> None:
    """Download file from URL to local path."""
    headers = headers or {}
    with httpx.stream("GET", url, headers=headers, timeout=timeout, follow_redirects=True) as resp:
        resp.raise_for_status()
        with dest.open("wb") as f:
            for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                f.write(chunk)


def upload_file_chunked(
    url: str,
    src: Path,
    *,
    method: str = "PUT",
    chunk_size: int = 5 * 1024 * 1024,
    headers: Optional[dict] = None,
    timeout: int = 300,
) -> dict:
    """Upload file in chunks with Content-Range support."""
    headers = headers or {}
    total_size = src.stat().st_size

    # Single-shot upload for small files
    if total_size <= chunk_size:
        with src.open("rb") as f:
            resp = httpx.request(
                method,
                url,
                content=f.read(),
                headers=headers,
                timeout=timeout,
                follow_redirects=True,
            )
            resp.raise_for_status()
            return resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}

    # Chunked upload
    offset = 0
    with src.open("rb") as f:
        while offset < total_size:
            chunk_end = min(offset + chunk_size, total_size) - 1
            chunk_data = f.read(chunk_size)
            chunk_headers = {
                **headers,
                "Content-Range": f"bytes {offset}-{chunk_end}/{total_size}",
            }

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    resp = httpx.request(
                        method,
                        url,
                        content=chunk_data,
                        headers=chunk_headers,
                        timeout=timeout,
                        follow_redirects=True,
                    )
                    resp.raise_for_status()
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(2 ** attempt)

            offset = chunk_end + 1

    # Final response
    return resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
