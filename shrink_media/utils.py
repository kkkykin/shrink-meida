from __future__ import annotations

import hashlib
import shutil
import signal
from pathlib import Path
from typing import Any, Optional

from rich.filesize import decimal as format_bytes

from .constants import ARCHIVE_EXTS, STATE_DEFAULT_NAME, STATE_DIR_NAME, LOCKS_DIR_NAME

__all__ = [
    "is_url",
    "ensure_parent",
    "tail_text",
    "describe_returncode",
    "extract_result_from_ffmpeg_err",
    "sha1_hex",
    "safe_int",
    "size_of",
    "fmt_bytes",
    "fmt_size_change",
    "looks_like_archive_name",
    "should_ignore_name",
    "find_7z",
]


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
