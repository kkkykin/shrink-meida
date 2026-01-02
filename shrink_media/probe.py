from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from .logging import log_err

__all__ = [
    "require_tools",
    "ffmpeg_encoders",
    "has_encoder",
    "ffprobe_json",
    "opus_bitrate_for_channels",
    "normalize_pix_fmt",
    "map_pix_fmt_for_video",
    "map_pix_fmt_for_avif",
]

_ENCODER_CACHE: Optional[str] = None


def require_tools() -> None:
    for t in ("ffmpeg", "ffprobe"):
        if shutil.which(t) is None:
            log_err(f"ERROR: missing {t}, please install and ensure it is in PATH.")
            sys.exit(2)


def ffmpeg_encoders() -> str:
    global _ENCODER_CACHE
    if _ENCODER_CACHE is None:
        cp = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
