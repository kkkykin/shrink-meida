"""Capability detection for worker."""
from __future__ import annotations

import shutil
import subprocess
from typing import Any


def detect_capabilities() -> dict[str, Any]:
    """Detect worker capabilities (ffmpeg encoders, 7z, etc)."""
    caps: dict[str, Any] = {}

    # Check ffmpeg/ffprobe
    caps["has_ffmpeg"] = shutil.which("ffmpeg") is not None
    caps["has_ffprobe"] = shutil.which("ffprobe") is not None

    # Check 7z
    caps["has_7z"] = shutil.which("7z") is not None or shutil.which("7zz") is not None

    # Detect ffmpeg encoders
    if caps["has_ffmpeg"]:
        try:
            cp = subprocess.run(
                ["ffmpeg", "-hide_banner", "-encoders"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,
            )
            encoder_text = ((cp.stdout or "") + "\n" + (cp.stderr or "")).strip()

            # Check for specific encoders
            caps["has_libx264"] = "libx264" in encoder_text
            caps["has_libx265"] = "libx265" in encoder_text
            caps["has_h264_nvenc"] = "h264_nvenc" in encoder_text
            caps["has_hevc_nvenc"] = "hevc_nvenc" in encoder_text
            caps["has_libopus"] = "libopus" in encoder_text
            caps["has_libwebp"] = "libwebp" in encoder_text
            caps["has_libaom_av1"] = "libaom-av1" in encoder_text
        except Exception:
            pass

    return caps
