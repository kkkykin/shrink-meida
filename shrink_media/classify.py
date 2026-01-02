from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .constants import (
    VIDEO_EXTS, AUDIO_EXTS, IMAGE_EXTS, ANIMATED_IMAGE_EXTS, SUB_EXTS,
    COMIC_EXTS, BITMAP_SUB_CODECS, TEXT_SUB_CODECS
)
from .utils import looks_like_archive_name

__all__ = [
    "MediaInfo",
    "classify",
    "detect_subtitle_compat",
    "get_main_video_stream",
]


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
