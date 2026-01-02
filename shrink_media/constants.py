from __future__ import annotations

# ------------------------
# 常量/扩展名
# ------------------------

VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv", ".wmv", ".m4v", ".ts", ".m2ts", ".mts", ".3gp", ".3g2"}
AUDIO_EXTS = {".mp3", ".aac", ".m4a", ".wav", ".flac", ".ogg", ".opus", ".wma", ".alac"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff", ".bmp", ".heic", ".heif", ".avif"}
ANIMATED_IMAGE_EXTS = {".gif", ".apng"}
SUB_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub"}

COMIC_EXTS = {".cbz", ".cbr", ".cb7"}
ARCHIVE_EXTS = {".zip", ".7z", ".rar", ".tar", ".gz", ".bz2", ".xz", ".zst", ".tgz", ".tbz2", ".txz"}

LOSSY_AUDIO_CODECS = {"aac", "mp3", "opus", "vorbis", "ac3", "eac3", "dts", "mp2", "wma"}
TEXT_SUB_CODECS = {"subrip", "srt", "ass", "ssa", "mov_text", "webvtt"}
BITMAP_SUB_CODECS = {"hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle", "xsub", "vobsub"}

STATE_DEFAULT_NAME = ".shrink_media_state.jsonl"
STATE_DIR_NAME = ".shrink_media_state"
LOCKS_DIR_NAME = ".shrink_media_locks"

__all__ = [
    "VIDEO_EXTS",
    "AUDIO_EXTS",
    "IMAGE_EXTS",
    "ANIMATED_IMAGE_EXTS",
    "SUB_EXTS",
    "COMIC_EXTS",
    "ARCHIVE_EXTS",
    "LOSSY_AUDIO_CODECS",
    "TEXT_SUB_CODECS",
    "BITMAP_SUB_CODECS",
    "STATE_DEFAULT_NAME",
    "STATE_DIR_NAME",
    "LOCKS_DIR_NAME",
]
