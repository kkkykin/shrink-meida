"""ffmpeg 命令构建模块。"""

from __future__ import annotations

import logging
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .logging import _LOGGER, _DEBUG_ENABLED, log
from .constants import LOSSY_AUDIO_CODECS
from .utils import ensure_parent, tail_text, describe_returncode, safe_int
from .probe import has_encoder, opus_bitrate_for_channels, map_pix_fmt_for_video, map_pix_fmt_for_avif
from .classify import MediaInfo, detect_subtitle_compat, get_main_video_stream

__all__ = [
    "nvenc_preset_from_generic",
    "replace_arg",
    "insert_after_pair",
    "replace_last",
    "build_video_cmd_single",
    "build_video_candidates",
    "build_audio_candidates",
    "build_image_candidates",
    "run_ffmpeg_with_candidates",
]


def nvenc_preset_from_generic(p: str) -> str:
    p = (p or "").strip().lower()
    if re.fullmatch(r"p[1-7]", p):
        return p
    mapping = {
        "ultrafast": "p1",
        "superfast": "p2",
        "veryfast": "p3",
        "faster": "p4",
        "fast": "p5",
        "medium": "p5",
        "slow": "p6",
        "slower": "p7",
        "veryslow": "p7",
    }
    return mapping.get(p, "p6")


def replace_arg(cmd: List[str], key: str, new_value: str) -> Optional[List[str]]:
    idx = None
    for i in range(len(cmd) - 1):
        if cmd[i] == key:
            idx = i
    if idx is None:
        return None
    out = cmd.copy()
    out[idx + 1] = new_value
    return out


def insert_after_pair(cmd: List[str], key: str, value: str, insert: List[str]) -> Optional[List[str]]:
    for i in range(len(cmd) - 1):
        if cmd[i] == key and cmd[i + 1] == value:
            out = cmd.copy()
            out[i + 2 : i + 2] = insert
            return out
    return None


def replace_last(cmd: List[str], new_last: str) -> List[str]:
    out = cmd.copy()
    out[-1] = new_last
    return out


def build_video_cmd_single(
    in_path: str | Path,
    out_path: Path,
    info: MediaInfo,
    *,
    container_pref: str,
    video_policy: str,
    audio_policy: str,
    allow_opus_in_mp4: bool,
    encoder: str,  # hevc_nvenc/libx265/libx264
    video_crf: int,
    video_preset: str,
    pix_fmt: str,
    faststart: bool,
) -> Tuple[List[str], Optional[List[str]]]:
    streams = info.streams
    has_subs, mp4_sub_ok = detect_subtitle_compat(streams)

    container = container_pref
    if container == "auto":
        container = "mp4"
    if container == "mp4" and has_subs and not mp4_sub_ok:
        container = "mkv"

    cmd: List[str] = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-y",
        "-i",
        str(in_path),
        "-map",
        "0",
        "-map_metadata",
        "0",
        "-map_chapters",
        "0",
    ]
    if container == "mp4":
        cmd += ["-dn", "-map", "-0:t?"]

    v_stream = get_main_video_stream(info)
    v_codec_in = (v_stream.get("codec_name") or "").lower() if v_stream else ""
    src_pf = str(v_stream.get("pix_fmt")) if (v_stream and v_stream.get("pix_fmt")) else None
    src_range = (v_stream.get("color_range") or "").strip().lower() if v_stream else ""
    src_full_range = bool(src_range == "pc" or (src_pf or "").strip().lower().startswith("yuvj"))

    want_pix_retry = False
    retry_cmd: Optional[List[str]] = None

    # video
    if video_policy == "always_copy" or (video_policy == "copy_if_hevc" and v_codec_in in {"hevc", "h265"}):
        cmd += ["-c:v", "copy"]
    else:
        if encoder == "hevc_nvenc":
            cmd += [
                "-c:v",
                "hevc_nvenc",
                "-preset",
                nvenc_preset_from_generic(video_preset),
                "-tune",
                "hq",
                "-rc",
                "constqp",
                "-qp",
                str(video_crf),
            ]
        elif encoder == "libx264":
            cmd += ["-c:v", "libx264", "-preset", video_preset, "-crf", str(video_crf)]
        else:
            cmd += ["-c:v", "libx265", "-preset", video_preset, "-crf", str(video_crf)]

        if pix_fmt != "auto":
            cmd += ["-pix_fmt", pix_fmt]
            if pix_fmt != "yuv420p":
                want_pix_retry = True
        else:
            mapped = map_pix_fmt_for_video(src_pf)
            if mapped:
                cmd += ["-pix_fmt", mapped]
                if mapped != "yuv420p":
                    want_pix_retry = True

        # yuvj*/pc(full-range) 来源若直接转成 yuv*，需要显式指定 range，否则可能出现亮度/对比度偏移。
        if src_full_range:
            cmd += ["-vf", "scale=in_range=pc:out_range=pc"]

        if container == "mp4":
            cmd += ["-tag:v", "hvc1"]

    # audio
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    lossy_in = False
    if audio_streams:
        c0 = (audio_streams[0].get("codec_name") or "").lower()
        if c0 in LOSSY_AUDIO_CODECS:
            lossy_in = True

    out_audio_codec = "opus"
    if container == "mp4" and not allow_opus_in_mp4:
        out_audio_codec = "aac"

    if audio_policy == "always_copy":
        cmd += ["-c:a", "copy"]
    elif audio_policy == "copy_if_lossy" and lossy_in:
        cmd += ["-c:a", "copy"]
    else:
        if out_audio_codec == "opus" and has_encoder("libopus"):
            cmd += ["-c:a", "libopus", "-vbr", "on", "-compression_level", "10", "-application", "audio"]
            for i, s in enumerate(audio_streams):
                cmd += [f"-b:a:{i}", opus_bitrate_for_channels(safe_int(s.get("channels")))]
        else:
            if has_encoder("aac") or has_encoder("libfdk_aac"):
                cmd += ["-c:a", "aac", "-b:a", "192k"]
            else:
                cmd += ["-c:a", "copy"]

    # subtitles
    sub_streams = [s for s in streams if s.get("codec_type") == "subtitle"]
    if sub_streams:
        if container == "mp4":
            cmd += ["-c:s", "mov_text"]
        else:
            cmd += ["-c:s", "copy"]

    if container == "mp4":
        movflags = ["use_metadata_tags"]
        if faststart:
            movflags.insert(0, "faststart")
        cmd += ["-movflags", "+" + "+".join(movflags)]

    cmd += [str(out_path)]

    if want_pix_retry:
        retry_cmd = replace_arg(cmd, "-pix_fmt", "yuv420p")

    return cmd, retry_cmd


def build_video_candidates(
    in_path: str | Path,
    out_path: Path,
    info: MediaInfo,
    *,
    container_pref: str,
    video_policy: str,
    audio_policy: str,
    allow_opus_in_mp4: bool,
    video_encoder: str,  # auto/hevc_nvenc/libx265/libx264
    video_crf: int,
    video_preset: str,
    pix_fmt: str,
    faststart: bool,
) -> List[List[str]]:
    cands: List[List[str]] = []

    def add(enc: str, *, preset_override: Optional[str] = None) -> None:
        cmd1, retry = build_video_cmd_single(
            in_path,
            out_path,
            info,
            container_pref=container_pref,
            video_policy=video_policy,
            audio_policy=audio_policy,
            allow_opus_in_mp4=allow_opus_in_mp4,
            encoder=enc,
            video_crf=video_crf,
            video_preset=preset_override or video_preset,
            pix_fmt=pix_fmt,
            faststart=faststart,
        )
        cands.append(cmd1)
        if retry:
            cands.append(retry)
        # NVENC 在部分环境（例如 Debian 的 ffmpeg build + mp4）可能会遇到 muxer 报错：
        #   [mp4] pts/dts pair unsupported
        # 这里额外尝试一次禁用 B-frames（避免 pts/dts 不匹配），以提升 NVENC 成功率。
        if enc == "hevc_nvenc":
            bf0 = insert_after_pair(cmd1, "-c:v", "hevc_nvenc", ["-bf", "0"])
            if bf0:
                cands.append(bf0)
                bf0_retry = replace_arg(bf0, "-pix_fmt", "yuv420p")
                if bf0_retry:
                    cands.append(bf0_retry)

    if video_encoder == "auto":
        has_nvenc = has_encoder("hevc_nvenc")
        has_x265 = has_encoder("libx265")
        has_x264 = has_encoder("libx264")

        if has_nvenc:
            add("hevc_nvenc")
        if has_x265:
            add("libx265")
            if video_preset in {"slow", "slower", "veryslow"}:
                add("libx265", preset_override="medium")
        elif has_x264:
            add("libx264")

        # 最后兜底：当 x265/NVENC 失败时仍尝试 x264（除非强制 always_hevc）。
        if video_policy != "always_hevc" and has_x264:
            add("libx264")
    else:
        add(video_encoder)
        if video_encoder == "hevc_nvenc":
            if has_encoder("libx265"):
                add("libx265")
                if video_preset in {"slow", "slower", "veryslow"}:
                    add("libx265", preset_override="medium")
            if video_policy != "always_hevc" and has_encoder("libx264"):
                add("libx264")
        if video_encoder == "libx265":
            if video_preset in {"slow", "slower", "veryslow"}:
                add("libx265", preset_override="medium")
            if video_policy != "always_hevc" and has_encoder("libx264"):
                add("libx264")

    # 去重
    seen = set()
    uniq: List[List[str]] = []
    for c in cands:
        k = "\0".join(c)
        if k not in seen:
            seen.add(k)
            uniq.append(c)
    return uniq


def build_audio_candidates(
    in_path: str | Path,
    out_path: Path,
    info: MediaInfo,
    *,
    audio_policy: str,
) -> List[List[str]]:
    audio_streams = [s for s in info.streams if s.get("codec_type") == "audio"]
    if not audio_streams:
        return []

    lossy_in = False
    c0 = (audio_streams[0].get("codec_name") or "").lower()
    if c0 in LOSSY_AUDIO_CODECS:
        lossy_in = True

    if audio_policy == "always_copy" or (audio_policy == "copy_if_lossy" and lossy_in):
        return [
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-y",
                "-i",
                str(in_path),
                "-map_metadata",
                "0",
                "-map_chapters",
                "0",
                "-vn",
                "-map",
                "0:a",
                "-c:a",
                "copy",
                str(out_path),
            ]
        ]

    if has_encoder("libopus"):
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-y",
            "-i",
            str(in_path),
            "-map_metadata",
            "0",
            "-map_chapters",
            "0",
            "-vn",
            "-map",
            "0:a",
            "-c:a",
            "libopus",
            "-vbr",
            "on",
            "-compression_level",
            "10",
            "-application",
            "audio",
        ]
        for i, s in enumerate(audio_streams):
            cmd += [f"-b:a:{i}", opus_bitrate_for_channels(safe_int(s.get("channels")))]
        cmd += [str(out_path)]
        return [cmd]

    return [
        [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-y",
            "-i",
            str(in_path),
            "-map_metadata",
            "0",
            "-map_chapters",
            "0",
            "-vn",
            "-map",
            "0:a",
            "-c:a",
            "copy",
            str(out_path),
        ]
    ]


def build_image_candidates(
    in_path: str | Path,
    out_path: Path,
    *,
    image_codec: str,  # webp/avif
    webp_quality: int,
    webp_lossless: bool,
    avif_crf: int,
    avif_pix_fmt: str,
    src_pix_fmt: Optional[str],
) -> List[List[str]]:
    if image_codec == "webp":
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-y",
            "-i",
            str(in_path),
            "-frames:v",
            "1",
            "-c:v",
            "libwebp",
            "-compression_level",
            "6",
        ]
        if webp_lossless:
            cmd += ["-lossless", "1"]
        else:
            cmd += ["-q:v", str(webp_quality)]
        cmd += [str(out_path)]
        return [cmd]

    # avif fallback webp
    if not has_encoder("libaom-av1"):
        out2 = out_path.with_suffix(".webp")
        return build_image_candidates(
            in_path,
            out2,
            image_codec="webp",
            webp_quality=webp_quality,
            webp_lossless=webp_lossless,
            avif_crf=avif_crf,
            avif_pix_fmt=avif_pix_fmt,
            src_pix_fmt=src_pix_fmt,
        )

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-y",
        "-i",
        str(in_path),
        "-frames:v",
        "1",
        "-c:v",
        "libaom-av1",
        "-still-picture",
        "1",
        "-crf",
        str(avif_crf),
        "-b:v",
        "0",
    ]

    if avif_pix_fmt != "auto":
        cmd += ["-pix_fmt", avif_pix_fmt]
        cmd += [str(out_path)]
        if avif_pix_fmt != "yuv420p":
            retry = replace_arg(cmd, "-pix_fmt", "yuv420p")
            return [cmd, retry] if retry else [cmd]
        return [cmd]

    mapped = map_pix_fmt_for_avif(src_pix_fmt)
    if mapped:
        cmd += ["-pix_fmt", mapped]
    cmd += [str(out_path)]
    if mapped and mapped != "yuv420p":
        retry = replace_arg(cmd, "-pix_fmt", "yuv420p")
        return [cmd, retry] if retry else [cmd]
    return [cmd]


def run_ffmpeg_with_candidates(
    candidates: List[List[str]],
    out_final: Path,
    *,
    overwrite: bool,
    dry_run: bool,
) -> Tuple[bool, str, bool]:
    if dry_run:
        for c in candidates:
            log("[dry-run] " + " ".join(c))
        return True, "dry-run", False

    if out_final.exists() and not overwrite:
        return True, f"output exists: {out_final}", False

    if _DEBUG_ENABLED:
        _LOGGER.debug("ffmpeg candidates=%d out=%s", len(candidates), out_final)

    # 保留原始扩展名，避免 ffmpeg 因未知扩展无法推断格式
    suffix = out_final.suffix
    tmp_name = out_final.stem + ".__tmp__" + suffix
    out_tmp = out_final.with_name(tmp_name)
    ensure_parent(out_tmp)
    last_err = ""

    for idx, cmd in enumerate(candidates):
        cmd2 = replace_last(cmd, str(out_tmp))
        if _DEBUG_ENABLED:
            _LOGGER.debug("ffmpeg run %d/%d: %s", idx + 1, len(candidates), shlex.join(cmd2))

        try:
            if out_tmp.exists():
                out_tmp.unlink()
        except Exception:
            pass

        # ffmpeg 可能输出非 UTF-8 字符（例如携带非 UTF-8 元数据），避免 decode 失败
        cp = subprocess.run(
            cmd2,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if cp.returncode == 0:
            if _DEBUG_ENABLED:
                _LOGGER.debug("ffmpeg ok %d/%d -> %s", idx + 1, len(candidates), out_tmp)
            try:
                if out_final.exists():
                    if overwrite:
                        out_final.unlink()
                    else:
                        try:
                            out_tmp.unlink()
                        except Exception:
                            pass
                        return True, f"output exists: {out_final}", False
                ensure_parent(out_final)
                out_tmp.replace(out_final)
                return True, "ok", True
            except Exception as e:
                try:
                    if out_tmp.exists():
                        out_tmp.unlink()
                except Exception:
                    pass
                return False, f"rename failed: {e}", False
        else:
            try:
                if out_tmp.exists():
                    out_tmp.unlink()
            except Exception:
                pass
            cmd_s = shlex.join(cmd2)
            rc_s = describe_returncode(cp.returncode)
            stderr_tail = tail_text(cp.stderr, n_lines=60, max_chars=6000)
            last_err = f"cmd: {cmd_s}\nresult: {rc_s}\n{stderr_tail}".strip()
            if _DEBUG_ENABLED:
                _LOGGER.debug("ffmpeg fail %d/%d exit=%d: %s", idx + 1, len(candidates), cp.returncode, last_err)

    return False, (last_err or "ffmpeg failed"), False
