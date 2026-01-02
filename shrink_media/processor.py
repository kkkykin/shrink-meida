from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from .logging import log
from .utils import ensure_parent, size_of, fmt_bytes, fmt_size_change, extract_result_from_ffmpeg_err, find_7z
from .probe import ffprobe_json
from .classify import classify, detect_subtitle_compat, get_main_video_stream
from .ffmpeg_cmd import build_video_candidates, build_audio_candidates, build_image_candidates, run_ffmpeg_with_candidates
from .comic import comic_smart_skip_already_target, process_comic_to_cbz
from .workitem import apply_out_name_mode

__all__ = [
    "JobResult",
    "copy_file_local",
    "process_one_local",
]


@dataclass
class JobResult:
    ok: bool
    action: str
    msg: str
    out_local: Optional[Path] = None
    out_rel: Optional[str] = None


def copy_file_local(src: Path, dst: Path, *, overwrite: bool) -> None:
    ensure_parent(dst)
    if dst.exists() and not overwrite:
        return
    shutil.copy2(src, dst)


def process_one_local(
    src_local: Path,
    in_root: Path,
    out_root: Path,
    *,
    container: str,
    video_policy: str,
    audio_policy: str,
    allow_opus_in_mp4: bool,
    video_encoder: str,
    video_crf: int,
    video_preset: str,
    pix_fmt: str,
    image_codec: str,
    webp_quality: int,
    webp_lossless: bool,
    avif_crf: int,
    avif_pix_fmt: str,
    faststart: bool,
    overwrite: bool,
    dry_run: bool,
    min_savings: float,
    try_archives: bool,
    comic_min_images: int,
    comic_keep_non_images: bool,
    comic_accept_bigger: bool,
    archive_password: Optional[str],
    out_name_mode: str,
    rel_override: Optional[str] = None,
    out_rel_override: Optional[str] = None,
    src_size_hint: int = 0,
) -> JobResult:
    rel = rel_override or src_local.relative_to(in_root).as_posix()
    src_arg = str(src_local)
    dst_base = out_root / rel

    def apply_out_override(p: Path) -> Path:
        if not out_rel_override:
            return p
        try:
            from pathlib import PurePosixPath
            ov_posix = PurePosixPath(out_rel_override)
            if ov_posix.is_absolute() or ".." in ov_posix.parts:
                return p
            ov = Path(out_rel_override)
            if ov.is_absolute() or ov.drive or ov.anchor or ".." in ov.parts:
                return p
            if ov.suffix.lower() != p.suffix.lower():
                return p
            return out_root / ov
        except Exception:
            return p

    def ensure_local() -> Optional[str]:
        if src_local.exists():
            return None
        return "no local file available"

    def src_size_val() -> int:
        if src_local.exists():
            return size_of(src_local)
        return src_size_hint

    def _ffmpeg_fail_msg(err: str) -> str:
        rc = extract_result_from_ffmpeg_err(err)
        return f"ffmpeg failed ({rc}) -> copied original" if rc else "ffmpeg failed -> copied original"

    def _transcode_or_copy(candidates: List[List[str]], out_final: Path) -> JobResult:
        ok, err, wrote = run_ffmpeg_with_candidates(candidates, out_final, overwrite=overwrite, dry_run=dry_run)
        if not ok:
            if dry_run:
                return JobResult(True, "dry-run", "ffmpeg failed -> would copy", None, rel)
            try:
                if overwrite and out_final.exists():
                    out_final.unlink()
            except Exception:
                pass
            err2 = ensure_local()
            if err2:
                return JobResult(False, "fail", err2)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", _ffmpeg_fail_msg(err), dst_base, rel)

        if dry_run:
            return JobResult(True, "dry-run", "ok", None, out_final.relative_to(out_root).as_posix())
        if not wrote:
            return JobResult(
                True,
                "copy",
                f"output exists -> kept: {out_final}",
                out_final,
                out_final.relative_to(out_root).as_posix(),
            )

        src_sz = src_size_val()
        out_sz = size_of(out_final)
        if src_sz > 0 and out_sz > 0 and out_sz >= src_sz * (1.0 - min_savings):
            try:
                out_final.unlink()
            except Exception:
                pass
            err3 = ensure_local()
            if err3:
                return JobResult(False, "fail", err3)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(
                True,
                "copy",
                f"not enough savings ({fmt_size_change(src_sz, out_sz)}) -> copied original",
                dst_base,
                rel,
            )

        return JobResult(True, "ok", fmt_size_change(src_sz, out_sz), out_final, out_final.relative_to(out_root).as_posix())

    probe = None
    info0 = classify(src_local, None)
    if info0.kind in {"video", "audio", "image"}:
        probe = ffprobe_json(src_arg, dry_run=dry_run)
    info = classify(src_local, probe)

    # subtitle/other -> copy
    if info.kind in {"subtitle", "other"}:
        if dry_run:
            return JobResult(True, "dry-run", f"{info.kind} would copy", None, rel)
        err = ensure_local()
        if err:
            return JobResult(False, "fail", err)
        copy_file_local(src_local, dst_base, overwrite=overwrite)
        return JobResult(True, "copy", f"{info.kind} copied", dst_base, rel)

    # comic/archive
    if info.kind == "comic":
        if not try_archives:
            if dry_run:
                return JobResult(True, "dry-run", "comic try disabled -> would copy", None, rel)
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", "comic copied (try disabled)", dst_base, rel)

        err = ensure_local()
        if err:
            return JobResult(False, "fail", err)
        archiver = find_7z()
        if not archiver:
            return JobResult(False, "fail", "missing 7z/7zz")

        target_ext = ".webp" if image_codec == "webp" else ".avif"
        dst_cbz = (out_root / rel).with_suffix(".cbz")
        dst_cbz = apply_out_name_mode(dst_cbz, src_rel=rel, target_ext=".cbz", out_name_mode=out_name_mode)
        dst_cbz = apply_out_override(dst_cbz)

        # smart-skip
        try:
            skip, reason = comic_smart_skip_already_target(
                archiver,
                src_local,
                password=archive_password,
                min_images=comic_min_images,
                target_ext=target_ext,
                dry_run=dry_run,
            )
        except Exception:
            skip, reason = (False, "skip-check error")

        if skip:
            if dry_run:
                return JobResult(True, "dry-run", f"comic smart-skip: {reason}", None, dst_cbz.relative_to(out_root).as_posix())
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", f"comic smart-skip -> copied original ({reason})", dst_base, rel)

        ensure_parent(dst_cbz)
        ok, msg = process_comic_to_cbz(
            src_local,
            dst_cbz,
            archiver=archiver,
            password=archive_password,
            image_codec=image_codec,
            out_name_mode=out_name_mode,
            webp_quality=webp_quality,
            webp_lossless=webp_lossless,
            avif_crf=avif_crf,
            avif_pix_fmt=avif_pix_fmt,
            detect_min_images=comic_min_images,
            keep_non_images=comic_keep_non_images,
            overwrite=True,
            dry_run=dry_run,
        )
        if not ok and msg.startswith("not a comic archive"):
            if not try_archives:
                if dry_run:
                    return JobResult(True, "dry-run", "archive try disabled -> would copy", None, rel)
                err = ensure_local()
                if err:
                    return JobResult(False, "fail", err)
                copy_file_local(src_local, dst_base, overwrite=overwrite)
                return JobResult(True, "copy", "archive copied (try disabled)", dst_base, rel)
            if dry_run:
                return JobResult(True, "dry-run", msg, None, rel)
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", msg, dst_base, rel)

        if not ok:
            return JobResult(False, "fail", msg)

        if dry_run:
            return JobResult(True, "dry-run", msg, None, dst_cbz.relative_to(out_root).as_posix())

        src_sz = src_size_val()
        out_sz = size_of(dst_cbz)
        if src_sz > 0 and out_sz > 0 and out_sz >= src_sz * (1.0 - min_savings) and not comic_accept_bigger:
            try:
                dst_cbz.unlink()
            except Exception:
                pass
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", f"{msg}; not smaller -> copied original", dst_base, rel)

        return JobResult(True, "ok", f"{msg}; size {src_sz}->{out_sz}", dst_cbz, dst_cbz.relative_to(out_root).as_posix())

    # image already target -> copy
    image_target_ext = ".webp" if image_codec == "webp" else ".avif"
    if info.kind == "image" and src_local.suffix.lower() == image_target_ext:
        if dry_run:
            return JobResult(True, "dry-run", f"already {image_target_ext}", None, rel)
        err = ensure_local()
        if err:
            return JobResult(False, "fail", err)
        copy_file_local(src_local, dst_base, overwrite=overwrite)
        return JobResult(True, "copy", f"already {image_target_ext.lstrip('.')}, copied", dst_base, rel)

    if info.kind == "video":
        has_subs, mp4_sub_ok = detect_subtitle_compat(info.streams)
        container2 = container
        if container2 == "auto":
            container2 = "mp4"
        if container2 == "mp4" and has_subs and not mp4_sub_ok:
            container2 = "mkv"
        out_final = dst_base.with_suffix("." + container2)
        out_final = apply_out_name_mode(out_final, src_rel=rel, target_ext=out_final.suffix, out_name_mode=out_name_mode)
        out_final = apply_out_override(out_final)

        candidates = build_video_candidates(
            src_arg,
            out_final,
            info,
            container_pref=container,
            video_policy=video_policy,
            audio_policy=audio_policy,
            allow_opus_in_mp4=allow_opus_in_mp4,
            video_encoder=video_encoder,
            video_crf=video_crf,
            video_preset=video_preset,
            pix_fmt=pix_fmt,
            faststart=faststart,
        )
        return _transcode_or_copy(candidates, out_final)

    if info.kind == "audio":
        out_final = dst_base.with_suffix(".opus" if audio_policy != "always_copy" else src_local.suffix)
        out_final = apply_out_name_mode(out_final, src_rel=rel, target_ext=out_final.suffix, out_name_mode=out_name_mode)
        out_final = apply_out_override(out_final)
        cmds = build_audio_candidates(src_arg, out_final, info, audio_policy=audio_policy)
        if not cmds:
            if dry_run:
                return JobResult(True, "dry-run", "no audio stream -> copy", None, rel)
            err = ensure_local()
            if err:
                return JobResult(False, "fail", err)
            copy_file_local(src_local, dst_base, overwrite=overwrite)
            return JobResult(True, "copy", "no audio stream -> copied", dst_base, rel)

        return _transcode_or_copy(cmds, out_final)

    if info.kind == "image":
        v = get_main_video_stream(info)
        src_pf = str(v.get("pix_fmt")) if (v and v.get("pix_fmt")) else None
        out_final = dst_base.with_suffix(image_target_ext)
        out_final = apply_out_name_mode(out_final, src_rel=rel, target_ext=out_final.suffix, out_name_mode=out_name_mode)
        out_final = apply_out_override(out_final)

        candidates = build_image_candidates(
            src_arg,
            out_final,
            image_codec=image_codec,
            webp_quality=webp_quality,
            webp_lossless=webp_lossless,
            avif_crf=avif_crf,
            avif_pix_fmt=avif_pix_fmt,
            src_pix_fmt=src_pf,
        )
        return _transcode_or_copy(candidates, out_final)

    if dry_run:
        return JobResult(True, "dry-run", "fallback copy", None, rel)
    err = ensure_local()
    if err:
        return JobResult(False, "fail", err)
    copy_file_local(src_local, dst_base, overwrite=overwrite)
    return JobResult(True, "copy", "fallback copied", dst_base, rel)
