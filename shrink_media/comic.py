from __future__ import annotations

import os
import re
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .logging import _LOGGER, _DEBUG_ENABLED, log
from .constants import IMAGE_EXTS, ANIMATED_IMAGE_EXTS, COMIC_EXTS
from .utils import ensure_parent
from .workitem import build_suffixed_target_name, compute_image_out_name_overrides
from .ffmpeg_cmd import build_image_candidates, run_ffmpeg_with_candidates

__all__ = [
    "sevenzip_password_args",
    "mask_cmd",
    "list_archive_paths",
    "extract_archive",
    "natural_key",
    "is_image_file",
    "collect_files",
    "make_cbz",
    "comic_smart_skip_already_target",
    "process_comic_to_cbz",
]


def sevenzip_password_args(password: Optional[str]) -> List[str]:
    if not password:
        return []
    return [f"-p{password}"]


def mask_cmd(cmd: List[str], password: Optional[str]) -> List[str]:
    if not password:
        return cmd
    out = []
    for a in cmd:
        if a == f"-p{password}":
            out.append("-p***")
        else:
            out.append(a)
    return out


def list_archive_paths(archiver: str, archive: Path, *, password: Optional[str], dry_run: bool) -> Optional[List[str]]:
    cmd = [archiver, "l", "-slt"] + sevenzip_password_args(password) + [str(archive)]
    if dry_run:
        log("[dry-run] " + " ".join(mask_cmd(cmd, password)))
        return []
    if _DEBUG_ENABLED:
        _LOGGER.debug("[7z] list: %s", " ".join(mask_cmd(cmd, password)))
    cp = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if _DEBUG_ENABLED:
        _LOGGER.debug("[7z] list exit=%d archive=%s", cp.returncode, archive)
    if cp.returncode != 0:
        return None
    paths: List[str] = []
    for line in (cp.stdout or "").splitlines():
        line = line.strip()
        if line.startswith("Path = "):
            paths.append(line[len("Path = ") :].strip())
    return [p for p in paths if p and p not in {".", "/"}]


def extract_archive(archiver: str, archive: Path, out_dir: Path, *, password: Optional[str], overwrite: bool, dry_run: bool) -> bool:
    ensure_parent(out_dir / "x")
    ao = "-aoa" if overwrite else "-aos"
    cmd = [archiver, "x", "-y", ao] + sevenzip_password_args(password) + [f"-o{str(out_dir)}", str(archive)]
    if dry_run:
        log("[dry-run] " + " ".join(mask_cmd(cmd, password)))
        return True
    if _DEBUG_ENABLED:
        _LOGGER.debug("[7z] extract: %s", " ".join(mask_cmd(cmd, password)))
    cp = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if _DEBUG_ENABLED:
        _LOGGER.debug("[7z] extract exit=%d archive=%s", cp.returncode, archive)
    return cp.returncode == 0


def natural_key(s: str) -> List[Any]:
    parts = re.split(r"(\\d+)", s)
    out: List[Any] = []
    for p in parts:
        if p.isdigit():
            out.append(int(p))
        else:
            out.append(p.lower())
    return out


def is_image_file(p: Path) -> bool:
    return p.suffix.lower() in IMAGE_EXTS or p.suffix.lower() in ANIMATED_IMAGE_EXTS


def collect_files(root: Path) -> List[Path]:
    return [p for p in root.rglob("*") if p.is_file()]


def make_cbz(zip_path: Path, files: List[Tuple[Path, str]], *, overwrite: bool) -> None:
    ensure_parent(zip_path)
    if zip_path.exists():
        if overwrite:
            zip_path.unlink()
        else:
            raise FileExistsError(str(zip_path))

    try:
        zf = zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9)  # type: ignore
    except TypeError:
        zf = zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED)

    with zf:
        for src, arcname in files:
            zf.write(src, arcname=arcname.replace(os.sep, "/"))


def comic_smart_skip_already_target(
    archiver: str,
    src: Path,
    *,
    password: Optional[str],
    min_images: int,
    target_ext: str,
    dry_run: bool,
) -> Tuple[bool, str]:
    if src.suffix.lower() != ".cbz":
        return False, "not cbz"
    paths = list_archive_paths(archiver, src, password=password, dry_run=dry_run)
    if paths is None:
        return False, "cannot list (password?)"
    imgs = [p for p in paths if Path(p).suffix.lower() in (IMAGE_EXTS | ANIMATED_IMAGE_EXTS)]
    if len(imgs) < min_images:
        return False, f"too few images ({len(imgs)})"
    non = [p for p in imgs if Path(p).suffix.lower() != target_ext]
    if not non:
        return True, f"all images are {target_ext.lstrip('.')}"
    return False, f"has non-{target_ext.lstrip('.')} images ({len(non)})"


def process_comic_to_cbz(
    src: Path,
    out_cbz: Path,
    *,
    archiver: str,
    password: Optional[str],
    image_codec: str,
    out_name_mode: str,
    webp_quality: int,
    webp_lossless: bool,
    avif_crf: int,
    avif_pix_fmt: str,
    detect_min_images: int,
    keep_non_images: bool,
    overwrite: bool,
    dry_run: bool,
) -> Tuple[bool, str]:
    target_ext = ".webp" if image_codec == "webp" else ".avif"

    with tempfile.TemporaryDirectory(prefix="comic_extract_") as td:
        root = Path(td)
        extracted = root / "extracted"
        converted = root / "converted"
        extracted.mkdir(parents=True, exist_ok=True)
        converted.mkdir(parents=True, exist_ok=True)

        if not extract_archive(archiver, src, extracted, password=password, overwrite=True, dry_run=dry_run):
            return False, "7z extract failed (password?)"

        all_files = collect_files(extracted)
        img_files = [p for p in all_files if is_image_file(p)]

        if len(img_files) < detect_min_images and src.suffix.lower() not in COMIC_EXTS:
            return False, f"not a comic archive (images={len(img_files)})"

        files_to_zip: List[Tuple[Path, str]] = []

        if keep_non_images:
            for p in all_files:
                if not is_image_file(p):
                    files_to_zip.append((p, str(p.relative_to(extracted))))

        img_files_sorted = sorted(img_files, key=lambda p: natural_key(str(p.relative_to(extracted))))

        folder_overrides: Dict[Path, Dict[str, str]] = {}
        folder_names: Dict[Path, List[str]] = {}
        for p in img_files_sorted:
            ext = p.suffix.lower()
            if ext in ANIMATED_IMAGE_EXTS:
                continue
            if ext not in IMAGE_EXTS:
                continue
            rel = p.relative_to(extracted)
            folder_names.setdefault(rel.parent, []).append(rel.name)
        for parent, names in folder_names.items():
            ov = compute_image_out_name_overrides(names, image_target_ext=target_ext, out_name_mode=out_name_mode)
            if ov:
                folder_overrides[parent] = ov

        for p in img_files_sorted:
            rel = p.relative_to(extracted)
            ext = p.suffix.lower()

            if ext in ANIMATED_IMAGE_EXTS:
                files_to_zip.append((p, str(rel)))
                continue

            if ext == target_ext:
                files_to_zip.append((p, str(rel)))
                continue

            if out_name_mode == "suffix":
                out_rel = rel.with_name(build_suffixed_target_name(rel.name, target_ext=target_ext))
            else:
                out_rel = rel.with_suffix(target_ext)
            ov = folder_overrides.get(rel.parent)
            if ov:
                out_name = ov.get(rel.name)
                if out_name:
                    out_rel = rel.with_name(out_name)
            out_img = converted / out_rel
            ensure_parent(out_img)

            candidates = build_image_candidates(
                p,
                out_img,
                image_codec=image_codec,
                webp_quality=webp_quality,
                webp_lossless=webp_lossless,
                avif_crf=avif_crf,
                avif_pix_fmt=avif_pix_fmt,
                src_pix_fmt=None,
            )
            ok, _err, _wrote = run_ffmpeg_with_candidates(candidates, out_img, overwrite=True, dry_run=dry_run)
            if not ok:
                files_to_zip.append((p, str(rel)))
                continue

            files_to_zip.append((out_img, str(out_rel)))

        if dry_run:
            log("[dry-run] would create " + str(out_cbz))
            return True, "dry-run ok"

        make_cbz(out_cbz, files_to_zip, overwrite=overwrite)
        return True, f"cbz created (images={len(img_files_sorted)}, fmt={target_ext.lstrip('.')})"
