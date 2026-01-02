from __future__ import annotations

import posixpath
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from .constants import (
    VIDEO_EXTS,
    AUDIO_EXTS,
    IMAGE_EXTS,
    ANIMATED_IMAGE_EXTS,
    COMIC_EXTS,
    LOCKS_DIR_NAME,
    STATE_DIR_NAME,
)
from .utils import should_ignore_name, looks_like_archive_name
from .openlist_client import OpenListClientSync, FatalAuthError, mtime_to_ns
from .probe import ffprobe_json
from .classify import classify, detect_subtitle_compat

__all__ = [
    "WorkItem",
    "normalize_ext",
    "build_suffixed_target_name",
    "apply_out_name_mode",
    "compute_image_out_name_overrides",
    "compute_output_name_overrides",
    "make_default_out_name_of",
    "iter_local_inputs",
    "iter_remote_inputs",
]


@dataclass
class WorkItem:
    rel: str
    is_remote: bool
    src_local: Optional[Path] = None
    remote_path: Optional[str] = None
    src_size: int = 0
    src_mtime_ns: int = 0
    out_rel_override: Optional[str] = None


def normalize_ext(ext: str) -> str:
    ext = (ext or "").strip().lower()
    if not ext:
        return ""
    return ext if ext.startswith(".") else "." + ext


def build_suffixed_target_name(src_name: str, *, target_ext: str) -> str:
    p = Path(src_name)
    stem = p.stem
    src_ext = p.suffix.lower().lstrip(".") or "src"
    return f"{stem}__{src_ext}{normalize_ext(target_ext)}"


def apply_out_name_mode(p: Path, *, src_rel: str, target_ext: str, out_name_mode: str) -> Path:
    """
    根据 out_name_mode 改写输出路径的"文件名"：
    - collision：默认保持 <stem><target_ext>（靠外层的 compute_output_name_overrides 仅在撞名时用 out_rel_override 修正）
    - suffix：当 target_ext != src_ext 时，输出名改为 <stem>__<src_ext><target_ext>
    """
    if out_name_mode != "suffix":
        return p
    target_ext2 = normalize_ext(target_ext)
    if not target_ext2:
        return p
    src_p = Path(src_rel)
    if src_p.suffix.lower() == target_ext2:
        return p
    return p.with_name(build_suffixed_target_name(src_p.name, target_ext=target_ext2))


def _unique_collision_name(stem: str, src_ext: str, target_ext: str, reserved: set[str]) -> str:
    base = f"{stem}__{src_ext}{target_ext}"
    cand = base
    if cand in reserved:
        for i in range(2, 10000):
            cand2 = f"{stem}__{src_ext}__{i}{target_ext}"
            if cand2 not in reserved:
                cand = cand2
                break
    reserved.add(cand)
    return cand


def compute_image_out_name_overrides(
    names: Sequence[str],
    *,
    image_target_ext: str,
    out_name_mode: str,
) -> Dict[str, str]:
    """
    给同一目录下的图片文件名做"仅在撞名时"的输出名改写。

    规则：
    - collision：1.jpg/1.png -> 1.webp（或 1.avif）
    - suffix：1.jpg/1.png -> 1__jpg.webp / 1__png.webp（或 .avif）
    - 如果同目录内多个源文件会映射到同一个目标名（例如 1.jpg + 1.png -> 1.webp），
      则对这些需要转码的源文件输出名改为：1__jpg.webp / 1__png.webp（必要时再加 __2/__3...）
    - 若同目录已经存在目标扩展（例如已有 1.webp），保留该文件名不改，对其他冲突源改名。

    返回：{src_name: out_name}，仅包含"需要改名"的源文件。
    """
    target_ext = normalize_ext(image_target_ext)
    if not target_ext:
        return {}

    img_names = [n for n in names if Path(n).suffix.lower() in IMAGE_EXTS]
    if len(img_names) <= 1:
        return {}

    def _default_out_name(n: str) -> str:
        ext = Path(n).suffix.lower()
        if ext == target_ext:
            return n
        if out_name_mode == "suffix":
            return build_suffixed_target_name(n, target_ext=target_ext)
        return Path(n).with_suffix(target_ext).name

    # default target name (after conversion/copy semantics for images)
    groups: Dict[str, List[str]] = {}
    for n in img_names:
        out = _default_out_name(n)
        groups.setdefault(out, []).append(n)

    need: set[str] = set()
    for out, srcs in groups.items():
        if len(srcs) <= 1:
            continue
        keep = out if out in srcs else None
        for s in srcs:
            if keep and s == keep:
                continue
            need.add(s)

    if not need:
        return {}

    reserved: set[str] = set()
    for n in sorted(img_names):
        if n in need:
            continue
        reserved.add(_default_out_name(n))

    overrides: Dict[str, str] = {}
    for n in sorted(need):
        p = Path(n)
        overrides[n] = _unique_collision_name(p.stem, p.suffix.lower().lstrip(".") or "src", target_ext, reserved)

    return overrides


def compute_output_name_overrides(
    names: Sequence[str],
    *,
    out_name_of: Callable[[str], Optional[str]],
) -> Dict[str, str]:
    """
    通用"仅在撞名时"的输出名改写（按目录内文件名）。

    - out_name_of(name) 返回该源文件的默认输出文件名（仅文件名，不含路径）；返回 None 表示不参与撞名检测。
    - 若多个源文件会产生相同 out_name，则保留其中一个不改名（优先保留"源文件名就等于 out_name"的那个），
      其余改为：<stem>__<src_ext><target_ext>（必要时追加 __2/__3...）。

    返回：{src_name: out_name}，仅包含"需要改名"的源文件。
    """
    out_by_name: Dict[str, str] = {}
    groups: Dict[str, List[str]] = {}
    for n in names:
        out = out_name_of(n)
        if not out:
            continue
        out_by_name[n] = out
        groups.setdefault(out, []).append(n)

    if not groups:
        return {}

    need: set[str] = set()
    for out, srcs in groups.items():
        if len(srcs) <= 1:
            continue
        keep = out if out in srcs else sorted(srcs)[0]
        for s in srcs:
            if s == keep:
                continue
            need.add(s)

    if not need:
        return {}

    reserved: set[str] = set()
    for s, out in out_by_name.items():
        if s in need:
            continue
        reserved.add(out)

    overrides: Dict[str, str] = {}
    for s in sorted(need):
        out = out_by_name.get(s)
        if not out:
            continue
        target_ext = Path(out).suffix.lower() or ""
        if not target_ext.startswith("."):
            target_ext = "." + target_ext if target_ext else ""
        src_p = Path(s)
        overrides[s] = _unique_collision_name(src_p.stem, src_p.suffix.lower().lstrip(".") or "src", target_ext, reserved)

    return overrides


def make_default_out_name_of(
    *,
    image_target_ext: str,
    audio_target_ext: str,
    video_target_ext_by_name: Dict[str, str],
    out_name_mode: str,
) -> Callable[[str], Optional[str]]:
    def _out_name_of(n: str) -> Optional[str]:
        ext = Path(n).suffix.lower()
        if ext in IMAGE_EXTS:
            if ext == image_target_ext:
                return n
            if out_name_mode == "suffix":
                return build_suffixed_target_name(n, target_ext=image_target_ext)
            return Path(n).with_suffix(image_target_ext).name
        if ext in AUDIO_EXTS and audio_target_ext:
            if ext == audio_target_ext:
                return n
            if out_name_mode == "suffix":
                return build_suffixed_target_name(n, target_ext=audio_target_ext)
            return Path(n).with_suffix(audio_target_ext).name
        if ext in VIDEO_EXTS or ext in ANIMATED_IMAGE_EXTS:
            vext = video_target_ext_by_name.get(n)
            if not vext:
                return None
            if ext == vext:
                return n
            if out_name_mode == "suffix":
                return build_suffixed_target_name(n, target_ext=vext)
            return Path(n).with_suffix(vext).name
        if ext in COMIC_EXTS or looks_like_archive_name(n):
            if ext == ".cbz":
                return n
            if out_name_mode == "suffix":
                return build_suffixed_target_name(n, target_ext=".cbz")
            return Path(n).with_suffix(".cbz").name
        return None

    return _out_name_of


def iter_local_inputs(
    input_path: Path,
    *,
    image_codec: str = "webp",
    container: str = "auto",
    audio_policy: str = "copy_if_lossy",
    out_name_mode: str = "suffix",
) -> Tuple[Path, Iterator[WorkItem]]:
    if input_path.is_file():
        st = input_path.stat()
        root = input_path.parent

        def _one() -> Iterator[WorkItem]:
            yield WorkItem(
                rel=input_path.name,
                is_remote=False,
                src_local=input_path,
                src_size=int(st.st_size),
                src_mtime_ns=int(st.st_mtime_ns),
            )

        return root, _one()

    root = input_path

    def _walk_dir(d: Path) -> Iterator[WorkItem]:
        try:
            children = sorted(d.iterdir(), key=lambda p: p.name)
        except Exception:
            return
        image_target_ext = ".webp" if image_codec == "webp" else ".avif"
        audio_target_ext = ".opus" if audio_policy != "always_copy" else ""
        baseline_video_ext = ".mkv" if container == "mkv" else ".mp4"
        file_names: List[str] = []
        rel_by_name: Dict[str, str] = {}
        file_by_name: Dict[str, Path] = {}
        for p in children:
            name = p.name
            if should_ignore_name(name):
                continue
            if p.is_file():
                rel = p.relative_to(root).as_posix()
                if rel.startswith(LOCKS_DIR_NAME + "/") or rel.startswith(STATE_DIR_NAME + "/"):
                    continue
                file_names.append(name)
                rel_by_name[name] = rel
                file_by_name[name] = p

        video_target_ext_by_name: Dict[str, str] = {}
        video_names = [
            n
            for n in file_names
            if (Path(n).suffix.lower() in VIDEO_EXTS) or (Path(n).suffix.lower() in ANIMATED_IMAGE_EXTS)
        ]
        for n in video_names:
            video_target_ext_by_name[n] = baseline_video_ext

        # container=auto/mp4 时，部分视频可能因为字幕兼容性而从 mp4 自动切到 mkv；仅在"潜在撞名"的 stem 组里探测，避免无谓 ffprobe。
        if out_name_mode == "collision" and baseline_video_ext == ".mp4" and video_names:
            tmp_groups: Dict[str, List[str]] = {}
            for n in video_names:
                ext = Path(n).suffix.lower()
                out = n if ext == ".mp4" else Path(n).with_suffix(".mp4").name
                tmp_groups.setdefault(out, []).append(n)
            need_probe: set[str] = set()
            for _out, srcs in tmp_groups.items():
                if len(srcs) > 1:
                    need_probe.update(srcs)
            for n in sorted(need_probe):
                p = file_by_name.get(n)
                if not p:
                    continue
                probe = ffprobe_json(p, dry_run=False)
                info = classify(p, probe)
                has_subs, mp4_sub_ok = detect_subtitle_compat(info.streams)
                if has_subs and not mp4_sub_ok:
                    video_target_ext_by_name[n] = ".mkv"

        out_overrides = compute_output_name_overrides(
            file_names,
            out_name_of=make_default_out_name_of(
                image_target_ext=image_target_ext,
                audio_target_ext=audio_target_ext,
                video_target_ext_by_name=video_target_ext_by_name,
                out_name_mode=out_name_mode,
            ),
        )
        for p in children:
            name = p.name
            if should_ignore_name(name):
                continue
            if p.is_dir():
                if p.is_symlink():
                    continue
                yield from _walk_dir(p)
                continue
            if not p.is_file():
                continue
            rel = rel_by_name.get(name)
            if not rel:
                continue
            try:
                st = p.stat()
            except Exception:
                continue
            out_rel_override = None
            ov = out_overrides.get(name)
            if ov:
                out_rel_override = Path(rel).with_name(ov).as_posix()
            yield WorkItem(
                rel=rel,
                is_remote=False,
                src_local=p,
                src_size=int(st.st_size),
                src_mtime_ns=int(st.st_mtime_ns),
                out_rel_override=out_rel_override,
            )

    return root, _walk_dir(root)


def iter_remote_inputs(
    client: OpenListClientSync,
    root_path: str,
    *,
    image_codec: str = "webp",
    container: str = "auto",
    audio_policy: str = "copy_if_lossy",
    out_name_mode: str = "suffix",
) -> Tuple[str, Iterator[WorkItem]]:
    root_norm = root_path.rstrip("/") or "/"
    image_target_ext = ".webp" if image_codec == "webp" else ".avif"
    audio_target_ext = ".opus" if audio_policy != "always_copy" else ""
    baseline_video_ext = ".mkv" if container == "mkv" else ".mp4"

    def _gen() -> Iterator[WorkItem]:
        try:
            root_info = client.info(root_norm)
        except FatalAuthError:
            raise
        except Exception as e:
            raise RuntimeError(f"remote info failed: {e}") from e

        if not getattr(root_info, "is_dir", False):
            rel0 = posixpath.basename(root_norm)
            if should_ignore_name(rel0):
                return
            yield WorkItem(
                rel=rel0,
                is_remote=True,
                remote_path=root_norm,
                src_size=int(getattr(root_info, "size", 0) or 0),
                src_mtime_ns=mtime_to_ns(getattr(root_info, "modified", None)),
            )
            return

        def walk(cur: str) -> Iterator[WorkItem]:
            dirs: List[str] = []
            files: List[Tuple[str, str, int, int, str]] = []  # (name, child_path, size, mtime_ns, sign)
            page = 1
            got = 0
            while True:
                res = client.listdir(cur, refresh=True, per_page=100, page=page)
                content = getattr(res, "content", []) or []
                for obj in content:
                    name = str(getattr(obj, "name", "") or "")
                    if not name or should_ignore_name(name):
                        continue
                    child_path = posixpath.join(cur, name)
                    is_dir = bool(getattr(obj, "is_dir", False))
                    if is_dir:
                        if name in {LOCKS_DIR_NAME, STATE_DIR_NAME}:
                            continue
                        dirs.append(child_path)
                        continue
                    rel = posixpath.relpath(child_path, root_norm).replace("\\", "/")
                    if rel.startswith(LOCKS_DIR_NAME + "/") or rel.startswith(STATE_DIR_NAME + "/"):
                        continue
                    files.append(
                        (
                            name,
                            child_path,
                            int(getattr(obj, "size", 0) or 0),
                            mtime_to_ns(getattr(obj, "modified", None)),
                            str(getattr(obj, "sign", "") or ""),
                        )
                    )
                got += len(content)
                total = int(getattr(res, "total", 0) or 0)
                if not content or (total > 0 and got >= total):
                    break
                page += 1

            names = [n for (n, *_rest) in files]
            video_target_ext_by_name: Dict[str, str] = {}
            for n in names:
                ext = Path(n).suffix.lower()
                if ext in VIDEO_EXTS or ext in ANIMATED_IMAGE_EXTS:
                    video_target_ext_by_name[n] = baseline_video_ext

            out_overrides = compute_output_name_overrides(
                names,
                out_name_of=make_default_out_name_of(
                    image_target_ext=image_target_ext,
                    audio_target_ext=audio_target_ext,
                    video_target_ext_by_name=video_target_ext_by_name,
                    out_name_mode=out_name_mode,
                ),
            )

            for name, child_path, size, mtime_ns, sign in sorted(files, key=lambda x: x[0]):
                rel = posixpath.relpath(child_path, root_norm).replace("\\", "/")
                out_rel_override = None
                ov = out_overrides.get(name)
                if ov:
                    out_rel_override = Path(rel).with_name(ov).as_posix()
                yield WorkItem(
                    rel=rel,
                    is_remote=True,
                    remote_path=child_path,
                    src_size=size,
                    src_mtime_ns=mtime_ns,
                    out_rel_override=out_rel_override,
                )

            for d2 in sorted(dirs):
                yield from walk(d2)

        yield from walk(root_norm)

    return root_norm, _gen()
