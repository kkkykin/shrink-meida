from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path


def _require_tool(name: str) -> str:
    p = shutil.which(name)
    if not p:
        raise SystemExit(f"ERROR: missing required tool: {name}")
    return p


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def _ffmpeg(*args: str) -> None:
    _run(["ffmpeg", "-hide_banner", "-nostdin", "-y", *args])


def _ensure_file(path: Path, *, overwrite: bool, gen: callable) -> None:
    if path.exists() and not overwrite:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    gen()


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed OpenList test data into openlist_data/test/*")
    ap.add_argument("--data-dir", type=Path, default=Path("openlist_data"))
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    _require_tool("ffmpeg")

    data_dir: Path = args.data_dir
    root = data_dir / "test"
    from1 = root / "from1"
    to1 = root / "to1"
    from2 = root / "from2"
    to2 = root / "to2"
    for p in (from1, to1, from2, to2):
        p.mkdir(parents=True, exist_ok=True)

    _ensure_file(
        from1 / "bg.png",
        overwrite=args.overwrite,
        gen=lambda: _ffmpeg("-f", "lavfi", "-i", "color=c=skyblue:s=1280x720", "-frames:v", "1", str(from1 / "bg.png")),
    )
    _ensure_file(
        from1 / "photo.jpg",
        overwrite=args.overwrite,
        gen=lambda: _ffmpeg(
            "-f",
            "lavfi",
            "-i",
            "testsrc2=s=1024x576:rate=1",
            "-frames:v",
            "1",
            "-q:v",
            "3",
            str(from1 / "photo.jpg"),
        ),
    )
    _ensure_file(
        from1 / "sample.mp3",
        overwrite=args.overwrite,
        gen=lambda: _ffmpeg(
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=880:sample_rate=44100",
            "-t",
            "4",
            "-c:a",
            "libmp3lame",
            "-q:a",
            "5",
            str(from1 / "sample.mp3"),
        ),
    )
    _ensure_file(
        from1 / "sample.mp4",
        overwrite=args.overwrite,
        gen=lambda: _ffmpeg(
            "-f",
            "lavfi",
            "-i",
            "testsrc2=s=640x360:rate=30",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=44100",
            "-t",
            "4",
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            str(from1 / "sample.mp4"),
        ),
    )

    _ensure_file(
        from2 / "anim.gif",
        overwrite=args.overwrite,
        gen=lambda: _ffmpeg("-f", "lavfi", "-i", "testsrc=s=320x240:rate=10", "-t", "3", str(from2 / "anim.gif")),
    )

    cbz = from2 / "comic.cbz"
    if (not cbz.exists()) or args.overwrite:
        with tempfile.TemporaryDirectory(prefix="seed_cbz_") as td:
            td_p = Path(td)
            img_dir = td_p / "imgs"
            img_dir.mkdir(parents=True, exist_ok=True)
            for i in range(1, 6):
                out = img_dir / f"{i:03d}.jpg"
                _ffmpeg(
                    "-f",
                    "lavfi",
                    "-i",
                    f"testsrc2=s=800x1200:rate=1",
                    "-frames:v",
                    "1",
                    "-q:v",
                    "4",
                    str(out),
                )
            cbz.parent.mkdir(parents=True, exist_ok=True)
            if cbz.exists():
                cbz.unlink()
            with zipfile.ZipFile(cbz, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:  # type: ignore[arg-type]
                for p in sorted(img_dir.iterdir()):
                    zf.write(p, arcname=p.name)

    # touch outputs so routes can exist even if empty
    for p in (to1, to2):
        (p / ".keep").write_text(f"seeded at {time.time()}\n", encoding="utf-8")


if __name__ == "__main__":
    main()

