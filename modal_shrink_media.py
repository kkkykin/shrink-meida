"""
在 Modal 上运行 shrink_media.py 的包装脚本。

目标：
- 在容器中安装 `ffmpeg`（包含 `ffprobe`）
- 默认关闭漫画/压缩包处理（避免依赖 7z）

用法：
1) 直接编辑本文件里的 `SHRINK_MEDIA_ARGV` 列表
2) 运行：`modal run modal_shrink_media.py`
"""

from __future__ import annotations

import re
import shlex
import subprocess
import sys
from pathlib import Path

import modal

MINUTES = 60
HOURS = 60 * MINUTES

_HERE = Path(__file__).resolve().parent
_LOCAL_SHRINK_MEDIA = _HERE / "shrink_media.py"
_REMOTE_SHRINK_MEDIA = "/root/shrink_media.py"

_SENSITIVE_FLAGS = {
    "--archive-password",
    "--openlist-otp",
    "--openlist-pass",
}


def _mask_argv(argv: list[str]) -> list[str]:
    masked: list[str] = []
    i = 0
    while i < len(argv):
        a = argv[i]

        handled = False
        for flag in _SENSITIVE_FLAGS:
            if a.startswith(f"{flag}="):
                masked.append(f"{flag}=***")
                handled = True
                break
        if handled:
            i += 1
            continue

        if a in _SENSITIVE_FLAGS and i + 1 < len(argv):
            masked.append(a)
            masked.append("***")
            i += 2
            continue

        masked.append(a)
        i += 1
    return masked


def _auto_disable_archives(argv: list[str]) -> list[str]:
    if any(a in argv for a in ("-h", "--help", "--try-archives", "--no-try-archives")):
        return argv
    # shrink_media.py 的 `--try-archives` 默认开启；这里默认关闭，避免在 Modal 里依赖 7z
    return argv + ["--no-try-archives"]


def _argv_flag_value(argv: list[str], flag: str) -> str | None:
    for i, a in enumerate(argv):
        if a == flag and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith(f"{flag}="):
            return a.split("=", 1)[1]
    return None


def _ffmpeg_text(args: list[str]) -> str:
    cp = subprocess.run(
        ["ffmpeg", "-hide_banner", *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    # 某些 ffmpeg build 可能把列表输出到 stderr；合并避免误判。
    return ((cp.stdout or "") + "\n" + (cp.stderr or "")).strip()


def _ffmpeg_nvenc_encoders() -> list[str]:
    txt = _ffmpeg_text(["-encoders"])
    # 例：h264_nvenc / hevc_nvenc / av1_nvenc
    return sorted(set(re.findall(r"\b[a-z0-9_]+_nvenc\b", txt)))


def _print_nvenc_diagnostics(argv: list[str]) -> None:
    nvenc = _ffmpeg_nvenc_encoders()
    if nvenc:
        print("ffmpeg NVENC encoders:", ", ".join(nvenc))
    else:
        print(
            "WARNING: this ffmpeg build has no *_nvenc encoders; "
            "shrink_media.py cannot use NVENC here (it will fall back to CPU encoders)."
        )

    want = _argv_flag_value(argv, "--video-encoder")
    if want == "hevc_nvenc" and "hevc_nvenc" not in nvenc:
        print("WARNING: --video-encoder=hevc_nvenc requested, but hevc_nvenc is not available in ffmpeg -encoders.")


SHRINK_MEDIA_ARGV: list[str] = [
    # 在这里填写 shrink_media.py 的参数（包含 input / -o output / 以及 OpenList 账号密码等）。
    #
    # 注意：不要把真实密码提交到 git；建议用 Modal Secret 注入环境变量，再在这里引用环境变量。
    #
    # 默认值：通过环境变量读取 input/output/openlist，并强制尝试 NVENC（便于确认 L4 是否真的在用）
    "--debug",
    "--video-encoder",
    "hevc_nvenc",
    "--video-policy",
    "always_hevc",
    "--jobs",
    "2",
    "--prefetch",
    "3",
    "--upload-jobs",
    "2",
    "--out-name-mode",
    "collision",
]


image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("ffmpeg")
    .uv_pip_install(
        "httpx>=0.27",
        "openlist",
        "tenacity>=8.2",
        "rich>=13.7",
    )
    .env({"PYTHONUNBUFFERED": "1"})
    .add_local_file(_LOCAL_SHRINK_MEDIA, remote_path=_REMOTE_SHRINK_MEDIA)
)

app = modal.App("shrink-media", image=image)


@app.function(timeout=12 * HOURS, gpu="L4", secrets=[modal.Secret.from_name("shrink-media")])
def run_shrink_media() -> int:
    argv = SHRINK_MEDIA_ARGV.copy()
    if not argv:
        argv = ["--help"]
    argv = _auto_disable_archives(argv)

    if not any(a in argv for a in ("-h", "--help")):
        subprocess.run(["ffmpeg", "-version"], check=True)
        subprocess.run(["ffprobe", "-version"], check=True)
        _print_nvenc_diagnostics(argv)

    cmd = [sys.executable, _REMOTE_SHRINK_MEDIA, *argv]
    print("Running:", shlex.join(_mask_argv(cmd)))
    cp = subprocess.run(cmd, check=False)
    return cp.returncode


@app.local_entrypoint()
def main() -> None:
    raise SystemExit(run_shrink_media.remote())
