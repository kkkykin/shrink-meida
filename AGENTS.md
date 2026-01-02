# Repository Guidelines

## Project Structure & Module Organization

多文件包结构:

```
shrink_media/
├── __init__.py        # 包入口，导出公共 API
├── __main__.py        # CLI 入口 (python -m shrink_media)
├── constants.py       # 常量/扩展名 (VIDEO_EXTS, AUDIO_EXTS, etc.)
├── logging.py         # 日志配置 (configure_logging, log, log_err)
├── utils.py           # 杂项工具 (sha1_hex, fmt_bytes, tail_text, etc.)
├── openlist_client.py # OpenList 辅助 (OpenListClientSync, RemoteEntry)
├── openlist_iter.py   # OpenList 目录遍历 (list_openlist_recursive, iter_openlist_recursive)
├── workitem.py        # WorkItem & 输入枚举 (WorkItem, enumerate_inputs)
├── state.py           # 状态/锁管理 (StateManager, LockManager)
├── probe.py           # ffprobe / encoder cache (probe_media, EncoderCache)
├── classify.py        # 分类逻辑 (classify_media, MediaType)
├── ffmpeg_cmd.py      # ffmpeg 命令构建 (build_ffmpeg_cmd, VideoTranscodeParams)
├── comic.py           # 漫画/压缩包处理 (process_comic, repack_archive)
├── upload.py          # 远端上传 (upload_result, chunked_upload)
├── processor.py       # 单文件本地处理 (process_single_file, transcode_video)
├── pipeline.py        # PipelineContext: 集中管理流水线共享状态
└── cli.py             # CLI / main (parse_args, main)
```

- `_tags`: editor/ctags index (optional; can be regenerated).
- Runtime artifacts (do not commit): output roots may contain `.shrink_media_state.jsonl` and `.shrink_media_locks/` for multi-device coordination, plus temporary `.__tmp__*` files.

## Build, Test, and Development Commands

- `./shrink_media.py --help`: show CLI options (works even if `ffmpeg` is missing).
- `./shrink_media.py <input> -o <output> --dry-run`: preview planned work without writing output files.
- `./shrink_media.py <input> -o <output> -j 4 --overwrite`: run locally with 4 worker jobs, overwriting existing outputs.
- `uv run --script shrink_media.py ...`: explicit runner if you don’t want to rely on the shebang.
- `python -m py_compile shrink_media.py`: quick syntax check.

Notes: real runs require `ffmpeg` and `ffprobe` in `PATH`. Comic/archive handling requires `7z`/`7zz`.

## Coding Style & Naming Conventions

- Python 3.12+ (see the `# /// script` header). Use 4-space indentation, type hints, and `snake_case` names.
- Keep the “single file + section headers” organization; add helpers near the relevant section to preserve readability.
- Prefer the standard library; if you add a dependency, update the dependency list in the script header and keep imports minimal.

## Testing Guidelines

- No dedicated test suite yet. Validate changes by running `--dry-run` plus a small local sample directory and confirming “fallback to copy” behavior when savings are insufficient.
- When touching OpenList logic, test both local↔local and remote↔remote flows (state/locks, retries, and overwrite semantics).

## Commit & Pull Request Guidelines

- Follow Conventional Commits as used in history (e.g., `feat: ...`, `fix(windows): ...`, `refactor: ...`).
- PRs should include: what changed, a minimal repro command, OS + `ffmpeg -version`, and any OpenList setup steps. Avoid committing credentials or large media files.
