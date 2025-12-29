# Repository Guidelines

## Project Structure & Module Organization

- `shrink_media.py`: single-file Python CLI that enumerates inputs, classifies media, runs `ffmpeg`/`ffprobe`, and optionally reads/writes to OpenList.
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
