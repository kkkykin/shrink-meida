# Repository Guidelines

## Project Overview (Target C/S Architecture)

目标：将当前“多设备协同（state+locks）”的模式升级为零信任 C/S 架构：
- **Server（Control Plane）**：持有 OpenList 凭证；负责扫描/建任务、租约(lease)、下发下载/上传能力、落盘与校验、重试与审计。
- **Worker（Execution Plane）**：不持有 OpenList 账号/Token；只通过服务端下发的能力执行 `HTTP GET` 下载、`ffmpeg/7z` 转码、再按能力上传，并回报结果。
- **Engine**：保留现有本地“转码引擎”能力（`process_one_local()` 等），由 worker 调用。
- **Routes（多组 in/out）**：Server 支持多条“输入根 -> 输出根”的路由配置（可选绑定 profile），例如 `/a -> /b`、`/c -> /d`。

关键约束（为可维护性与安全）：
- Server 强制使用 `--out-name-mode=suffix`，避免目录级撞名推理。
- Worker 只能上传到 Server 指定的 **staging** 路径；最终输出路径由 Server 使用 OpenList 凭证完成 **finalize（rename/move + verify）**。
- Staging 位于每条 route 的 `out_root` 下：`.shrink_media_staging/`（每任务独立、不可覆盖）。

## Project Structure & Module Organization (Current + Planned)

当前引擎多文件包结构（已存在）：

```
shrink_media/
├── __init__.py        # 包入口，导出公共 API
├── constants.py       # 常量/扩展名 (VIDEO_EXTS, AUDIO_EXTS, etc.)
├── logging.py         # 日志配置 (configure_logging, log, log_err)
├── utils.py           # 杂项工具 (sha1_hex, fmt_bytes, tail_text, etc.)
├── openlist_client.py # OpenList 辅助 (OpenListClientSync, RemoteEntry)
├── openlist_iter.py   # OpenList 目录遍历 (list_openlist_recursive, iter_openlist_recursive)
├── workitem.py        # WorkItem & 输入枚举/命名策略 (iter_local_inputs/iter_remote_inputs)
├── state.py           # 状态/锁管理（旧的 multi-device 协同模式；C/S 后将逐步降级为 legacy）
├── probe.py           # ffprobe / encoder cache
├── classify.py        # 分类逻辑
├── ffmpeg_cmd.py      # ffmpeg 命令构建
├── comic.py           # 漫画/压缩包处理（依赖 7z/7zz）
├── upload.py          # OpenList 上传逻辑（C/S 后主要供 server finalize 使用）
├── processor.py       # 单文件本地处理（Engine 核心）
├── pipeline.py        # 旧 CLI 流水线（legacy）
└── cli.py             # 旧 CLI 入口（legacy）
```

C/S 目录（已落地）：

```
shrink_media_server/      # Server：任务调度 + OpenList 代理/能力下发 + finalize
  ├── api.py              # FastAPI 路由：register/lease/heartbeat/upload_intent/complete/fail
  ├── db.py               # SQLite 连接与迁移
  ├── models.py           # Task/Worker/Attempt 等模型
  ├── openlist.py         # 仅服务端使用的 OpenList 封装（生成下载直链/获取 direct-upload info/finalize）
  ├── planner.py          # route 规划 + 输出命名与 dst_rel 规划（强制 suffix）
  └── config.py           # 环境变量/配置加载（严禁提交明文凭证）

shrink_media_worker/      # Worker：只调用服务端 API + 本地转码 + HTTP 上传
  ├── worker.py           # 主循环：lease -> download -> transcode -> upload -> complete/fail + heartbeat
  ├── transport.py        # HTTP GET 下载 / 分块 PUT 上传（Content-Range）
  └── caps.py             # 上报能力：ffmpeg encoders、nvenc、7z 等
```

运行产物（请勿提交）：
- legacy 模式：`.shrink_media_state.jsonl`、`.shrink_media_state/`、`.shrink_media_locks/`、`.__tmp__*`
- C/S 模式：每个 out_root 下 `.shrink_media_staging/`（server 控制的 staging 根）

## Build, Test, and Development Commands

Dev environment (prefer `uv`):
- Python deps: use `uv` (`pyproject.toml` + `uv.lock`), and prefer `uv run --with <pkg>` to add extra deps for local/dev.
- Nix (`flake.nix`) is optional and mainly for system binaries (OpenList/ffmpeg/7z); packages may be incomplete, so don’t rely on Nix for Python deps.

- `make openlist`: start local OpenList server (uses `openlist_data/`)
- `make init`: create local `pass.txt` / `routes.json` from examples (both git-ignored)
- `make seed`: generate sample media into `openlist_data/test/*`
- `make smoke`: verify `/d?sign=` download works; upload uses direct-upload if available, otherwise falls back to authenticated upload_proxy

Legacy CLI（仍可用，便于回归验证引擎行为）：
- `python -m shrink_media.cli --help`
- `python -m shrink_media.cli <input> -o <output> --dry-run`

C/S 模式（已实现）：
- Server：`uv run -m shrink_media_server.api`（支持 `--scan-once` 单次扫描）
- Worker：`uv run -m shrink_media_worker.worker --server <url>`（支持 `--once` 单次执行）

Notes: real runs require `ffmpeg` and `ffprobe` in `PATH`. Comic/archive handling requires `7z`/`7zz`.

## Coding Style & Naming Conventions

- Python 3.12+ (see the `# /// script` header). Use 4-space indentation, type hints, and `snake_case` names.
- Prefer clear module boundaries (engine/server/worker). Avoid circular imports; keep OpenList credentials strictly on server side.
- Prefer the standard library; if you add a dependency, update the dependency list in the script header and keep imports minimal.

## Testing Guidelines

短期（仍无测试框架时）：
- Engine：用 `--dry-run` + 小样本目录验证“收益不足回退 copy / ffmpeg 失败回退 copy”。
- Comic：验证 `7z` 存在/不存在、带密码/不带密码、smart-skip 行为。

C/S 模式（已实现，需持续验证）：
- Lease/heartbeat：任务超时回收与重派。
- Zero-trust：worker 不持有 OpenList 凭证；只能上传到 staging，无法写任意路径。
- Finalize：server 将 staging rename/move 到最终输出，size 校验正确，幂等（重复 complete 不产生脏数据）。
- 兜底：direct-upload 不可用时的 proxy 上传/下载路径。
- Routes：多组 in/out 下任务隔离正确（/a->/b 与 /c->/d 互不影响）。

## Commit & Pull Request Guidelines

- Follow Conventional Commits as used in history (e.g., `feat: ...`, `fix(windows): ...`, `refactor: ...`).
- PRs should include: what changed, a minimal repro command, OS + `ffmpeg -version`, and any OpenList setup steps. Avoid committing credentials or large media files.
