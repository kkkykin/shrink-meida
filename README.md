# shrink-media

零信任 C/S 架构的媒体压缩系统。Server 持有 OpenList 凭证，Worker 只执行转码任务。

## 架构概述

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│    OpenList     │◄─────►│     Server      │◄─────►│     Worker      │
│  (存储后端)      │       │ (任务调度/凭证)   │       │ (转码执行)       │
└─────────────────┘       └─────────────────┘       └─────────────────┘
                                  │
                                  ▼
                          ┌─────────────────┐
                          │    SQLite DB    │
                          └─────────────────┘
```

- **Server**：扫描 OpenList 生成任务、下发下载/上传能力、finalize 落盘
- **Worker**：从 Server 获取任务、下载文件、调用 ffmpeg/7z 转码、上传到 staging
- **零信任**：Worker 不持有 OpenList 凭证，只能上传到 Server 指定的 staging 路径

## 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install uv
uv sync

# 确保系统工具可用
ffmpeg -version
ffprobe -version
7z        # 可选，用于漫画/压缩包处理
```

### 2. 配置 Server

#### 2.1 YAML 配置文件

创建 `server.yaml`（从示例复制）：

```bash
cp server.example.yaml server.yaml
```

编辑 `server.yaml`（包含 OpenList 凭证、routes、bootstrap tokens 等）：

也可使用环境变量覆盖（优先级高于 YAML）：

```bash
export SERVER_CONFIG_FILE=server.yaml  # 可选；默认会自动探测 ./server.yaml|./server.yml
export OPENLIST_BASE_URL=http://127.0.0.1:15244
export OPENLIST_USER=admin
export OPENLIST_PASS=your_password
export OPENLIST_OTP=123456  # 可选，2FA
export ROUTES_JSON='[{"id":"default","in_root":"/input","out_root":"/output"}]'
```

每条 route 可选设置 `mode`（默认 `compress`）：
- `compress`: 走现有 worker 流水线（下载 → 转码/复制 → 上传到 staging → server finalize）
- `copy`: server 直接调用 OpenList 远端 `copy` 到输出目录（不下载/不上传；worker 不会 lease 该 route 的任务）

#### 2.2 Route Profile（转码参数）

`routes[*].profile` 会原样下发给 worker，用于控制引擎 `ffmpeg/7z` 行为（示例见 `server.example.yaml`）。

常用字段：
- `video_encoder`: `auto`（优先 NVENC，失败回退 CPU）、`auto_gpu`（尽量用 NVENC；仅在 NVENC 不可用时才用 CPU）、或固定 `hevc_nvenc/libx265/libx264`
- `tolerate_corrupt`: `true` 时对“可播放但存在坏帧/坏包”的输入更宽容（可能丢坏帧/有瑕疵，但尽量不因解码错误而失败）

#### 2.3 Worker 认证 Token

```bash
# 方式一：多个环境变量
export WORKER_TOKEN_1=secret-token-abc
export WORKER_TOKEN_2=secret-token-xyz

# 方式二：逗号分隔列表
export WORKER_TOKENS=secret-token-abc,secret-token-xyz
```

可选：为 bootstrap token 配置 scope（限制该 token 注册出来的 worker 只能 lease 指定 `route_id` / `kind` 的任务）：

```bash
export WORKER_TOKENS_SCOPES_JSON='{"secret-token-abc":{"allow_routes":["photos"],"allow_kinds":["image"]}}'
```

也可选：为某个 token 指定 `base_url`，用于下发给该 token 注册出来的 worker 的 OpenList 下载/直传 URL（内网 worker 可避免走公网）：

```bash
export WORKER_TOKENS_SCOPES_JSON='{"secret-token-abc":{"allow_routes":["photos"],"allow_kinds":["image"],"base_url":"http://10.0.0.10:15244"}}'
```

注意：scope 会写入 worker（注册时生成的 `WORKER_TOKEN`）。如果你修改了 scope，需要让 worker 重新注册（清空 `WORKER_TOKEN`，仅设置 `WORKER_BOOTSTRAP_TOKEN` 再启动）。

#### 2.4 其他配置

```bash
export SERVER_DB_URL=sqlite:///shrink_media_server.db  # 默认
export SERVER_HOST=0.0.0.0  # 默认 127.0.0.1
export SERVER_PORT=8000     # 默认
export SERVER_SCAN_ON_STARTUP=1          # 默认 1
export SERVER_SCAN_INTERVAL_SECONDS=300  # 默认 300；设为 0 表示只启动时扫描一次
```

也可对单个 route 设置 `scan_interval_seconds` 覆盖默认扫描间隔；不设置则回退到 `SERVER_SCAN_INTERVAL_SECONDS`。注意：`scan_interval_seconds` 仅影响周期扫描，启动时扫描仍由 `SERVER_SCAN_ON_STARTUP` 对所有 routes 统一控制。

### 3. 启动 Server

```bash
# 正常启动（后台扫描任务）
uv run -m shrink_media_server.api --config server.yaml

# 可选：启动前将 failed/deadletter 统一重置回 queued（并重置 attempts=0）
uv run -m shrink_media_server.api --config server.yaml --requeue-failed-on-startup
# 如果 server 已经在运行，此命令只会更新 DB 并退出（不会启动第二个 server）。

# 调试：只扫描一次生成任务
uv run -m shrink_media_server.api --config server.yaml --scan-once
```

Server 启动后会：
1. 连接 OpenList 并验证凭证
2. 扫描所有 routes 的 `in_root` 目录
3. 为新文件创建任务（幂等，不重复）
4. 监听 Worker 请求

### 4. 配置 Worker

```bash
export WORKER_SERVER_URL=http://localhost:8000
export WORKER_BOOTSTRAP_TOKEN=secret-token-abc  # 用于首次注册
# 或
export WORKER_TOKEN=已注册的worker-token        # 已注册后使用

# 可选配置
export WORKER_NAME=worker-01
export WORKER_LEASE_BATCH_SIZE=1   # 每次 lease 最大获取任务数（会按剩余并发容量自动下调）
export WORKER_HEARTBEAT_INTERVAL=60  # 心跳间隔（秒）
export WORKER_LEASE_POLL_INTERVAL_SECONDS=5  # lease 返回空列表时的轮询间隔（秒，指数退避 base）
export WORKER_LEASE_POLL_MAX_INTERVAL_SECONDS=60  # lease 指数退避最大间隔（秒）

# 并发/流水线配置（跨任务并行：下载/转码/上传可重叠）
export WORKER_MAX_INFLIGHT_TASKS=2     # 同时处理的 task 上限（线程池大小）
export WORKER_DOWNLOAD_CONCURRENCY=2  # 同时下载数（HTTP GET）
export WORKER_TRANSCODE_CONCURRENCY=1 # 同时转码数（ffmpeg/7z，建议 1 起步）
export WORKER_UPLOAD_CONCURRENCY=2    # 同时上传数（HTTP PUT / 分块上传）
```

### 5. 启动 Worker

```bash
# 正常启动（持续运行）
uv run -m shrink_media_worker.worker

# 调试：只 lease 一次，然后等待已领取任务处理完后退出（可能包含多个任务）
uv run -m shrink_media_worker.worker --once

# 调试：输出引擎侧 ffmpeg candidates/重试/失败尾部日志
uv run -m shrink_media_worker.worker --debug
```

Worker 启动后会：
1. 使用 bootstrap token 注册（首次）或验证已有 token
2. 循环获取任务（lease，按并发容量拉取）
3. 多任务并行执行：下载 / 转码 / 上传（跨任务可重叠）
4. Server 收到 complete 后 finalize 到最终路径

## 本地开发

```bash
# 启动本地 OpenList（需要 openlist_data/ 目录）
make openlist

# 初始化配置文件
make init

# 生成测试媒体文件
make seed

# 冒烟测试
make smoke
```

## API 接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/v1/workers/register` | POST | Worker 注册 |
| `/v1/tasks/lease` | POST | 获取任务租约 |
| `/v1/tasks/{id}/heartbeat` | POST | 续租心跳 |
| `/v1/tasks/{id}/upload_intent` | POST | 获取上传能力 |
| `/v1/tasks/{id}/complete` | POST | 报告完成 |
| `/v1/tasks/{id}/fail` | POST | 报告失败 |
| `/v1/tasks/{id}/download_proxy` | GET | 下载代理（兜底） |
| `/v1/tasks/{id}/upload_proxy` | PUT | 上传代理（兜底） |

## 文件产物

运行时会产生以下文件（已加入 .gitignore）：

- `server.yaml` - Server 配置（OpenList 凭证、routes 等）
- `pass.txt` - OpenList 凭证（legacy fallback）
- `routes.json` - 路由配置（legacy fallback）
- `*.db` / `*.sqlite*` - 数据库文件
- `.shrink_media_staging/` - 临时 staging 目录（在 out_root 下）

## Legacy CLI

旧版单机模式仍可使用：

```bash
python -m shrink_media.cli --help
python -m shrink_media.cli /input -o /output --dry-run
```
