# shrink-media C/S (Zero-Trust) 详细 TODO

目标：在保持现有本地转码引擎（`shrink_media/processor.py`）可复用的前提下，新增 **Server/Worker** 两个组件，完成“服务端分配任务 + 下发下载/上传能力 + 客户端自动下载转码上传 + 服务端 finalize 落盘”的零信任闭环。

---

## 0. 架构决策（先定死，避免反复返工）

- [ ] **Routes（多组 in/out）**
  - Server 以路由列表驱动任务生成：每条 route 定义 `in_root` 与 `out_root`（可选 profile）。
  - 任务必须带 `route_id`，避免不同 route 的任务/输出互相污染。
- [ ] **强制输出命名模式：`suffix`**
  - Server 统一规划 `dst_rel`，避免目录级撞名推理与 ffprobe 依赖。
  - 输出名规则：`<stem>__<src_ext><target_ext>`（与现有 `workitem.build_suffixed_target_name()` 对齐）。
- [ ] **Staging + Server Finalize**
  - Worker 只允许上传到 `route.out_root/.shrink_media_staging/<task_id>/<nonce>/blob`
  - Server 拿到 complete 后，使用 OpenList 凭证执行：
    - 校验 staging 文件存在且 size 匹配
    - `rename/move` 到最终 `dst_path`
    - 再次校验 size
    - 写 DB 状态、产生日志/审计记录
- [ ] **租约(lease)模型**
  - 任务被领取后进入 `leased`，带 `lease_expires_at`
  - Worker `heartbeat` 续租；超时自动回收重派
  - finalize/complete/fail 必须是幂等接口（重复调用不制造脏状态）
- [ ] **下载/上传能力下发策略**
  - 首选：下发 OpenList `/d?...sign=...` 下载直链（Worker 仅 HTTP GET）
  - 首选：下发 OpenList direct-upload `upload_url`（Worker 分块 PUT，`Content-Range`）
  - 兜底：Server 提供 download/upload proxy（带宽成本高，但最稳）

---

## 1. 仓库结构与打包（可维护性优先）

- [ ] 新增 packages（建议 top-level）：
  - [ ] `shrink_media_server/`（FastAPI + DB + OpenList 管理 + finalize）
  - [ ] `shrink_media_worker/`（Worker loop + HTTP transport + capability 上报）
- [ ] `pyproject.toml` 增加 scripts：
  - [ ] `shrink-media-server = shrink_media_server.api:main`
  - [ ] `shrink-media-worker = shrink_media_worker.worker:main`
- [ ] 新增配置加载约定（全部通过 env/secret 注入）：
  - `SERVER_DB_URL`（默认 sqlite）
  - `OPENLIST_BASE_URL/OPENLIST_USER/OPENLIST_PASS/OPENLIST_OTP`
  - `ROUTES_JSON`（多组 in/out：`[{id,in_root,out_root,profile?}, ...]`）
  - `WORKER_TOKEN_*`（worker auth）
- [ ] `.gitignore` 增加：
  - `.env`、`*.db`、`*.sqlite*`、`__pycache__/`、server/worker runtime logs

---

## 2. Server：数据模型（DB）与状态机

### 2.1 表结构（最小可用）
- [ ] `workers`
  - `id, name, token_hash, caps_json, last_seen_at, created_at`
- [ ] `tasks`
  - `id (uuid/ulid)`
  - `route_id`
  - `src_path`（OpenList 绝对 path）
  - `src_rel`（相对 input root；用于输出规划）
  - `src_size, src_mtime_ns`
  - `status`：`queued|leased|uploaded_to_staging|finalized|failed|deadletter`
  - `lease_worker_id, lease_expires_at`
  - `attempts, max_attempts`
  - `profile_json`（转码参数快照）
  - `staging_path`（最近一次 staging）
  - `final_path`（最终输出路径）
  - `action`：`ok|copy|skip`（对齐引擎语义）
  - `out_size`
  - `last_error`
  - `created_at, updated_at`
- [ ] `attempts`（审计/排障强烈建议）
  - `task_id, worker_id, started_at, finished_at, ok, action, err, metrics_json`

### 2.2 任务幂等键（避免重复建任务）
- [ ] 以 `(route_id, src_path, src_size, src_mtime_ns)` 作为“同一版本文件”的幂等键
- [ ] 新版本文件（mtime/size 变化）允许生成新 task 或复用原 task 进入重跑

---

## 3. Server：OpenList 交互层（必须只在服务端）

- [ ] 封装 `OpenListManager`（仅 server 使用）
  - [ ] `get_download_url(path) -> {url, expires_at?}`（基于 `info.sign` 拼 `/d`）
  - [ ] `get_direct_upload_info(dst_path, size) -> {upload_url, method, chunk_size, headers?}`（调用 `/api/fs/get_direct_upload_info`）
  - [ ] `ensure_dir(path)`
  - [ ] `info(path)` / `listdir(refresh=True)`（用于 verify）
  - [ ] `rename(src, dst)` / `remove(path)`（用于 finalize）
- [ ] 实现 **finalize**：
  - [ ] 校验 staging size == expected
  - [ ] 计算最终 `dst_path`（`dst_path = route.out_root + dst_rel`）
  - [ ] overwrite/冲突策略：
    - 默认：若目标存在且 size 相同 → 视为已完成（幂等）
    - 若目标存在且 size 不同 → 生成 `__2/__3` 或进入 fail（策略需配置化）
  - [ ] rename staging → final
  - [ ] 再次 verify final size

---

## 4. Server：API（FastAPI）详细接口

### 4.1 鉴权
- [ ] Worker token：`Authorization: Bearer <token>`
- [ ] token 存储用 hash（不可明文落库）
- [ ] 简单的 rate limit / IP allowlist（可选但建议）

### 4.2 接口清单
- [ ] `POST /v1/workers/register`
  - req: `{name, caps}`
  - resp: `{worker_id, worker_token}`
- [ ] `POST /v1/tasks/lease`
  - req: `{worker_id, n}`
  - resp: `[{task, lease_expires_at, download, profile}]`
- [ ] `POST /v1/tasks/{id}/heartbeat`
  - req: `{worker_id}`
  - resp: `{lease_expires_at}`
- [ ] `POST /v1/tasks/{id}/upload_intent`
  - req: `{worker_id, out_size, out_ext, action}`
  - resp: `{staging_path, upload:{url, method, chunk_size, headers?, expires_at}}`
- [ ] `POST /v1/tasks/{id}/complete`
  - req: `{worker_id, staging_path, action, out_size, metrics}`
  - server: finalize + 更新 DB
- [ ] `POST /v1/tasks/{id}/fail`
  - req: `{worker_id, err, retryable}`
  - server: attempts++；决定回到 queued / deadletter

### 4.3 Proxy 兜底（建议预留）
- [ ] `GET /v1/tasks/{id}/download_proxy`（server 代下发/转发）
- [ ] `PUT /v1/tasks/{id}/upload_proxy`（worker 把结果传给 server；server 再写 OpenList）

---

## 5. Worker：执行引擎封装（复用现有 shrink_media）

- [ ] 把现有引擎整理成“可被 worker 调用的纯函数入口”
  - 输入：`local_src_path + profile`
  - 输出：`{action, out_local_path, out_ext, out_size, logs/metrics}`
  - 强制 `out-name-mode=suffix`（由 server 决策，worker 只遵从）
- [ ] Worker 主循环
  - [ ] register（一次）/ 加载 token
  - [ ] lease N 个任务
  - [ ] 对每个任务：
    - 下载（GET `download.url`）到 tmp
    - 调用 engine 转码（含 comic/7z）
    - 计算 out_size/out_ext/action
    - upload_intent 获取 staging upload capability
    - 分块 PUT 上传（`Content-Range`），支持断点/重试
    - complete（附带 metrics）
  - [ ] heartbeat 线程/协程定时续租（长任务必需）
  - [ ] SIGINT/SIGTERM：停止领取新任务，尽力完成当前任务或标记 fail

---

## 6. 兼容与迁移（把旧 CLI 变成“Engine 验证工具”）

- [ ] 保留 `python -m shrink_media.cli` 作为 legacy 回归工具
- [ ] 新增 `shrink-media-worker --once`（只跑一次 lease，便于调试）
- [ ] 新增 `shrink-media-server --scan-once`（扫描一次生成任务，便于调试）

---

## 7. 运维与可观测性（上线前必须有）

- [ ] Server 日志：每个 task_id 一条结构化日志（status/attempt/latency）
- [ ] Worker 日志：下载/转码/上传耗时、ffmpeg rc、输出大小变化
- [ ] 指标（可选但建议）：任务吞吐、失败率、平均耗时、按 codec 分类
- [ ] 管理接口（可选）：查看队列、死信重放、按路径过滤重跑

---

## 8. 验收标准（Definition of Done）

- [ ] Worker 完全不需要 OpenList 凭证，且无法写入非 staging 的 OpenList 路径
- [ ] 同一批输入可被多个 worker 并行处理，server lease 可回收、可重派
- [ ] 输出路径命名稳定、可预测；重复 complete/重启不会制造重复文件或脏状态
- [ ] 断网/服务端重启/worker 崩溃后可自动恢复
- [ ] 多组 routes（例如 /a->/b 与 /c->/d）可同时运行，且任务/输出隔离正确
- [ ] 至少覆盖：video/audio/image/comic 四类任务的端到端跑通
