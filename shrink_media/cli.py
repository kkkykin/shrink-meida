# -*- coding: utf-8 -*-
"""CLI / main entry point for shrink_media."""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import itertools
import json
import os
import posixpath
import queue
import shlex
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from .logging import configure_logging, log, log_err
from .constants import STATE_DEFAULT_NAME, STATE_DIR_NAME, LOCKS_DIR_NAME
from .utils import is_url
from .openlist_client import OpenListClientSync, FatalAuthError, remote_join
from .workitem import WorkItem
from .state import StateBackendJsonl, StateBackendPerFile, LockBackend, Coordinator
from .probe import require_tools
from .processor import JobResult
from .pipeline import PipelineContext, StageResult, UploadFutureInfo
from .remote import get_device_id, init_remote_client, validate_same_server
from .planning import plan_input, plan_output

__all__ = ["main"]


def _env_nonempty(key: str) -> Optional[str]:
    v = os.environ.get(key)
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def _env_argv() -> List[str]:
    js = _env_nonempty("SHRINK_MEDIA_ARGV_JSON")
    if js:
        try:
            v = json.loads(js)
        except Exception as e:
            log_err(f"ERROR: invalid JSON in SHRINK_MEDIA_ARGV_JSON: {e}")
            sys.exit(2)
        if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
            log_err("ERROR: SHRINK_MEDIA_ARGV_JSON must be a JSON array of strings.")
            sys.exit(2)
        return [str(x) for x in v]

    s = _env_nonempty("SHRINK_MEDIA_ARGS")
    if not s:
        return []
    try:
        return shlex.split(s)
    except ValueError as e:
        log_err(f"ERROR: invalid SHRINK_MEDIA_ARGS (shell-like string): {e}")
        sys.exit(2)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="shrink_media: local/OpenList + multi-device state+locks")
    ap.add_argument("input", type=str, nargs="?", default=None, help="输入（本地路径或 OpenList URL）")
    ap.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="输出（本地路径或 OpenList URL）。缺省：本地输入用同级 <name>__compressed；远端输入用同级 <name>__compressed 目录。",
    )
    ap.add_argument("--inplace", action="store_true", help="原地替换（不推荐远端多设备并发）")
    ap.add_argument(
        "--out-name-mode",
        choices=["suffix", "collision"],
        default="suffix",
        help="输出文件命名：suffix=默认追加 __<src_ext>（例如 1.jpg->1__jpg.webp）；collision=仅在撞名时追加（旧逻辑）。",
    )

    ap.add_argument("--container", choices=["mp4", "mkv", "auto"], default="auto")
    ap.add_argument("--faststart", action="store_true", default=True)
    ap.add_argument("--no-faststart", dest="faststart", action="store_false")

    ap.add_argument("--video-policy", choices=["copy_if_hevc", "always_hevc", "always_copy"], default="copy_if_hevc")
    ap.add_argument("--audio-policy", choices=["copy_if_lossy", "always_opus", "always_copy"], default="copy_if_lossy")
    ap.add_argument("--allow-opus-in-mp4", action="store_true")

    ap.add_argument(
        "--video-encoder",
        choices=["auto", "hevc_nvenc", "libx265", "libx264"],
        default="auto",
        help="auto 优先 hevc_nvenc，失败自动回退 libx265/libx264",
    )
    ap.add_argument("--video-crf", type=int, default=22, help="x265/x264 为 CRF；NVENC 为 QP（越小越清晰）")
    ap.add_argument("--video-preset", type=str, default="slow")
    ap.add_argument("--pix-fmt", type=str, default="auto", help="auto 尽量保留源 pix_fmt；失败回退 yuv420p")

    ap.add_argument("--image-codec", choices=["webp", "avif"], default="webp", help="图片默认 webp；需要 avif 用此切换")
    ap.add_argument("--webp-quality", type=int, default=80)
    ap.add_argument("--webp-lossless", action="store_true")
    ap.add_argument("--image-crf", type=int, default=30, help="avif crf（仅 image-codec=avif）")
    ap.add_argument("--image-pix-fmt", type=str, default="auto", help="avif pix_fmt（auto/显式）")

    ap.add_argument("--min-savings", type=float, default=0.01)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-j", "--jobs", type=int, default=1)

    ap.add_argument("--try-archives", action="store_true", default=True)
    ap.add_argument("--no-try-archives", dest="try_archives", action="store_false")
    ap.add_argument("--comic-detect-min-images", type=int, default=3)
    ap.add_argument("--comic-keep-non-images", action="store_true", default=True)
    ap.add_argument("--comic-drop-non-images", dest="comic_keep_non_images", action="store_false")
    ap.add_argument("--comic-accept-bigger", action="store_true", default=True)
    ap.add_argument("--comic-no-accept-bigger", dest="comic_accept_bigger", action="store_false")

    ap.add_argument("--archive-password", type=str, default=None)
    ap.add_argument("--debug", action="store_true", help="verbose debug logging for pipeline, locks, IO")
    ap.add_argument("--prefetch-debug", action="store_true", help="log detailed prefetch events")
    ap.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="将所有日志写入文件（替代 stdout/stderr）",
    )
    ap.add_argument("--log-append", action="store_true", help="追加写入 --log-file（默认覆盖）")

    # openlist
    ap.add_argument("--openlist-user", type=str, default=None)
    ap.add_argument("--openlist-pass", type=str, default=None)
    ap.add_argument("--openlist-otp", type=str, default=None)
    ap.add_argument("--openlist-timeout", type=int, default=None)
    ap.add_argument("--retries", type=int, default=3, help="OpenList 网络失败重试次数（0 表示不重试）")
    ap.add_argument("--retry-backoff", type=float, default=0.5, help="OpenList 重试退避基准秒数（指数退避）")
    ap.add_argument(
        "--prefetch",
        type=int,
        default=0,
        help="预取远端输入文件的并发数（仅远端输入；0 表示关闭）",
    )
    ap.add_argument(
        "--upload-jobs",
        type=int,
        default=0,
        help="异步上传远端输出的并发数（仅远端输出；0 表示在 worker 内同步上传）",
    )
    ap.add_argument(
        "--upload-idle-retries",
        type=int,
        default=5,
        help="异步上传失败后，在队列较空闲时额外重试次数（仅远端输出 + upload-jobs>0；0 表示关闭）",
    )

    # multi-device
    ap.add_argument("--device-id", type=str, default=None)
    ap.add_argument("--lock-ttl", type=int, default=6 * 3600)
    ap.add_argument("--no-steal-stale-lock", dest="steal_stale_lock", action="store_false", default=True)

    cli_argv = sys.argv[1:] if argv is None else argv
    env_argv = _env_argv()
    args = ap.parse_args(env_argv + cli_argv)

    # 从环境变量补齐常用参数（命令行优先）。
    if args.input is None:
        args.input = _env_nonempty("SHRINK_MEDIA_INPUT")
    if args.output is None:
        args.output = _env_nonempty("SHRINK_MEDIA_OUTPUT")

    if args.openlist_user is None:
        args.openlist_user = _env_nonempty("SHRINK_MEDIA_OPENLIST_USER") or _env_nonempty("OPENLIST_USER")
    if args.openlist_pass is None:
        args.openlist_pass = _env_nonempty("SHRINK_MEDIA_OPENLIST_PASS") or _env_nonempty("OPENLIST_PASS")
    if args.openlist_otp is None:
        args.openlist_otp = _env_nonempty("SHRINK_MEDIA_OPENLIST_OTP") or _env_nonempty("OPENLIST_OTP")
    if args.openlist_timeout is None:
        t = _env_nonempty("SHRINK_MEDIA_OPENLIST_TIMEOUT") or _env_nonempty("OPENLIST_TIMEOUT")
        if t:
            try:
                args.openlist_timeout = int(t)
            except ValueError:
                ap.error("Invalid OPENLIST_TIMEOUT/SHRINK_MEDIA_OPENLIST_TIMEOUT: must be int seconds.")
        else:
            args.openlist_timeout = 60

    if args.archive_password is None:
        args.archive_password = _env_nonempty("SHRINK_MEDIA_ARCHIVE_PASSWORD") or _env_nonempty("ARCHIVE_PASSWORD")

    if args.retries < 0:
        ap.error("--retries must be >= 0")
    if args.retry_backoff < 0:
        ap.error("--retry-backoff must be >= 0")
    if args.upload_idle_retries < 0:
        ap.error("--upload-idle-retries must be >= 0")

    if args.device_id is None:
        args.device_id = _env_nonempty("SHRINK_MEDIA_DEVICE_ID") or _env_nonempty("DEVICE_ID")

    if args.input is None:
        ap.error("Missing input: provide positional `input` or set env `SHRINK_MEDIA_INPUT`.")

    return args


def _run_pipeline(ctx: PipelineContext, items_iter: Iterator[WorkItem]) -> None:
    """运行主处理流水线"""
    ctx.init_upload()
    ctx.init_prefetch()

    scan_thread = threading.Thread(target=ctx.scan_producer, args=(items_iter,), name="scan_inputs")
    scan_thread.start()

    log(f"Input root: {ctx.in_root_display}")
    log(f"Scan: streaming | jobs: {ctx.args.jobs}")
    log(f"Output: {'(inplace)' if ctx.args.inplace else (ctx.out_root_display or '')}")
    log(f"Device: {ctx.device_id} | lock ttl: {ctx.args.lock_ttl}s | steal stale: {'ON' if ctx.args.steal_stale_lock else 'OFF'}")
    log(f"Video encoder: {ctx.args.video_encoder} | Image codec: {ctx.args.image_codec}")
    if ctx.input_is_remote:
        log(f"Remote prefetch: {'ON' if ctx.prefetch_executor is not None else 'OFF'} | prefetch: {ctx.args.prefetch}")
    if ctx.out_is_remote:
        if ctx.upload_executor is not None:
            log(
                f"Remote upload async: ON | upload jobs: {ctx.args.upload_jobs} | idle retries: {ctx.upload_idle_retries}"
                f" (when upload_futs < {ctx.args.upload_jobs}/2)"
            )
        else:
            log(f"Remote upload async: OFF | upload jobs: {ctx.args.upload_jobs}")

    def _sig(_s: int, _f: Any) -> None:
        ctx.interrupted = True
        ctx.stop_event.set()
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    try:
        if ctx.args.jobs <= 1:
            _run_single_threaded(ctx)
        else:
            _run_multi_threaded(ctx)
    except KeyboardInterrupt:
        ctx.interrupted = True
        ctx.stop_event.set()
        ctx.drain_upload_futs(block=False, only_success=True)
        if ctx.remote_client is not None:
            try:
                ctx.remote_client.cancel_pending()
            except Exception:
                pass

    try:
        scan_thread.join()
    except Exception:
        pass

    total, done, proc, enqueued = ctx.get_scan_stats()
    log(f"Scan done: total={total} done={done} processing={proc} enqueued={enqueued}")

    ctx.raise_if_fatal()

    if ctx.scan_error is not None and not ctx.interrupted:
        ctx.failed += 1
        log_err(f"ERROR: input scan failed: {ctx.scan_error}")

    if ctx.interrupted:
        log("\nInterrupted. 已尽力写入已完成任务的 done；processing 会保留并在 TTL 超时后可被接管。")


def _run_single_threaded(ctx: PipelineContext) -> None:
    """单线程处理模式"""
    while True:
        ctx.raise_if_fatal()
        if ctx.stop_event.is_set():
            break
        try:
            it = ctx.work_q.get(timeout=0.2)
        except queue.Empty:
            continue
        if it is None:
            break
        try:
            st = ctx.worker(it)
            if st.upload_dst_path is not None:
                if ctx.upload_executor is None or ctx.upload_slots is None:
                    ctx.finalize(st.it, JobResult(False, "fail", "internal: upload executor not available"))
                    if not ctx.args.dry_run:
                        ctx.coord.locks.release(st.it.rel)
                    continue
                if not ctx.acquire_upload_slot():
                    continue
                try:
                    uf = ctx.upload_executor.submit(ctx.upload_worker, st.it, st.jr, st.upload_dst_path, 0)
                except Exception as e:
                    try:
                        ctx.upload_slots.release()
                    except ValueError:
                        pass
                    ctx.finalize(st.it, JobResult(False, "fail", f"upload submit failed: {e}"))
                    if not ctx.args.dry_run:
                        ctx.coord.locks.release(st.it.rel)
                    continue
                ctx.upload_futs[uf] = UploadFutureInfo(
                    it=st.it,
                    jr=st.jr,
                    dst_path=st.upload_dst_path,
                    idle_retry_attempt=0,
                )
                ctx.drain_upload_futs(block=False, only_success=False)
                ctx.pump_deferred_uploads()
            else:
                ctx.finalize(st.it, st.jr)
        except FatalAuthError:
            raise
        except Exception as e:
            ctx.failed += 1
            ctx.record_fail(it, f"worker exception: {e}")
            log_err(f"[FAIL] worker exception: {e}")
            if not ctx.args.dry_run:
                ctx.safe_append("fail", it, dst_rel=None)

    if ctx.stop_event.is_set():
        ctx.drain_upload_futs(block=False, only_success=True)
    else:
        while ctx.upload_futs or ctx.deferred_uploads:
            ctx.pump_deferred_uploads()
            ctx.drain_upload_futs(block=bool(ctx.upload_futs), only_success=False)
            if not ctx.upload_futs and ctx.deferred_uploads:
                next_at = min(du.next_retry_at for du in ctx.deferred_uploads.values())
                time.sleep(max(0.0, min(0.5, next_at - time.time())))


def _run_multi_threaded(ctx: PipelineContext) -> None:
    """多线程处理模式"""
    with cf.ThreadPoolExecutor(max_workers=ctx.args.jobs) as ex:
        proc_futs: Dict[cf.Future[StageResult], WorkItem] = {}
        work_done = False

        def try_submit_some(limit: int) -> None:
            nonlocal work_done
            for _ in range(limit):
                if ctx.stop_event.is_set() or work_done:
                    return
                try:
                    it0 = ctx.work_q.get_nowait()
                except queue.Empty:
                    return
                if it0 is None:
                    work_done = True
                    return
                proc_futs[ex.submit(ctx.worker, it0)] = it0

        while True:
            ctx.raise_if_fatal()
            ctx.pump_deferred_uploads()
            try_submit_some(max(0, ctx.args.jobs - len(proc_futs)))
            if not (proc_futs or ctx.upload_futs):
                if ctx.stop_event.is_set() or (work_done and not ctx.deferred_uploads):
                    break
                timeout0 = 0.5
                if ctx.deferred_uploads:
                    next_at = min(du.next_retry_at for du in ctx.deferred_uploads.values())
                    due_in = next_at - time.time()
                    if due_in > 0:
                        timeout0 = min(timeout0, due_in)
                try:
                    it0 = ctx.work_q.get(timeout=timeout0)
                except queue.Empty:
                    continue
                if it0 is None:
                    work_done = True
                    continue
                proc_futs[ex.submit(ctx.worker, it0)] = it0
                continue

            all_futs: List[cf.Future[Any]] = list(proc_futs.keys()) + list(ctx.upload_futs.keys())
            timeout = 0.5 if not ctx.stop_event.is_set() else 0.0
            if ctx.deferred_uploads and not ctx.stop_event.is_set():
                next_at = min(du.next_retry_at for du in ctx.deferred_uploads.values())
                due_in = next_at - time.time()
                if due_in > 0:
                    timeout = min(timeout, due_in)
            done, _ = cf.wait(all_futs, timeout=timeout, return_when=cf.FIRST_COMPLETED)
            if not done:
                if ctx.stop_event.is_set():
                    break
                continue
            for fut in done:
                it_ref = proc_futs.pop(fut, None)
                if it_ref is not None:
                    try:
                        st = fut.result()
                    except FatalAuthError:
                        raise
                    except Exception as e:
                        if not ctx.stop_event.is_set():
                            ctx.failed += 1
                            ctx.record_fail(it_ref, f"worker exception: {e}")
                            log_err(f"[FAIL] worker exception: {e}")
                            if not ctx.args.dry_run:
                                ctx.safe_append("fail", it_ref, dst_rel=None)
                            try_submit_some(1)
                        continue

                    if st.upload_dst_path is not None:
                        if not ctx.stop_event.is_set():
                            if ctx.upload_executor is None or ctx.upload_slots is None:
                                ctx.finalize(st.it, JobResult(False, "fail", "internal: upload executor not available"))
                                if not ctx.args.dry_run:
                                    ctx.coord.locks.release(st.it.rel)
                            else:
                                if not ctx.acquire_upload_slot():
                                    continue
                                try:
                                    uf = ctx.upload_executor.submit(ctx.upload_worker, st.it, st.jr, st.upload_dst_path, 0)
                                except Exception as e:
                                    try:
                                        ctx.upload_slots.release()
                                    except ValueError:
                                        pass
                                    ctx.finalize(st.it, JobResult(False, "fail", f"upload submit failed: {e}"))
                                    if not ctx.args.dry_run:
                                        ctx.coord.locks.release(st.it.rel)
                                else:
                                    ctx.upload_futs[uf] = UploadFutureInfo(
                                        it=st.it,
                                        jr=st.jr,
                                        dst_path=st.upload_dst_path,
                                        idle_retry_attempt=0,
                                    )
                    else:
                        if ctx.stop_event.is_set() and not (st.jr.ok and st.jr.action in {"ok", "copy"}):
                            pass
                        else:
                            ctx.finalize(st.it, st.jr)

                    if not ctx.stop_event.is_set():
                        try_submit_some(1)
                    continue

                meta2 = ctx.upload_futs.pop(fut, None)
                if meta2 is None:
                    continue
                try:
                    it2, jr = fut.result()
                except FatalAuthError:
                    raise
                except Exception as e:
                    if not ctx.stop_event.is_set():
                        ctx.failed += 1
                        ctx.record_fail(meta2.it, f"upload exception: {e}")
                        log_err(f"[FAIL] upload exception: {e}")
                        if not ctx.args.dry_run:
                            ctx.safe_append("fail", meta2.it, dst_rel=None)
                    continue
                if jr.action == "defer" and not ctx.stop_event.is_set():
                    ctx.defer_upload(meta2, err_msg=jr.msg)
                    ctx.pump_deferred_uploads()
                    continue
                if ctx.stop_event.is_set() and not (jr.ok and jr.action in {"ok", "copy"}):
                    continue
                ctx.finalize(it2, jr)
                ctx.pump_deferred_uploads()

        if ctx.stop_event.is_set():
            ctx.drain_upload_futs(block=False, only_success=True)
            for fut in list(proc_futs.keys()):
                fut.cancel()
            for fut in list(ctx.upload_futs.keys()):
                fut.cancel()
            ex.shutdown(wait=False, cancel_futures=True)


def main() -> None:
    args = parse_args()
    configure_logging(
        args.log_file,
        append=args.log_append,
        debug=bool(args.debug),
        prefetch_debug=bool(args.prefetch_debug),
    )
    require_tools()

    if args.inplace and args.output:
        log_err("ERROR: --inplace 与 --output 不能同时使用。")
        sys.exit(2)

    device_id = get_device_id(args)
    remote_client, remote_base = init_remote_client(args)
    validate_same_server(args)

    # 输入枚举
    input_plan = plan_input(args, remote_client)

    items_iter = iter(input_plan.items_iter)
    try:
        first_item = next(items_iter)
    except StopIteration:
        log("没有找到可处理的文件。")
        return
    items_iter = itertools.chain([first_item], items_iter)

    # 输出位置
    output_plan, remote_client, remote_base = plan_output(args, input_plan, remote_client, remote_base)

    # state + locks
    if output_plan.is_remote:
        assert remote_client is not None and output_plan.root_remote_path is not None
        state_backend = StateBackendPerFile(
            remote_dir_path=remote_join(output_plan.root_remote_path, STATE_DIR_NAME),
            client=remote_client,
            legacy_jsonl_path=remote_join(output_plan.root_remote_path, STATE_DEFAULT_NAME),
        )
        lock_backend = LockBackend(
            local_dir=None,
            remote_dir_path=remote_join(output_plan.root_remote_path, LOCKS_DIR_NAME),
            client=remote_client,
            device_id=device_id,
            ttl_sec=args.lock_ttl,
            steal_stale=args.steal_stale_lock,
        )
    else:
        assert output_plan.root_local is not None
        state_backend = StateBackendJsonl(local_path=output_plan.root_local / STATE_DEFAULT_NAME, remote_path=None, client=None)
        lock_backend = LockBackend(
            local_dir=output_plan.root_local / LOCKS_DIR_NAME,
            remote_dir_path=None,
            client=None,
            device_id=device_id,
            ttl_sec=args.lock_ttl,
            steal_stale=args.steal_stale_lock,
        )

    coord = Coordinator(state_backend, lock_backend, device_id=device_id, ttl_sec=args.lock_ttl)

    # 创建 PipelineContext
    ctx = PipelineContext(
        args=args,
        device_id=device_id,
        coord=coord,
        remote_client=remote_client,
        input_is_remote=input_plan.is_remote,
        out_is_remote=output_plan.is_remote,
        in_root_local=input_plan.root_local,
        in_root_remote_path=input_plan.root_remote_path,
        out_root_local=output_plan.root_local,
        out_root_remote_path=output_plan.root_remote_path,
        in_root_display=input_plan.display,
        out_root_display=output_plan.display or "",
    )

    try:
        _run_pipeline(ctx, items_iter)
    finally:
        ctx.cleanup()

    ctx.print_summary()
    if ctx.failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except FatalAuthError as e:
        log_err(f"ERROR: {e}")
        sys.exit(2)
