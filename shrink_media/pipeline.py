from __future__ import annotations

import argparse
import concurrent.futures as cf
import queue
import random
import shutil
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

from .logging import _LOGGER, _DEBUG_ENABLED, _PREFETCH_DEBUG_ENABLED, log, log_err
from .openlist_client import OpenListClientSync, FatalAuthError, remote_join
from .utils import ensure_parent, size_of, fmt_bytes
from .workitem import WorkItem
from .state import Coordinator
from .processor import JobResult, process_one_local
from .upload import upload_file_remote, _rewrite_out_rel_for_uploaded_path

__all__ = [
    "StageResult",
    "UploadFutureInfo",
    "DeferredUpload",
    "PipelineContext",
]


@dataclass
class StageResult:
    it: WorkItem
    jr: JobResult
    upload_dst_path: Optional[str] = None


@dataclass
class UploadFutureInfo:
    it: WorkItem
    jr: JobResult
    dst_path: str
    idle_retry_attempt: int  # 0=首次上传；1..N=空闲重试次数


@dataclass
class DeferredUpload:
    it: WorkItem
    jr: JobResult
    dst_path: str
    idle_retry_attempt: int  # 下一次尝试的编号（1..N）
    next_retry_at: float
    last_err: str = ""


class PipelineContext:
    """
    集中管理流水线的所有共享状态，减少闭包捕获变量。

    职责：
    - 持有 stop_event / fatal_error 用于中断控制
    - 持有 prefetch / upload 相关的 executor、队列、状态
    - 持有统计计数器
    - 提供 dbg/pdebug/set_fatal/raise_if_fatal 等工具方法

    线程安全约束：
    - 以下字段仅由主线程(调度线程)访问，不加锁：
      - ok, skipped, failed, failed_items, interrupted
    - 以下字段由多线程访问，需加锁：
      - _fatal_error: 通过 _fatal_mu 保护
      - scan_total/done/proc/enqueued: 通过 _scan_mu 保护
      - prefetch_futs/results/claimed: 通过 _prefetch_mu 保护
      - prefetch_candidates: 通过 _prefetch_candidates_mu 保护
      - upload_futs, deferred_uploads: 通过 _upload_mu 保护
    - stop_event: threading.Event，本身线程安全
    - work_q: queue.Queue，本身线程安全
    """

    def __init__(
        self,
        *,
        args: argparse.Namespace,
        device_id: str,
        coord: Coordinator,
        remote_client: Optional[OpenListClientSync],
        input_is_remote: bool,
        out_is_remote: bool,
        in_root_local: Optional[Path],
        in_root_remote_path: Optional[str],
        out_root_local: Optional[Path],
        out_root_remote_path: Optional[str],
        in_root_display: str,
        out_root_display: str,
    ) -> None:
        self.args = args
        self.device_id = device_id
        self.coord = coord
        self.remote_client = remote_client
        self.input_is_remote = input_is_remote
        self.out_is_remote = out_is_remote
        self.in_root_local = in_root_local
        self.in_root_remote_path = in_root_remote_path
        self.out_root_local = out_root_local
        self.out_root_remote_path = out_root_remote_path
        self.in_root_display = in_root_display
        self.out_root_display = out_root_display

        # 中断控制
        self.stop_event = threading.Event()
        self._fatal_mu = threading.Lock()
        self._fatal_error: Optional[FatalAuthError] = None

        # 统计
        self.ok = 0
        self.skipped = 0
        self.failed = 0
        self.failed_items: Dict[str, str] = {}
        self.interrupted = False

        # scan 统计
        self.scan_total = 0
        self.scan_done = 0
        self.scan_proc = 0
        self.scan_enqueued = 0
        self._scan_mu = threading.Lock()
        self.scan_error: Optional[BaseException] = None

        # 工作队列
        self.work_q: queue.Queue[Optional[WorkItem]] = queue.Queue(maxsize=max(1, args.jobs) * 4)

        # 上传相关 (受 _upload_mu 保护)
        self._upload_mu = threading.Lock()
        self.upload_dir: Optional[Path] = None
        self.upload_executor: Optional[cf.ThreadPoolExecutor] = None
        self.upload_slots: Optional[threading.BoundedSemaphore] = None
        self.upload_idle_retries = max(0, int(getattr(args, "upload_idle_retries", 0) or 0))
        self.upload_idle_retry_base_sec = max(30.0, float(args.retry_backoff) * 10.0)
        self.upload_idle_retry_cap_sec = 10.0 * 60.0
        self.deferred_uploads: Dict[str, DeferredUpload] = {}
        self.upload_futs: Dict[cf.Future[Tuple[WorkItem, JobResult]], UploadFutureInfo] = {}

        # 预取相关
        self.prefetch_dir: Optional[Path] = None
        self.prefetch_futs: Dict[str, cf.Future[Tuple[bool, Optional[Path], str]]] = {}
        self.prefetch_results: Dict[str, Tuple[bool, Optional[Path], str]] = {}
        self.prefetch_claimed: set[str] = set()
        self.prefetch_executor: Optional[cf.ThreadPoolExecutor] = None
        self._prefetch_mu = threading.Lock()
        self._prefetch_pump_mu = threading.Lock()
        self.prefetch_candidates: deque[WorkItem] = deque()
        self._prefetch_candidates_mu = threading.Lock()
        self.max_cached = 0
        self.max_candidates = 0

    # ---- 中断控制 ----
    def set_fatal(self, e: FatalAuthError) -> None:
        with self._fatal_mu:
            if self._fatal_error is None:
                self._fatal_error = e
        self.stop_event.set()

    def raise_if_fatal(self) -> None:
        with self._fatal_mu:
            e = self._fatal_error
        if e is not None:
            raise e

    # ---- 日志 ----
    def dbg(self, msg: str) -> None:
        if _DEBUG_ENABLED:
            _LOGGER.debug(msg)

    def pdebug(self, msg: str) -> None:
        if _PREFETCH_DEBUG_ENABLED or _DEBUG_ENABLED:
            _LOGGER.debug(f"[PREFETCH] {msg}")

    # ---- scan 统计 ----
    def inc_scan_total(self) -> None:
        with self._scan_mu:
            self.scan_total += 1

    def inc_scan_done(self) -> None:
        with self._scan_mu:
            self.scan_done += 1

    def inc_scan_proc(self) -> None:
        with self._scan_mu:
            self.scan_proc += 1

    def inc_scan_enqueued(self) -> None:
        with self._scan_mu:
            self.scan_enqueued += 1

    def get_scan_stats(self) -> Tuple[int, int, int, int]:
        with self._scan_mu:
            return self.scan_total, self.scan_done, self.scan_proc, self.scan_enqueued

    # ---- 失败记录 ----
    def record_fail(self, it: WorkItem, msg: str) -> None:
        self.failed_items[it.rel] = msg

    # ---- state 写入 ----
    def safe_append(self, status: str, it: WorkItem, *, dst_rel: Optional[str] = None) -> None:
        try:
            self.coord.append(status, it, dst_rel=dst_rel)
            self.dbg(f"state {status} {it.rel} dst={dst_rel or ''}")
        except FatalAuthError:
            raise
        except Exception as e:
            log_err(f"[WARN] state append failed ({status}) for {it.rel}: {e}")

    # ---- 上传重试相关 ----
    def upload_idle_retry_delay_sec(self, attempt: int) -> float:
        attempt = max(1, int(attempt) or 1)
        delay = self.upload_idle_retry_base_sec * (2 ** max(0, attempt - 1))
        delay = min(delay, self.upload_idle_retry_cap_sec)
        jitter = 0.8 + random.random() * 0.4
        return max(0.0, delay * jitter)

    def is_retryable_upload_error(self, e: Exception) -> bool:
        try:
            if self.remote_client is not None and self.remote_client.is_retryable(e):
                return True
        except Exception:
            pass
        msg = str(e).lower()
        if "upload verify failed" in msg:
            return True
        return False

    def upload_queue_idleish_for_retry(self) -> bool:
        if self.upload_executor is None or self.args.upload_jobs <= 0:
            return False
        with self._upload_mu:
            return len(self.upload_futs) < (self.args.upload_jobs / 2)

    # ---- 预取相关 ----
    def offer_prefetch(self, it: WorkItem) -> None:
        if self.prefetch_executor is None:
            return
        if not it.is_remote or it.remote_path is None:
            return
        with self._prefetch_candidates_mu:
            if len(self.prefetch_candidates) >= self.max_candidates:
                return
            self.prefetch_candidates.append(it)
        self.pump_prefetch()

    def submit_prefetch(self, it: WorkItem) -> None:
        if self.prefetch_executor is None or self.prefetch_dir is None:
            return
        if not it.is_remote or it.remote_path is None:
            return

        prefetch_dir = self.prefetch_dir
        remote_client = self.remote_client

        def _task() -> Tuple[bool, Optional[Path], str]:
            try:
                target = prefetch_dir / it.rel
                ensure_parent(target)
                assert remote_client is not None
                remote_client.download_to(it.remote_path, target)
                return True, target, ""
            except FatalAuthError:
                raise
            except Exception as e:
                return False, None, str(e)

        fut = self.prefetch_executor.submit(_task)
        with self._prefetch_mu:
            self.prefetch_futs[it.rel] = fut
        self.pdebug(f"queued {it.rel}")

        def _done(f: cf.Future) -> None:
            try:
                res = f.result()
            except FatalAuthError as e:
                self.set_fatal(e)
                return
            except Exception as e:
                res = (False, None, str(e))
            with self._prefetch_mu:
                self.prefetch_futs.pop(it.rel, None)
                claimed = it.rel in self.prefetch_claimed
                self.prefetch_claimed.discard(it.rel)
                if claimed:
                    pass
                elif len(self.prefetch_results) < self.max_cached:
                    self.prefetch_results[it.rel] = res
                elif res[0] and res[1] is not None:
                    try:
                        res[1].unlink(missing_ok=True)
                    except Exception:
                        pass
            ok_dl, path_dl, err = res
            if ok_dl:
                self.pdebug(f"done {it.rel} size={path_dl.stat().st_size if path_dl and path_dl.exists() else 'n/a'}")
            else:
                self.pdebug(f"fail {it.rel}: {err}")
            self.pump_prefetch()

        fut.add_done_callback(_done)

    def pump_prefetch(self) -> None:
        if self.prefetch_executor is None:
            return
        if self.stop_event.is_set():
            return
        if not self._prefetch_pump_mu.acquire(blocking=False):
            return
        try:
            while True:
                with self._prefetch_mu:
                    running = len(self.prefetch_futs)
                    cached = len(self.prefetch_results)
                self.pdebug(f"pump running={running} cached={cached}")
                if running >= self.args.prefetch or cached >= self.max_cached:
                    self.pdebug(f"window hold (running={running}, cached={cached})")
                    break
                with self._prefetch_candidates_mu:
                    nxt = self.prefetch_candidates.popleft() if self.prefetch_candidates else None
                if nxt is None:
                    self.pdebug("no more to prefetch")
                    break
                with self._prefetch_mu:
                    if nxt.rel in self.prefetch_futs or nxt.rel in self.prefetch_results:
                        continue
                if not nxt.is_remote or nxt.remote_path is None:
                    continue
                self.pdebug(f"dequeue {nxt.rel}")
                self.submit_prefetch(nxt)
        finally:
            self._prefetch_pump_mu.release()

    def get_prefetch_result(self, rel: str) -> Tuple[Optional[Tuple[bool, Optional[Path], str]], Optional[cf.Future]]:
        """获取预取结果或 future，并标记为 claimed"""
        with self._prefetch_mu:
            res = self.prefetch_results.pop(rel, None)
            fut = self.prefetch_futs.get(rel)
            if res is None and fut is not None:
                self.prefetch_claimed.add(rel)
        return res, fut

    # ---- 资源初始化 ----
    def init_upload(self) -> None:
        if self.out_is_remote and self.args.upload_jobs > 0 and self.remote_client is not None and not self.args.dry_run:
            self.upload_dir = Path(tempfile.mkdtemp(prefix="shrink_upload_"))
            self.dbg(f"upload dir: {self.upload_dir}")
            self.upload_executor = cf.ThreadPoolExecutor(max_workers=self.args.upload_jobs)
            self.upload_slots = threading.BoundedSemaphore(max(1, self.args.upload_jobs) * 2)

    def init_prefetch(self) -> None:
        if self.input_is_remote and self.args.prefetch > 0 and self.remote_client is not None:
            self.prefetch_dir = Path(tempfile.mkdtemp(prefix="shrink_prefetch_"))
            self.dbg(f"prefetch dir: {self.prefetch_dir}")
            self.prefetch_executor = cf.ThreadPoolExecutor(max_workers=self.args.prefetch)
            self.max_cached = max(1, self.args.prefetch) * 2
            self.max_candidates = max(1, self.args.prefetch) * 8
            self.pump_prefetch()

    # ---- 资源清理 ----
    def cleanup(self) -> None:
        if self.prefetch_executor is not None:
            self.prefetch_executor.shutdown(wait=False, cancel_futures=True)
        if self.prefetch_dir is not None:
            try:
                self.dbg(f"cleanup prefetch dir {self.prefetch_dir}")
                shutil.rmtree(self.prefetch_dir, ignore_errors=True)
            except Exception:
                pass

        if self.upload_executor is not None:
            self.upload_executor.shutdown(wait=not self.stop_event.is_set(), cancel_futures=self.stop_event.is_set())
        if self.upload_dir is not None:
            try:
                self.dbg(f"cleanup upload dir {self.upload_dir}")
                shutil.rmtree(self.upload_dir, ignore_errors=True)
            except Exception:
                pass

        if self.remote_client is not None:
            try:
                self.remote_client.close()
            except Exception:
                pass

    def print_summary(self) -> None:
        log(f"\nSummary: OK={self.ok}, SKIP/COPY={self.skipped}, FAIL={self.failed}")
        if self.failed_items:
            log("Failed files:")
            for rel, msg in sorted(self.failed_items.items()):
                log(f"- {rel} | {msg}")

    # ---- finalize ----
    def finalize(self, it: WorkItem, jr: JobResult) -> None:
        if jr.ok and jr.action in {"ok", "copy"}:
            if jr.action == "ok":
                self.ok += 1
            else:
                self.skipped += 1
            log(f"[{jr.action.upper()}] {it.rel} | {jr.msg}")
            if not self.args.dry_run:
                self.safe_append("done", it, dst_rel=jr.out_rel)
            return

        if jr.ok and jr.action == "skip":
            self.skipped += 1
            log(f"[SKIP] {it.rel} | {jr.msg}")
            return

        if jr.ok and jr.action == "dry-run":
            self.skipped += 1
            log(f"[DRY] {it.rel} | {jr.msg}")
            return

        self.failed += 1
        self.record_fail(it, jr.msg)
        log_err(f"[FAIL] {it.rel} | {jr.msg}")
        if not self.args.dry_run:
            self.safe_append("fail", it, dst_rel=jr.out_rel)

    # ---- upload worker ----
    def upload_worker(self, it: WorkItem, jr: JobResult, dst_path: str, idle_retry_attempt: int) -> Tuple[WorkItem, JobResult]:
        assert self.remote_client is not None
        defer_retry = False
        try:
            if jr.out_local is None or jr.out_rel is None:
                return it, JobResult(True, "skip", "no output generated")
            self.dbg(f"upload {jr.out_local} -> {dst_path}")
            uploaded_path = upload_file_remote(
                self.remote_client, jr.out_local, dst_path, overwrite=self.args.overwrite, cancel_event=self.stop_event
            )
            out_rel = _rewrite_out_rel_for_uploaded_path(jr.out_rel, uploaded_path)
            return it, JobResult(True, jr.action, jr.msg, None, out_rel)
        except FatalAuthError:
            raise
        except Exception as e:
            retryable = self.is_retryable_upload_error(e)
            if (
                retryable
                and not self.stop_event.is_set()
                and self.upload_executor is not None
                and self.upload_idle_retries > 0
                and idle_retry_attempt < self.upload_idle_retries
            ):
                defer_retry = True
                return it, JobResult(False, "defer", f"upload failed (defer): {e}", None, jr.out_rel)
            return it, JobResult(False, "fail", f"upload failed: {e}", None, jr.out_rel)
        finally:
            if not defer_retry:
                if jr.out_local is not None:
                    try:
                        jr.out_local.unlink(missing_ok=True)
                    except Exception:
                        pass
                if not self.args.dry_run and not self.stop_event.is_set():
                    self.dbg(f"lock release (upload) {it.rel}")
                    self.coord.locks.release(it.rel)
                if self.upload_slots is not None:
                    try:
                        self.upload_slots.release()
                    except ValueError:
                        pass

    # ---- deferred upload helpers ----
    def finalize_deferred_as_fail(self, meta: UploadFutureInfo, *, msg: str) -> None:
        self.finalize(meta.it, JobResult(False, "fail", msg, None, meta.jr.out_rel))
        if meta.jr.out_local is not None:
            try:
                meta.jr.out_local.unlink(missing_ok=True)
            except Exception:
                pass
        if not self.args.dry_run and not self.stop_event.is_set():
            try:
                self.coord.locks.release(meta.it.rel)
            except FatalAuthError:
                raise
            except Exception:
                pass
        if self.upload_slots is not None:
            try:
                self.upload_slots.release()
            except ValueError:
                pass

    def defer_upload(self, meta: UploadFutureInfo, *, err_msg: str) -> None:
        if meta.jr.out_local is None or not meta.jr.out_local.exists():
            self.finalize_deferred_as_fail(meta, msg="upload deferred but local output missing")
            return
        next_attempt = int(meta.idle_retry_attempt) + 1
        delay = self.upload_idle_retry_delay_sec(next_attempt)
        self.deferred_uploads[meta.it.rel] = DeferredUpload(
            it=meta.it,
            jr=meta.jr,
            dst_path=meta.dst_path,
            idle_retry_attempt=next_attempt,
            next_retry_at=time.time() + delay,
            last_err=err_msg,
        )
        log_err(
            f"[WARN] upload defer {meta.it.rel} | retry {next_attempt}/{self.upload_idle_retries} in {delay:.1f}s | {err_msg}"
        )

    def pump_deferred_uploads(self) -> None:
        if self.stop_event.is_set():
            return
        if self.upload_executor is None:
            return
        if self.upload_idle_retries <= 0:
            return
        if not self.deferred_uploads:
            return
        if not self.upload_queue_idleish_for_retry():
            return
        now = time.time()
        due = [du for du in self.deferred_uploads.values() if du.next_retry_at <= now]
        if not due:
            return
        due.sort(key=lambda x: (x.next_retry_at, x.idle_retry_attempt, x.it.rel))
        for du in due:
            if self.stop_event.is_set():
                break
            if not self.upload_queue_idleish_for_retry():
                break
            if du.jr.out_local is None or not du.jr.out_local.exists():
                meta0 = UploadFutureInfo(
                    it=du.it,
                    jr=du.jr,
                    dst_path=du.dst_path,
                    idle_retry_attempt=max(0, du.idle_retry_attempt - 1),
                )
                self.finalize_deferred_as_fail(meta0, msg="upload deferred but local output missing")
                self.deferred_uploads.pop(du.it.rel, None)
                continue
            self.deferred_uploads.pop(du.it.rel, None)
            self.dbg(f"upload retry#{du.idle_retry_attempt}/{self.upload_idle_retries} {du.it.rel} -> {du.dst_path}")
            try:
                uf = self.upload_executor.submit(self.upload_worker, du.it, du.jr, du.dst_path, du.idle_retry_attempt)
            except Exception as e:
                meta0 = UploadFutureInfo(
                    it=du.it,
                    jr=du.jr,
                    dst_path=du.dst_path,
                    idle_retry_attempt=max(0, du.idle_retry_attempt - 1),
                )
                self.finalize_deferred_as_fail(meta0, msg=f"upload retry submit failed: {e}")
                continue
            self.upload_futs[uf] = UploadFutureInfo(
                it=du.it,
                jr=du.jr,
                dst_path=du.dst_path,
                idle_retry_attempt=du.idle_retry_attempt,
            )

    def acquire_upload_slot(self, *, timeout: float = 0.2) -> bool:
        if self.upload_slots is None:
            return False
        while not self.stop_event.is_set():
            self.pump_deferred_uploads()
            if self.upload_slots.acquire(timeout=timeout):
                return True
        return False

    def drain_upload_futs(self, *, block: bool, only_success: bool) -> None:
        while self.upload_futs:
            timeout = 0.5 if block else 0.0
            done, _ = cf.wait(self.upload_futs.keys(), timeout=timeout, return_when=cf.FIRST_COMPLETED)
            if not done:
                break
            for fut in done:
                meta = self.upload_futs.pop(fut, None)
                try:
                    it2, jr = fut.result()
                except FatalAuthError:
                    raise
                except Exception as e:
                    if only_success:
                        continue
                    self.failed += 1
                    if meta is not None:
                        self.record_fail(meta.it, f"upload exception: {e}")
                    log_err(f"[FAIL] upload exception: {e}")
                    if meta is not None and not self.args.dry_run:
                        self.safe_append("fail", meta.it, dst_rel=None)
                    continue
                if jr.action == "defer" and meta is not None and not only_success:
                    self.defer_upload(meta, err_msg=jr.msg)
                    continue
                if only_success and not (jr.ok and jr.action in {"ok", "copy"}):
                    continue
                self.finalize(it2, jr)

    # ---- worker ----
    def worker(self, it: WorkItem) -> StageResult:
        self.dbg(f"start {it.rel} remote={it.is_remote} size={it.src_size}")
        lock_acquired = False
        defer_release = False
        try:
            if not self.args.dry_run:
                acquired, reason = self.coord.locks.try_acquire(it.rel)
                if not acquired:
                    self.dbg(f"lock skip {it.rel}: {reason}")
                    return StageResult(it, JobResult(True, "skip", f"lock failed: {reason}", None, None))
                self.dbg(f"lock ok {it.rel}")
                lock_acquired = True

                ent = self.coord.get_latest(it.rel, force=True)
                if ent and ent.status == "done" and ent.src_size == it.src_size and ent.src_mtime_ns == it.src_mtime_ns:
                    self.dbg(f"already done elsewhere {it.rel}")
                    return StageResult(it, JobResult(True, "skip", "already done by other device", None, ent.dst_rel))

                self.safe_append("processing", it)

            with tempfile.TemporaryDirectory(prefix="shrink_in_", ignore_cleanup_errors=True) as td_in, tempfile.TemporaryDirectory(
                prefix="shrink_out_", ignore_cleanup_errors=True
            ) as td_out:
                in_tmp_root = Path(td_in)
                out_tmp_root = Path(td_out)

                local_src = in_tmp_root / it.rel
                ensure_parent(local_src)

                if it.is_remote:
                    assert self.remote_client is not None and it.remote_path is not None
                    res, fut = self.get_prefetch_result(it.rel)
                    if res is None and fut is not None:
                        try:
                            res = fut.result()
                        except FatalAuthError:
                            raise
                        except Exception as e:
                            res = (False, None, str(e))
                    self.pump_prefetch()
                    if res is not None:
                        ok_dl, path_dl, err = res
                        if self.args.prefetch_debug:
                            self.pdebug(f"use {'hit' if ok_dl else 'miss'} {it.rel}")
                        if ok_dl and path_dl is not None and path_dl.exists():
                            try:
                                ensure_parent(local_src)
                                self.dbg(f"prefetch copy {path_dl} -> {local_src}")
                                shutil.copy2(path_dl, local_src)
                                if size_of(local_src) != it.src_size:
                                    self.dbg(f"size mismatch; re-download {it.remote_path}")
                                    self.remote_client.download_to(it.remote_path, local_src)
                                try:
                                    path_dl.unlink(missing_ok=True)
                                except Exception:
                                    pass
                            except FatalAuthError:
                                raise
                            except FileNotFoundError:
                                try:
                                    self.dbg(f"prefetch vanished -> download {it.remote_path}")
                                    self.remote_client.download_to(it.remote_path, local_src)
                                except FatalAuthError:
                                    raise
                                except Exception as e:
                                    return StageResult(it, JobResult(False, "fail", f"prefetch vanished+download failed: {e}"))
                            except Exception as e:
                                return StageResult(it, JobResult(False, "fail", f"prefetch copy failed: {e}"))
                        else:
                            try:
                                self.dbg(f"prefetch miss/stale -> download {it.remote_path}")
                                self.remote_client.download_to(it.remote_path, local_src)
                            except FatalAuthError:
                                raise
                            except Exception as e:
                                return StageResult(it, JobResult(False, "fail", f"prefetch+download failed: {err or e}"))
                    else:
                        try:
                            self.dbg(f"download {it.remote_path} -> {local_src}")
                            self.remote_client.download_to(it.remote_path, local_src)
                        except FatalAuthError:
                            raise
                        except Exception as e:
                            return StageResult(it, JobResult(False, "fail", f"download failed: {e}"))
                else:
                    assert it.src_local is not None
                    if not self.args.dry_run:
                        self.dbg(f"local copy {it.src_local} -> {local_src}")
                        shutil.copy2(it.src_local, local_src)

                if self.out_is_remote:
                    local_out_root = self.upload_dir if self.upload_dir is not None else out_tmp_root
                else:
                    local_out_root = self.out_root_local  # type: ignore

                self.dbg(f"process {it.rel} src={local_src} out_root={local_out_root}")
                jr = process_one_local(
                    local_src,
                    in_tmp_root,
                    local_out_root,  # type: ignore
                    container=self.args.container,
                    video_policy=self.args.video_policy,
                    audio_policy=self.args.audio_policy,
                    allow_opus_in_mp4=self.args.allow_opus_in_mp4,
                    video_encoder=self.args.video_encoder,
                    video_crf=self.args.video_crf,
                    video_preset=self.args.video_preset,
                    pix_fmt=self.args.pix_fmt,
                    image_codec=self.args.image_codec,
                    webp_quality=self.args.webp_quality,
                    webp_lossless=self.args.webp_lossless,
                    avif_crf=self.args.image_crf,
                    avif_pix_fmt=self.args.image_pix_fmt,
                    faststart=self.args.faststart,
                    overwrite=self.args.overwrite,
                    dry_run=self.args.dry_run,
                    min_savings=self.args.min_savings,
                    try_archives=self.args.try_archives,
                    comic_min_images=self.args.comic_detect_min_images,
                    comic_keep_non_images=self.args.comic_keep_non_images,
                    comic_accept_bigger=self.args.comic_accept_bigger,
                    archive_password=self.args.archive_password,
                    out_name_mode=self.args.out_name_mode,
                    rel_override=it.rel,
                    out_rel_override=it.out_rel_override,
                    src_size_hint=it.src_size,
                )
                self.dbg(f"process result {it.rel} ok={jr.ok} action={jr.action} msg={jr.msg}")

                if self.args.dry_run:
                    return StageResult(it, jr)

                if not jr.ok:
                    return StageResult(it, jr)

                if self.out_is_remote:
                    assert self.remote_client is not None and self.out_root_remote_path is not None
                    if jr.out_local is None or jr.out_rel is None:
                        return StageResult(it, JobResult(True, "skip", "no output generated"))
                    dst_path = remote_join(self.out_root_remote_path, jr.out_rel)
                    if self.upload_executor is not None and self.upload_dir is not None and self.upload_slots is not None:
                        defer_release = True
                        self.dbg(f"upload enqueue {it.rel} -> {dst_path}")
                        return StageResult(it, jr, upload_dst_path=dst_path)
                    try:
                        self.dbg(f"upload {jr.out_local} -> {dst_path}")
                        uploaded_path = upload_file_remote(
                            self.remote_client, jr.out_local, dst_path, overwrite=self.args.overwrite, cancel_event=self.stop_event
                        )
                    except FatalAuthError:
                        raise
                    except Exception as e:
                        return StageResult(it, JobResult(False, "fail", f"upload failed: {e}", None, jr.out_rel))
                    out_rel2 = _rewrite_out_rel_for_uploaded_path(jr.out_rel, uploaded_path)
                    return StageResult(it, JobResult(True, jr.action, jr.msg, None, out_rel2))

                return StageResult(it, jr)

        finally:
            if lock_acquired and not defer_release and not self.args.dry_run and not self.stop_event.is_set():
                self.dbg(f"lock release {it.rel}")
                self.coord.locks.release(it.rel)

    # ---- scan producer ----
    def scan_producer(self, items_iter: Iterator[WorkItem]) -> None:
        self.dbg("scan thread start")
        try:
            for it in items_iter:
                if self.stop_event.is_set():
                    break
                self.inc_scan_total()
                if self.coord.is_done(it):
                    self.inc_scan_done()
                    continue
                if self.coord.is_processing(it, current_device_id=self.device_id):
                    self.inc_scan_proc()
                    continue
                self.offer_prefetch(it)
                while not self.stop_event.is_set():
                    try:
                        self.work_q.put(it, timeout=0.2)
                        break
                    except queue.Full:
                        continue
                self.inc_scan_enqueued()
        except BaseException as e:
            self.scan_error = e
            if isinstance(e, FatalAuthError):
                self.set_fatal(e)
            self.stop_event.set()
        finally:
            total, done, proc, enqueued = self.get_scan_stats()
            self.dbg(f"scan thread end total={total} done={done} processing={proc} enqueued={enqueued}")
            while True:
                try:
                    self.work_q.put(None, timeout=0.2)
                    break
                except queue.Full:
                    if self.stop_event.is_set():
                        break
