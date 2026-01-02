from __future__ import annotations

import json
import os
import posixpath
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .workitem import WorkItem

from .openlist_client import OpenListClientSync, FatalAuthError, remote_join
from .utils import ensure_parent, sha1_hex

__all__ = [
    "StateEntry",
    "StateBackendJsonl",
    "StateBackendPerFile",
    "LockBackend",
    "Coordinator",
]


@dataclass
class StateEntry:
    ts: float
    status: str
    device_id: str
    src_rel: str
    src_size: int
    src_mtime_ns: int
    dst_rel: Optional[str] = None


class StateBackendJsonl:
    """
    - 本地：单文件 JSONL，append 写入（历史保留）。
    - OpenList：单文件 JSONL 只能通过"读旧文本 + 覆盖上传"模拟 append，存在并发丢行风险（不建议）。
    """

    def __init__(self, *, local_path: Optional[Path], remote_path: Optional[str], client: Optional[OpenListClientSync]) -> None:
        self.local_path = local_path
        self.remote_path = remote_path
        self.client = client
        self._append_mu = threading.Lock()
        if (local_path is None) == (remote_path is None):
            raise ValueError("StateBackendJsonl needs exactly one of local_path or remote_path")

    def read_all(self) -> str:
        if self.local_path is not None:
            if not self.local_path.exists():
                return ""
            return self.local_path.read_text("utf-8", errors="replace")

        assert self.client is not None and self.remote_path is not None
        try:
            return self.client.read_text(self.remote_path)
        except FileNotFoundError:
            return ""
        except FatalAuthError:
            raise
        except Exception:
            return ""

    def append_line(self, line: str) -> None:
        # OpenList 的"读旧文本 + 覆盖上传"不是原子 append：并发写会丢行。
        # 这里至少保证"单进程内"串行 append，降低丢失 done 的概率。
        with self._append_mu:
            if self.local_path is not None:
                ensure_parent(self.local_path)
                with self.local_path.open("a", encoding="utf-8") as f:
                    f.write(line + "\n")
                    f.flush()
                return

            assert self.client is not None and self.remote_path is not None
            # ensure remote parent directory exists
            parent = posixpath.dirname(self.remote_path.rstrip("/")) or "/"
            try:
                self.client.ensure_dir(parent)
            except FatalAuthError:
                raise
            except Exception:
                pass
            # Remote append 采用"读旧文本 + 覆盖上传"。read_all() 的"吞异常返回空串"适合容错读取，
            # 但用于 append 时会导致网络抖动/鉴权异常时把历史 state 当作空串覆盖，破坏多设备协同。
            # 因此这里显式读取并在失败时抛错，让上层 safe_append 记录 WARN 而不是清空 state。
            created_new = False
            try:
                old = self.client.read_text(self.remote_path)
            except FileNotFoundError:
                old = ""
                created_new = True
            except FatalAuthError:
                raise
            except Exception as e:
                raise RuntimeError(f"state read failed: {e}")
            new = old
            if new and not new.endswith("\n"):
                new += "\n"
            new += line + "\n"
            self.client.upload_text(self.remote_path, new, overwrite=True)
            if created_new:
                # OpenList 服务端偶尔不会立即刷新"新创建文件"的状态；触发一次 listdir(refresh=True) 提示它更新。
                try:
                    self.client.listdir(parent, refresh=True, per_page=1)
                except FatalAuthError:
                    raise
                except Exception:
                    pass


class StateBackendPerFile:
    """
    OpenList：每个 src_rel 单独一个 state 文件，避免单文件 JSONL 的"读旧+覆盖"并发丢行。

    Layout:
    - <state_dir>/<prefix>/<sha1(src_rel)>.json
    """

    def __init__(self, *, remote_dir_path: str, client: OpenListClientSync, legacy_jsonl_path: Optional[str] = None) -> None:
        self.remote_dir_path = remote_dir_path.rstrip("/") or "/"
        self.client = client
        self.legacy_jsonl_path = legacy_jsonl_path
        self._ensured_dirs: set[str] = set()
        self._mu = threading.Lock()
        self._legacy_cache: Dict[str, StateEntry] = {}
        self._legacy_cache_at = 0.0
        self._ensure_dir(self.remote_dir_path)

    def _ensure_dir(self, path: str) -> None:
        with self._mu:
            if path in self._ensured_dirs:
                return
        self.client.ensure_dir(path)
        with self._mu:
            self._ensured_dirs.add(path)

    def _path_for_rel(self, rel: str) -> Tuple[str, str]:
        token = sha1_hex(rel)
        prefix = token[:2]
        parent = remote_join(self.remote_dir_path, prefix)
        p = remote_join(self.remote_dir_path, f"{prefix}/{token}.json")
        return parent, p

    def _load_legacy_latest(self, *, force: bool = False) -> Dict[str, StateEntry]:
        if not self.legacy_jsonl_path:
            return {}
        with self._mu:
            if not force and self._legacy_cache and (time.time() - self._legacy_cache_at) < 10:
                return self._legacy_cache
        try:
            txt = self.client.read_text(self.legacy_jsonl_path)
        except FileNotFoundError:
            txt = ""
        except FatalAuthError:
            raise
        except Exception:
            txt = ""
        latest: Dict[str, StateEntry] = {}
        for line in txt.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            rel = obj.get("src_rel")
            st = obj.get("status")
            if not isinstance(rel, str) or not isinstance(st, str):
                continue
            try:
                ent = StateEntry(
                    ts=float(obj.get("ts", 0)),
                    status=st,
                    device_id=str(obj.get("device_id", "")),
                    src_rel=rel,
                    src_size=int(obj.get("src_size", 0)),
                    src_mtime_ns=int(obj.get("src_mtime_ns", 0)),
                    dst_rel=obj.get("dst_rel"),
                )
            except Exception:
                continue
            prev = latest.get(rel)
            if prev is None or ent.ts >= prev.ts:
                latest[rel] = ent
        with self._mu:
            self._legacy_cache = latest
            self._legacy_cache_at = time.time()
        return latest

    def read_latest(self, rel: str) -> Optional[StateEntry]:
        _parent, p = self._path_for_rel(rel)
        try:
            txt = self.client.read_text(p)
        except FileNotFoundError:
            ent = self._load_legacy_latest().get(rel)
            # 懒迁移：把 legacy 单文件 JSONL 的最新记录写入 per-file state，降低后续对 legacy 的依赖。
            if ent is not None and ent.status == "done":
                try:
                    self.write_latest(ent)
                except FatalAuthError:
                    raise
                except Exception:
                    pass
            return ent
        except FatalAuthError:
            raise
        except Exception:
            return None
        try:
            obj = json.loads(txt or "{}")
        except Exception:
            return None
        if not isinstance(obj, dict):
            return None
        st = obj.get("status")
        if not isinstance(st, str):
            return None
        try:
            ent = StateEntry(
                ts=float(obj.get("ts", 0)),
                status=st,
                device_id=str(obj.get("device_id", "")),
                src_rel=str(obj.get("src_rel", rel)),
                src_size=int(obj.get("src_size", 0)),
                src_mtime_ns=int(obj.get("src_mtime_ns", 0)),
                dst_rel=obj.get("dst_rel"),
            )
        except Exception:
            return None
        if ent.src_rel != rel:
            return None
        return ent

    def write_latest(self, ent: StateEntry) -> None:
        parent, p = self._path_for_rel(ent.src_rel)
        self._ensure_dir(parent)
        payload = json.dumps(
            {
                "ts": ent.ts,
                "status": ent.status,
                "device_id": ent.device_id,
                "src_rel": ent.src_rel,
                "src_size": ent.src_size,
                "src_mtime_ns": ent.src_mtime_ns,
                "dst_rel": ent.dst_rel,
            },
            ensure_ascii=False,
        )
        self.client.upload_text(p, payload, overwrite=True)
        # OpenList 服务端偶尔不会立即刷新文件变更；触发一次 listdir(refresh=True) 提示它更新。
        try:
            self.client.listdir(parent, refresh=True, per_page=1)
        except FatalAuthError:
            raise
        except Exception:
            pass


class LockBackend:
    def __init__(
        self,
        *,
        local_dir: Optional[Path],
        remote_dir_path: Optional[str],
        client: Optional[OpenListClientSync],
        device_id: str,
        ttl_sec: int,
        steal_stale: bool,
    ) -> None:
        self.local_dir = local_dir
        self.remote_dir_path = remote_dir_path
        self.client = client
        self.device_id = device_id
        self.ttl_sec = ttl_sec
        self.steal_stale = steal_stale

        if (local_dir is None) == (remote_dir_path is None):
            raise ValueError("LockBackend needs exactly one of local_dir or remote_dir_path")

        if self.local_dir is not None:
            self.local_dir.mkdir(parents=True, exist_ok=True)
        else:
            assert self.client is not None and self.remote_dir_path is not None
            base = self.remote_dir_path.rstrip("/") or "/"
            self.remote_dir_path = base
            self.client.ensure_dir(base)

    def _now(self) -> float:
        return time.time()

    def is_active(self, key: str) -> bool:
        """
        Best-effort 判断锁是否"仍然有效"（存在且未超过 TTL）。
        - 仅用于提升 liveness：如果无法确认（网络/解析错误），返回 False 让任务继续跑。
        """
        token = sha1_hex(key)
        now = self._now()

        if self.local_dir is not None:
            lock_path = self.local_dir / f"{token}.lock"
            if not lock_path.exists():
                return False
            try:
                obj = json.loads(lock_path.read_text("utf-8", errors="replace") or "{}")
                ts = float(obj.get("ts", 0) or 0)
            except Exception:
                return False
            return ts > 0 and (now - ts) <= self.ttl_sec

        assert self.client is not None and self.remote_dir_path is not None
        lock_file = remote_join(self.remote_dir_path, f"{token}.lock")
        try:
            txt = self.client.read_text(lock_file)
            obj = json.loads(txt or "{}")
            ts = float(obj.get("ts", 0) or 0)
        except FileNotFoundError:
            return False
        except FatalAuthError:
            raise
        except Exception:
            return False
        return ts > 0 and (now - ts) <= self.ttl_sec

    def try_acquire(self, key: str) -> Tuple[bool, str]:
        token = sha1_hex(key)
        now = self._now()

        if self.local_dir is not None:
            lock_path = self.local_dir / f"{token}.lock"

            if lock_path.exists():
                try:
                    obj = json.loads(lock_path.read_text("utf-8", errors="replace") or "{}")
                    ts = float(obj.get("ts", 0) or 0)
                    owner = str(obj.get("device_id", ""))
                    stale = now - ts > self.ttl_sec
                    same = owner == self.device_id
                    if same or (self.steal_stale and stale):
                        lock_path.unlink(missing_ok=True)  # type: ignore
                    else:
                        return False, f"locked by {owner or 'unknown'}"
                except Exception:
                    # metadata broken：默认按"活跃锁"处理；只有在允许 steal 时才尝试清理
                    if not self.steal_stale:
                        return False, "local lock present (bad metadata)"
                    try:
                        lock_path.unlink(missing_ok=True)  # type: ignore
                    except Exception:
                        return False, "local lock present (cannot remove)"

            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(json.dumps({"ts": now, "device_id": self.device_id}, ensure_ascii=False))
                return True, "ok"
            except FileExistsError:
                return False, "local lock active"

        assert self.client is not None and self.remote_dir_path is not None
        lock_file = remote_join(self.remote_dir_path, f"{token}.lock")

        stale = True
        same = False
        owner = ""

        try:
            txt = self.client.read_text(lock_file)
            obj = json.loads(txt or "{}")
            ts = float(obj.get("ts", 0) or 0)
            owner = str(obj.get("device_id", ""))
            stale = now - ts > self.ttl_sec if ts > 0 else True
            same = owner == self.device_id
            if not (same or (self.steal_stale and stale)):
                return False, f"locked by {owner or 'unknown'}"
        except FileNotFoundError:
            stale = True
        except FatalAuthError:
            raise
        except Exception as e:
            # 读锁失败：为了不断跑（默认 steal_stale=ON）可以继续尝试接管；
            # 但当明确关闭 steal 时，应该把它当作"有人在跑/未知状态"来避免误抢。
            if not self.steal_stale:
                return False, f"lock read failed: {e}"
            stale = True

        try:
            lock_token = f"{now}:{self.device_id}"
            self.client.upload_text(
                lock_file,
                json.dumps({"ts": now, "device_id": self.device_id, "token": lock_token}, ensure_ascii=False),
                overwrite=True,
            )
            # OpenList 服务端偶尔不会立即刷新"新创建/更新 lock 文件"的状态；触发一次 listdir(refresh=True) 提示它更新。
            try:
                self.client.listdir(self.remote_dir_path, refresh=True, per_page=1)
            except FatalAuthError:
                raise
            except Exception:
                pass
            # read-back verification: 确保写入的是我们自己的锁，避免并发覆盖
            try:
                txt2 = self.client.read_text(lock_file)
                obj2 = json.loads(txt2 or "{}")
                token2 = str(obj2.get("token", ""))
                if token2 != lock_token:
                    return False, f"lock verify failed: token mismatch (expected={lock_token}, got={token2})"
            except FatalAuthError:
                raise
            except Exception as e:
                return False, f"lock verify failed: {e}"
            return True, "stolen" if (stale and owner) else "ok"
        except FatalAuthError:
            raise
        except Exception as e:
            return False, f"lock write failed: {e}"

    def release(self, key: str) -> None:
        token = sha1_hex(key)
        if self.local_dir is not None:
            p = self.local_dir / f"{token}.lock"
            try:
                p.unlink()
            except Exception:
                pass
            return

        assert self.client is not None and self.remote_dir_path is not None
        lock_file = remote_join(self.remote_dir_path, f"{token}.lock")
        try:
            self.client.remove(lock_file)
        except FatalAuthError:
            raise
        except Exception:
            pass
        # 删除锁后也刷新一下目录，避免服务端缓存导致其他设备仍"看见"旧锁。
        try:
            self.client.listdir(self.remote_dir_path, refresh=True, per_page=1)
        except FatalAuthError:
            raise
        except Exception:
            pass


class Coordinator:
    def __init__(self, state: StateBackendJsonl | StateBackendPerFile, locks: LockBackend, *, device_id: str, ttl_sec: int) -> None:
        self.state = state
        self.locks = locks
        self.device_id = device_id
        self.ttl_sec = ttl_sec
        self._cache_map: Dict[str, StateEntry] = {}
        self._cache_map_at = 0.0
        self._cache_one: Dict[str, Optional[StateEntry]] = {}
        self._cache_one_at: Dict[str, float] = {}
        self._mu = threading.Lock()

    def _now(self) -> float:
        return time.time()

    def _load_latest_map(self, *, force: bool = False) -> Dict[str, StateEntry]:
        if not isinstance(self.state, StateBackendJsonl):
            return {}
        with self._mu:
            if not force and self._cache_map and (self._now() - self._cache_map_at < 10):
                return self._cache_map

            txt = self.state.read_all()
            latest: Dict[str, StateEntry] = {}
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                rel = obj.get("src_rel")
                st = obj.get("status")
                if not isinstance(rel, str) or not isinstance(st, str):
                    continue
                try:
                    ent = StateEntry(
                        ts=float(obj.get("ts", 0)),
                        status=st,
                        device_id=str(obj.get("device_id", "")),
                        src_rel=rel,
                        src_size=int(obj.get("src_size", 0)),
                        src_mtime_ns=int(obj.get("src_mtime_ns", 0)),
                        dst_rel=obj.get("dst_rel"),
                    )
                except Exception:
                    continue
                prev = latest.get(rel)
                if prev is None or ent.ts >= prev.ts:
                    latest[rel] = ent

            self._cache_map = latest
            self._cache_map_at = self._now()
            return latest

    def get_latest(self, rel: str, *, force: bool = False) -> Optional[StateEntry]:
        if isinstance(self.state, StateBackendJsonl):
            return self._load_latest_map(force=force).get(rel)

        now = self._now()
        with self._mu:
            if not force:
                at = self._cache_one_at.get(rel)
                if at and (now - at) < 10:
                    return self._cache_one.get(rel)
        ent = self.state.read_latest(rel)
        with self._mu:
            self._cache_one[rel] = ent
            self._cache_one_at[rel] = now
        return ent

    def is_done(self, it: WorkItem) -> bool:
        ent = self.get_latest(it.rel)
        if not ent:
            return False
        if ent.status != "done":
            return False
        return ent.src_size == it.src_size and ent.src_mtime_ns == it.src_mtime_ns

    def is_processing(self, it: WorkItem, *, current_device_id: Optional[str] = None) -> bool:
        ent = self.get_latest(it.rel)
        if not ent:
            return False
        if ent.status != "processing":
            return False
        if (self._now() - ent.ts) > self.ttl_sec:
            return False
        # 同设备中断/重启：不要因为旧的 processing 阻塞自己继续跑
        if current_device_id and ent.device_id == current_device_id:
            return False
        # processing 仅作为提示；真实"是否有人在跑"以 lock 为准（读失败则当作不活跃，优先不断跑）
        try:
            return self.locks.is_active(it.rel)
        except FatalAuthError:
            raise
        except Exception:
            return False

    def append(self, status: str, it: WorkItem, *, dst_rel: Optional[str] = None) -> None:
        ts = self._now()
        ent = StateEntry(
            ts=ts,
            status=status,
            device_id=self.device_id,
            src_rel=it.rel,
            src_size=it.src_size,
            src_mtime_ns=it.src_mtime_ns,
            dst_rel=dst_rel,
        )
        if isinstance(self.state, StateBackendJsonl):
            rec = {
                "ts": ts,
                "status": status,
                "device_id": self.device_id,
                "src_rel": it.rel,
                "src_size": it.src_size,
                "src_mtime_ns": it.src_mtime_ns,
                "dst_rel": dst_rel,
            }
            self.state.append_line(json.dumps(rec, ensure_ascii=False))
        else:
            self.state.write_latest(ent)
        with self._mu:
            if isinstance(self.state, StateBackendJsonl):
                self._cache_map[it.rel] = ent
                self._cache_map_at = self._now()
            else:
                self._cache_one[it.rel] = ent
                self._cache_one_at[it.rel] = self._now()
