import fcntl
import json
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from config.settings import DATA_DIR


SINGLE_INSTANCE_SYNC_TASKS = (
    "adjust_factors",
    "benchmark_index_kline",
    "corporate_actions",
    "daily_kline",
    "financial",
    "index_list",
    "market_overview_refresh",
    "scorecard_refresh",
    "stock_list",
    "stock_profiles",
    "trading_calendar",
)

LOCK_DIR = DATA_DIR / "task_locks"


class TaskAlreadyRunningError(RuntimeError):
    def __init__(self, task_name: str, metadata: dict | None = None):
        self.task_name = task_name
        self.metadata = metadata or {}
        super().__init__(f"{task_name} is already running")


class TaskLockHandle:
    def __init__(self, task_name: str, handle, metadata: dict):
        self.task_name = task_name
        self._handle = handle
        self.metadata = metadata

    def update(self, **updates):
        if not updates:
            return
        self.metadata.update(updates)
        self.metadata["updated_at"] = datetime.now().isoformat(timespec="seconds")
        self._handle.seek(0)
        self._handle.truncate()
        json.dump(self.metadata, self._handle, ensure_ascii=False)
        self._handle.flush()
        os.fsync(self._handle.fileno())


def _lock_path(task_name: str) -> Path:
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    return LOCK_DIR / f"{task_name}.lock"


def _read_lock_payload(handle) -> dict:
    handle.seek(0)
    raw = handle.read().strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def get_task_lock_status(task_name: str) -> dict:
    path = _lock_path(task_name)
    with open(path, "a+", encoding="utf-8") as handle:
        payload = _read_lock_payload(handle)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {
                "task_name": task_name,
                "is_running": True,
                "metadata": payload,
            }

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return {
            "task_name": task_name,
            "is_running": False,
            "metadata": {},
        }


def get_task_lock_states(task_names: tuple[str, ...] = SINGLE_INSTANCE_SYNC_TASKS) -> dict:
    return {task_name: get_task_lock_status(task_name) for task_name in task_names}


@contextmanager
def task_lock(task_name: str):
    path = _lock_path(task_name)
    with open(path, "a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise TaskAlreadyRunningError(task_name, _read_lock_payload(handle)) from exc

        payload = {
            "task_name": task_name,
            "pid": os.getpid(),
            "started_at": datetime.now().isoformat(timespec="seconds"),
        }
        handle.seek(0)
        handle.truncate()
        json.dump(payload, handle, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
        lock_handle = TaskLockHandle(task_name, handle, payload)

        try:
            yield lock_handle
        finally:
            handle.seek(0)
            handle.truncate()
            handle.flush()
            os.fsync(handle.fileno())
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
