#!/usr/bin/env python3

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


WORKSPACE_DIR = Path(
    os.getenv("QUANT_WATCH_WORKSPACE_DIR", Path(__file__).resolve().parents[1])
).resolve()
DEPLOY_SCRIPT = Path(
    os.getenv("QUANT_WATCH_DEPLOY_SCRIPT", WORKSPACE_DIR / "scripts/prod_deploy.sh")
).resolve()
STATE_FILE = Path(
    os.getenv("QUANT_WATCH_STATE_FILE", WORKSPACE_DIR / "watch_state.json")
).resolve()
POLL_SECONDS = float(os.getenv("QUANT_WATCH_POLL_SECONDS", "2"))
DEBOUNCE_SECONDS = float(os.getenv("QUANT_WATCH_DEBOUNCE_SECONDS", "3"))

IGNORED_DIRS = {
    ".git",
    ".venv",
    "data",
    "logs",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
}
IGNORED_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".swp",
    ".tmp",
    ".log",
    ".zip",
}
WATCHED_SUFFIXES = {
    ".py",
    ".sh",
    ".html",
    ".css",
    ".js",
    ".json",
    ".toml",
    ".yaml",
    ".yml",
    ".ini",
    ".txt",
}
WATCHED_FILENAMES = {"requirements.txt"}

running = True


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} | watch | {message}", flush=True)


def should_watch(path: Path) -> bool:
    parts = set(path.parts)
    if parts & IGNORED_DIRS:
        return False
    if path.name.startswith(".") and path.name not in WATCHED_FILENAMES:
        return False
    if path.name in WATCHED_FILENAMES:
        return True
    return path.suffix.lower() in WATCHED_SUFFIXES and path.suffix.lower() not in IGNORED_SUFFIXES


def snapshot_workspace() -> dict[str, tuple[int, int]]:
    snapshot: dict[str, tuple[int, int]] = {}
    for path in WORKSPACE_DIR.rglob("*"):
        if not path.is_file() or not should_watch(path.relative_to(WORKSPACE_DIR)):
            continue

        stat = path.stat()
        snapshot[str(path.relative_to(WORKSPACE_DIR))] = (stat.st_mtime_ns, stat.st_size)
    return snapshot


def changed_files(old: dict[str, tuple[int, int]], new: dict[str, tuple[int, int]]) -> list[str]:
    paths = sorted(set(old) | set(new))
    return [path for path in paths if old.get(path) != new.get(path)]


def write_state(status: str, files: list[str] | None = None, error: str | None = None) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": status,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "workspace": str(WORKSPACE_DIR),
        "deploy_script": str(DEPLOY_SCRIPT),
        "files": files or [],
        "error": error,
    }
    STATE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def deploy() -> bool:
    log(f"检测到代码变化，开始部署: {DEPLOY_SCRIPT}")
    write_state("deploying")
    result = subprocess.run([str(DEPLOY_SCRIPT)], cwd=str(WORKSPACE_DIR), check=False)
    if result.returncode == 0:
        log("自动部署完成")
        write_state("idle")
        return True

    message = f"自动部署失败，exit_code={result.returncode}"
    log(message)
    write_state("failed", error=message)
    return False


def handle_signal(signum, _frame) -> None:
    global running
    running = False
    log(f"收到退出信号: {signum}")


def main() -> int:
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    if not DEPLOY_SCRIPT.exists():
        log(f"部署脚本不存在: {DEPLOY_SCRIPT}")
        write_state("failed", error=f"missing deploy script: {DEPLOY_SCRIPT}")
        return 1

    log(f"开始监听工作区: {WORKSPACE_DIR}")
    write_state("idle")

    previous = snapshot_workspace()
    pending_files: list[str] = []
    last_change_at: float | None = None

    while running:
        current = snapshot_workspace()
        changed = changed_files(previous, current)
        if changed:
            pending_files = changed
            last_change_at = time.monotonic()
            previous = current
            log(f"检测到 {len(changed)} 个变更文件")
            write_state("pending", files=changed)
        elif pending_files and last_change_at is not None:
            if time.monotonic() - last_change_at >= DEBOUNCE_SECONDS:
                success = deploy()
                if success:
                    pending_files = []
                    last_change_at = None
                    previous = snapshot_workspace()
        time.sleep(POLL_SECONDS)

    write_state("stopped")
    log("自动更新监听已停止")
    return 0


if __name__ == "__main__":
    sys.exit(main())
