import json
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from config.settings import BASE_DIR
from database.connection import db
from sync.task_locks import get_task_lock_status
from utils.logger import logger


RUNNER_SCRIPT = BASE_DIR / "scripts" / "run_sync_task.py"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
LATEST_DAILY_SYNC_LOOKBACK_OPEN_DAYS = 5


def spawn_sync_task(task_name: str, **kwargs: Any) -> dict:
    """以独立子进程启动同步任务，避免与 API / 调度器共享运行时状态"""
    status = get_task_lock_status(task_name)
    if status.get("is_running"):
        metadata = status.get("metadata", {})
        return {
            "spawned": False,
            "task_name": task_name,
            "pid": metadata.get("pid"),
            "started_at": metadata.get("started_at"),
        }

    env = os.environ.copy()
    payload = json.dumps(kwargs, ensure_ascii=False)
    process = subprocess.Popen(
        [sys.executable, str(RUNNER_SCRIPT), task_name, payload],
        cwd=str(BASE_DIR),
        env=env,
        start_new_session=True,
    )
    logger.info(f"已启动同步子进程: task={task_name}, pid={process.pid}")
    return {
        "spawned": True,
        "task_name": task_name,
        "pid": process.pid,
    }


def _current_shanghai_date() -> date:
    return datetime.now(SHANGHAI_TZ).date()


def _is_open_trade_date(trade_date: date) -> bool:
    row = db.fetchone(
        """
        SELECT is_open AS value
        FROM trading_calendar
        WHERE trade_date = ?
        """,
        (trade_date.isoformat(),),
    )
    return bool(row and row.get("value") == 1)


def _latest_open_trade_date(reference_date: date) -> date | None:
    row = db.fetchone(
        """
        SELECT MAX(trade_date) AS value
        FROM trading_calendar
        WHERE trade_date <= ?
          AND is_open = 1
        """,
        (reference_date.isoformat(),),
    )
    value = row.get("value") if row else None
    if not value:
        return None
    return date.fromisoformat(value)


def _recent_open_trade_window(end_date: date, lookback_open_days: int = LATEST_DAILY_SYNC_LOOKBACK_OPEN_DAYS) -> tuple[str, str]:
    rows = db.fetchall(
        """
        SELECT trade_date
        FROM trading_calendar
        WHERE trade_date <= ?
          AND is_open = 1
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (end_date.isoformat(), max(int(lookback_open_days or 1), 1)),
    )
    if not rows:
        iso_date = end_date.isoformat()
        return iso_date, iso_date

    start_date = rows[-1]["trade_date"]
    return start_date, end_date.isoformat()


def build_latest_daily_sync_kwargs(
    reference_date: date | None = None,
    lookback_open_days: int = LATEST_DAILY_SYNC_LOOKBACK_OPEN_DAYS,
) -> dict | None:
    resolved_reference_date = reference_date or _current_shanghai_date()
    end_date = _latest_open_trade_date(resolved_reference_date)
    if end_date is None:
        return None
    start_date, end_date_text = _recent_open_trade_window(end_date, lookback_open_days=lookback_open_days)
    return {
        "start_date": start_date,
        "end_date": end_date_text,
    }


def spawn_latest_daily_sync(lookback_open_days: int = LATEST_DAILY_SYNC_LOOKBACK_OPEN_DAYS) -> dict:
    kwargs = build_latest_daily_sync_kwargs(lookback_open_days=lookback_open_days)
    if kwargs is None:
        return {
            "spawned": False,
            "task_name": "daily_kline",
            "reason": "no_open_trade_day",
        }

    result = spawn_sync_task("daily_kline", **kwargs)
    result.update(kwargs)
    return result


def trigger_scheduled_task(task_name: str, **kwargs: Any) -> None:
    """供 APScheduler 调用的计划任务派发器"""
    result = spawn_sync_task(task_name, **kwargs)
    if result.get("spawned"):
        logger.info(f"计划任务已派发: task={task_name}, pid={result.get('pid')}")
        return

    detail_parts = []
    if result.get("started_at"):
        detail_parts.append(f"started_at={result['started_at']}")
    if result.get("pid"):
        detail_parts.append(f"pid={result['pid']}")
    detail_text = f" ({', '.join(detail_parts)})" if detail_parts else ""
    logger.warning(f"计划任务跳过，{task_name}已在运行{detail_text}")


def trigger_scheduled_daily_sync() -> None:
    """仅在交易日收盘后触发最近交易日的日线增量同步"""
    today = _current_shanghai_date()
    if not _is_open_trade_date(today):
        logger.info(f"计划任务跳过，{today.isoformat()} 非交易日")
        return

    kwargs = build_latest_daily_sync_kwargs(reference_date=today)
    if kwargs is None:
        logger.warning(f"计划任务跳过，未找到 {today.isoformat()} 对应的交易日窗口")
        return

    trigger_scheduled_task("daily_kline", **kwargs)
