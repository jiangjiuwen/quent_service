import json
import os
import subprocess
import sys
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from config.settings import BASE_DIR, SYNC_HOUR, SYNC_MINUTE
from database.connection import db
from sync.task_locks import get_task_lock_status
from utils.logger import logger


RUNNER_SCRIPT = BASE_DIR / "scripts" / "run_sync_task.py"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
LATEST_DAILY_SYNC_LOOKBACK_OPEN_DAYS = 1


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


def _latest_stock_trade_date() -> date | None:
    row = db.fetchone(
        """
        SELECT MAX(trade_date) AS value
        FROM daily_trade_flags
        """
    )
    value = row.get("value") if row else None
    if not value:
        return None
    return date.fromisoformat(value)


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


def _scheduled_cutoff(reference_date: date) -> datetime:
    return datetime.combine(
        reference_date,
        time(hour=SYNC_HOUR, minute=SYNC_MINUTE),
        tzinfo=SHANGHAI_TZ,
    )


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


def trigger_startup_daily_catchup_if_needed() -> None:
    """服务启动后检查是否错过当日收盘同步，必要时补跑最近交易日增量"""
    now = datetime.now(SHANGHAI_TZ)
    latest_open_trade_date = _latest_open_trade_date(now.date())
    if latest_open_trade_date is None:
        logger.warning("启动补跑检查跳过，交易日历中未找到最近交易日")
        return

    latest_stock_trade_date = _latest_stock_trade_date()
    if latest_stock_trade_date is not None and latest_stock_trade_date >= latest_open_trade_date:
        logger.info(
            f"启动补跑检查完成，股票日线已是最新交易日: {latest_stock_trade_date.isoformat()}"
        )
        return

    if latest_open_trade_date == now.date() and now < _scheduled_cutoff(now.date()):
        logger.info(
            f"启动补跑检查跳过，当前时间未到计划同步时点: "
            f"{now.isoformat(timespec='seconds')} < {_scheduled_cutoff(now.date()).isoformat(timespec='seconds')}"
        )
        return

    kwargs = build_latest_daily_sync_kwargs(
        reference_date=latest_open_trade_date,
        lookback_open_days=LATEST_DAILY_SYNC_LOOKBACK_OPEN_DAYS,
    )
    if kwargs is None:
        logger.warning(
            f"启动补跑检查跳过，未找到 {latest_open_trade_date.isoformat()} 对应的交易日同步窗口"
        )
        return

    logger.info(
        f"启动补跑检查命中，准备补跑股票日线增量: "
        f"latest_stock_trade_date={latest_stock_trade_date.isoformat() if latest_stock_trade_date else 'none'}, "
        f"latest_open_trade_date={latest_open_trade_date.isoformat()}, "
        f"window={kwargs['start_date']}->{kwargs['end_date']}"
    )
    trigger_scheduled_task("daily_kline", **kwargs)


def trigger_scheduled_stock_list_sync() -> None:
    trigger_scheduled_task("stock_list", trigger_profile_sync=False)


def trigger_scheduled_index_list_sync() -> None:
    trigger_scheduled_task("index_list")


def trigger_scheduled_stock_profile_sync() -> None:
    trigger_scheduled_task("stock_profiles", limit=None, only_missing=True)


def trigger_scheduled_adjust_factor_sync() -> None:
    trigger_scheduled_task("adjust_factors", limit=None)


def trigger_scheduled_corporate_action_sync() -> None:
    trigger_scheduled_task("corporate_actions", limit=None, years_back=3)


def trigger_scheduled_financial_sync() -> None:
    trigger_scheduled_task("financial", only_missing=False)
