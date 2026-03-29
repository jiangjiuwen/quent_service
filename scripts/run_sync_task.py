#!/usr/bin/env python3

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.connection import db
from sync.tasks import (
    sync_adjust_factors,
    sync_benchmark_index_kline,
    sync_corporate_actions,
    sync_daily_kline,
    sync_financial_data,
    sync_full_refresh,
    sync_index_list,
    sync_market_overview_refresh,
    sync_scorecard_refresh,
    sync_stock_list,
    sync_stock_profiles,
    sync_trade_calendar,
)
from utils.logger import logger


TASK_RUNNERS = {
    "adjust_factors": sync_adjust_factors,
    "benchmark_index_kline": sync_benchmark_index_kline,
    "corporate_actions": sync_corporate_actions,
    "daily_kline": sync_daily_kline,
    "financial": sync_financial_data,
    "full_refresh": sync_full_refresh,
    "index_list": sync_index_list,
    "market_overview_refresh": sync_market_overview_refresh,
    "scorecard_refresh": sync_scorecard_refresh,
    "stock_list": sync_stock_list,
    "stock_profiles": sync_stock_profiles,
    "trading_calendar": sync_trade_calendar,
}


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: run_sync_task.py <task_name> [json_kwargs]", file=sys.stderr)
        return 1

    task_name = sys.argv[1]
    if task_name not in TASK_RUNNERS:
        print(f"unknown task: {task_name}", file=sys.stderr)
        return 1

    try:
        kwargs = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    except json.JSONDecodeError as exc:
        print(f"invalid json kwargs: {exc}", file=sys.stderr)
        return 1

    if not isinstance(kwargs, dict):
        print("json kwargs must be an object", file=sys.stderr)
        return 1

    db.init_tables()
    logger.info(f"同步任务子进程启动: task={task_name}, kwargs={kwargs}")

    try:
        TASK_RUNNERS[task_name](**kwargs)
        logger.info(f"同步任务子进程完成: task={task_name}")
        return 0
    except Exception as exc:
        logger.exception(f"同步任务子进程失败: task={task_name}, error={exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
