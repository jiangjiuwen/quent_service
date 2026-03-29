#!/usr/bin/env python3

import argparse
import json
import os
import sqlite3
import statistics
from datetime import date
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="校验日线数据完整性")
    parser.add_argument("--db-path", help="SQLite 数据库路径，默认读取 QUANT_DB_PATH 或本地默认库")
    parser.add_argument("--start-date", default="2011-03-25", help="校验窗口开始日期，格式 YYYY-MM-DD")
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="校验窗口结束日期，格式 YYYY-MM-DD，默认今天",
    )
    parser.add_argument("--top", type=int, default=20, help="输出缺失最严重股票数量")
    return parser.parse_args()


def resolve_db_path(explicit_path: str | None) -> Path:
    if explicit_path:
        return Path(explicit_path).expanduser().resolve()

    env_path = os.getenv("QUANT_DB_PATH")
    if env_path:
        return Path(env_path).expanduser().resolve()

    default_path = Path(__file__).resolve().parents[1] / "data" / "a_stock_quant.db"
    return default_path.resolve()


def fetch_rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    row = conn.execute(sql, params).fetchone()
    if not row:
        return None
    return row[0]


def build_report(conn: sqlite3.Connection, start_date: str, end_date: str, top_n: int) -> dict:
    last_open_trade_date = scalar(
        conn,
        """
        SELECT MAX(trade_date)
        FROM trading_calendar
        WHERE trade_date <= ?
          AND is_open = 1
        """,
        (end_date,),
    )
    if not last_open_trade_date:
        raise RuntimeError("交易日历为空，无法校验日线完整性")

    open_dates = [
        row["trade_date"]
        for row in fetch_rows(
            conn,
            """
            SELECT trade_date
            FROM trading_calendar
            WHERE trade_date <= ?
              AND is_open = 1
            ORDER BY trade_date
            """,
            (end_date,),
        )
    ]
    open_date_rank = {trade_date: idx for idx, trade_date in enumerate(open_dates)}

    rows = fetch_rows(
        conn,
        """
        WITH stock_base AS (
            SELECT
                stock_code,
                stock_name,
                market_type,
                exchange,
                CASE
                    WHEN list_date IS NULL OR list_date < ? THEN ?
                    ELSE list_date
                END AS effective_start,
                CASE
                    WHEN delist_date IS NOT NULL AND delist_date < ? THEN delist_date
                    ELSE ?
                END AS effective_end
            FROM stocks
            WHERE status = 1
        ),
        expected AS (
            SELECT
                s.stock_code,
                s.stock_name,
                s.market_type,
                s.exchange,
                s.effective_start,
                s.effective_end,
                COUNT(tc.trade_date) AS expected_days
            FROM stock_base s
            LEFT JOIN trading_calendar tc
              ON tc.is_open = 1
             AND tc.trade_date BETWEEN s.effective_start AND s.effective_end
            GROUP BY
                s.stock_code,
                s.stock_name,
                s.market_type,
                s.exchange,
                s.effective_start,
                s.effective_end
        ),
        actual AS (
            SELECT
                stock_code,
                COUNT(*) AS actual_days,
                MIN(trade_date) AS first_date,
                MAX(trade_date) AS last_date
            FROM daily_kline
            WHERE trade_date BETWEEN ? AND ?
            GROUP BY stock_code
        )
        SELECT
            e.stock_code,
            e.stock_name,
            e.market_type,
            e.exchange,
            e.effective_start,
            e.effective_end,
            e.expected_days,
            COALESCE(a.actual_days, 0) AS actual_days,
            a.first_date,
            a.last_date,
            ROUND(
                CASE
                    WHEN e.expected_days > 0 THEN COALESCE(a.actual_days, 0) * 100.0 / e.expected_days
                END,
                2
            ) AS coverage_pct,
            e.expected_days - COALESCE(a.actual_days, 0) AS missing_days
        FROM expected e
        LEFT JOIN actual a
          ON a.stock_code = e.stock_code
        WHERE e.expected_days > 0
        """,
        (start_date, start_date, end_date, end_date, start_date, end_date),
    )

    coverages = [row["coverage_pct"] for row in rows if row["coverage_pct"] is not None]

    for row in rows:
        row["code_prefix"] = row["stock_code"][:3]
        last_date = row["last_date"]
        if not last_date:
            row["lag_open_days"] = None
        else:
            row["lag_open_days"] = open_date_rank[last_open_trade_date] - open_date_rank.get(
                last_date,
                open_date_rank[last_open_trade_date],
            )

    summary = {
        "window_start": start_date,
        "window_end": end_date,
        "last_open_trade_date": last_open_trade_date,
        "trade_days_in_window": scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM trading_calendar
            WHERE trade_date BETWEEN ? AND ?
              AND is_open = 1
            """,
            (start_date, end_date),
        ),
        "active_stocks": len(rows),
        "avg_coverage_pct": round(sum(coverages) / len(coverages), 2) if coverages else 0,
        "median_coverage_pct": round(statistics.median(coverages), 2) if coverages else 0,
        "stocks_full_coverage": sum(1 for row in rows if row["coverage_pct"] == 100.0),
        "stocks_ge_99": sum(1 for row in rows if row["coverage_pct"] is not None and row["coverage_pct"] >= 99),
        "stocks_ge_95": sum(1 for row in rows if row["coverage_pct"] is not None and row["coverage_pct"] >= 95),
        "stocks_lt_95": sum(1 for row in rows if row["coverage_pct"] is not None and row["coverage_pct"] < 95),
        "stocks_lt_80": sum(1 for row in rows if row["coverage_pct"] is not None and row["coverage_pct"] < 80),
        "stocks_lt_50": sum(1 for row in rows if row["coverage_pct"] is not None and row["coverage_pct"] < 50),
        "stocks_lt_20": sum(1 for row in rows if row["coverage_pct"] is not None and row["coverage_pct"] < 20),
        "stocks_with_no_kline": sum(1 for row in rows if row["actual_days"] == 0),
        "stocks_latest_today": sum(1 for row in rows if row["last_date"] == last_open_trade_date),
        "stocks_last_date_before_latest": sum(1 for row in rows if (row["last_date"] or "") < last_open_trade_date),
    }

    lag_buckets = {
        "latest_today": sum(1 for row in rows if row["last_date"] == last_open_trade_date),
        "lag_1_open_day": sum(1 for row in rows if row["lag_open_days"] == 1),
        "lag_2_to_5_open_days": sum(
            1 for row in rows if row["lag_open_days"] is not None and 2 <= row["lag_open_days"] <= 5
        ),
        "lag_6_to_20_open_days": sum(
            1 for row in rows if row["lag_open_days"] is not None and 6 <= row["lag_open_days"] <= 20
        ),
        "lag_gt_20_open_days": sum(
            1 for row in rows if row["lag_open_days"] is not None and row["lag_open_days"] > 20
        ),
        "no_kline": sum(1 for row in rows if row["last_date"] is None),
    }

    by_market: dict[str, dict] = {}
    for market in sorted({row["market_type"] or "UNKNOWN" for row in rows}):
        subset = [row for row in rows if (row["market_type"] or "UNKNOWN") == market]
        by_market[market] = {
            "stocks": len(subset),
            "latest_today": sum(1 for row in subset if row["last_date"] == last_open_trade_date),
            "no_kline": sum(1 for row in subset if row["last_date"] is None),
            "lag_gt_20_open_days": sum(
                1
                for row in subset
                if row["lag_open_days"] is not None and row["lag_open_days"] > 20
            ),
        }

    worst_missing = sorted(
        rows,
        key=lambda row: (row["missing_days"], row["expected_days"], row["stock_code"]),
        reverse=True,
    )[:top_n]
    stale_last_date = sorted(
        [row for row in rows if (row["last_date"] or "") < last_open_trade_date],
        key=lambda row: (
            row["last_date"] or "",
            row["coverage_pct"] if row["coverage_pct"] is not None else -1,
            row["stock_code"],
        ),
    )[:top_n]

    return {
        "summary": summary,
        "lag_buckets": lag_buckets,
        "by_market": by_market,
        "worst_missing": worst_missing,
        "stale_last_date": stale_last_date,
    }


def main() -> int:
    args = parse_args()
    db_path = resolve_db_path(args.db_path)
    end_date = args.end_date or os.getenv("VALIDATE_END_DATE") or date.today().isoformat()

    if not db_path.exists():
        raise FileNotFoundError(f"数据库不存在: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        report = build_report(conn, args.start_date, end_date, args.top)
    finally:
        conn.close()

    print(json.dumps({"db_path": str(db_path), **report}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
