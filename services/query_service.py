import time
from datetime import date, datetime
from typing import List, Optional, Tuple

from database.connection import db
from sync.task_locks import SINGLE_INSTANCE_SYNC_TASKS, get_task_lock_states


class QueryService:
    """数据查询服务"""

    def __init__(self):
        self._cache: dict[str, tuple[float, object]] = {}

    def _cached(self, key: str, ttl_seconds: float, builder):
        now = time.monotonic()
        cached = self._cache.get(key)
        if cached and cached[0] > now:
            return cached[1]

        value = builder()
        self._cache[key] = (now + ttl_seconds, value)
        return value

    def _scalar(self, sql: str, params: tuple = ()):
        row = db.fetchone(sql, params)
        if not row:
            return None
        return row.get("value")

    def _reconcile_stale_sync_logs(self, running_tasks: dict) -> None:
        placeholders = ", ".join("?" for _ in SINGLE_INSTANCE_SYNC_TASKS)
        rows = db.fetchall(
            f"""
            SELECT log_id, sync_type, start_time
            FROM sync_logs
            WHERE status = 'running'
              AND sync_type IN ({placeholders})
            ORDER BY sync_type, start_time DESC
            """,
            SINGLE_INSTANCE_SYNC_TASKS,
        )
        if not rows:
            return

        stale_log_ids = []
        grouped = {}
        for row in rows:
            grouped.setdefault(row["sync_type"], []).append(row)

        for sync_type, items in grouped.items():
            if running_tasks.get(sync_type, {}).get("is_running"):
                stale_log_ids.extend(item["log_id"] for item in items[1:])
            else:
                stale_log_ids.extend(item["log_id"] for item in items)

        for log_id in stale_log_ids:
            db.execute(
                """
                UPDATE sync_logs
                SET end_time = COALESCE(end_time, ?),
                    status = 'aborted',
                    error_message = COALESCE(error_message, '任务已终止或被新的单实例任务替代')
                WHERE log_id = ?
                """,
                (datetime.now(), log_id),
            )

    def _get_latest_sync_map(self) -> dict:
        row = db.fetchone(
            """
            SELECT
                sync_type,
                start_time AS last_time,
                status
            FROM sync_logs
            WHERE sync_type = 'daily_kline'
            ORDER BY start_time DESC
            LIMIT 1
            """,
        )
        if not row:
            return {}
        return {row["sync_type"]: row}

    def _sync_status_last_sync_payload(self, latest_sync_map: dict) -> list[dict]:
        return [
            {
                "sync_type": row["sync_type"],
                "status": row.get("status"),
                "last_time": row.get("last_time"),
            }
            for row in latest_sync_map.values()
        ]

    def _sync_status_running_tasks_payload(self, running_tasks: dict) -> dict:
        payload = {}
        for task_name, state in (running_tasks or {}).items():
            metadata = (state or {}).get("metadata") or {}
            payload[task_name] = {
                "is_running": bool((state or {}).get("is_running")),
                "metadata": {
                    "started_at": metadata.get("started_at"),
                    "updated_at": metadata.get("updated_at"),
                    "total": metadata.get("total"),
                    "processed": metadata.get("processed"),
                },
            }
        return payload

    def get_stocks(
        self,
        market: Optional[str] = None,
        industry: Optional[str] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Tuple[List[dict], int]:
        """获取股票列表"""
        where_clauses = ["status = 1"]
        params = []

        if market:
            where_clauses.append("market_type = ?")
            params.append(market)
        if industry:
            where_clauses.append("industry_code = ?")
            params.append(industry)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        count_sql = f"SELECT COUNT(*) as total FROM stocks WHERE {where_sql}"
        total_result = db.fetchone(count_sql, tuple(params))
        total = total_result["total"] if total_result else 0

        offset = (page - 1) * page_size
        sql = f"""
            SELECT * FROM stocks
            WHERE {where_sql}
            ORDER BY stock_code
            LIMIT ? OFFSET ?
        """
        params.extend([page_size, offset])

        data = db.fetchall(sql, tuple(params))
        return data, total

    def search_stocks(self, keyword: str, limit: int = 12) -> List[dict]:
        """按代码或名称搜索股票"""
        keyword = keyword.strip()
        if not keyword:
            return []

        like_keyword = f"%{keyword}%"
        prefix_keyword = f"{keyword}%"
        sql = """
            SELECT *
            FROM stocks
            WHERE status = 1
              AND (stock_code LIKE ? OR stock_name LIKE ?)
            ORDER BY
                CASE
                    WHEN stock_code = ? THEN 0
                    WHEN stock_code LIKE ? THEN 1
                    WHEN stock_name LIKE ? THEN 2
                    ELSE 3
                END,
                stock_code
            LIMIT ?
        """
        return db.fetchall(
            sql,
            (
                like_keyword,
                like_keyword,
                keyword,
                prefix_keyword,
                prefix_keyword,
                limit,
            ),
        )

    def get_stock_by_code(self, stock_code: str) -> Optional[dict]:
        """根据代码获取股票信息"""
        sql = "SELECT * FROM stocks WHERE stock_code = ?"
        return db.fetchone(sql, (stock_code,))

    def get_daily_kline(
        self,
        stock_code: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
    ) -> List[dict]:
        """获取日K线数据"""
        where_clauses = ["stock_code = ?"]
        params = [stock_code]

        if start_date:
            where_clauses.append("trade_date >= ?")
            params.append(start_date.isoformat())
        if end_date:
            where_clauses.append("trade_date <= ?")
            params.append(end_date.isoformat())

        where_sql = " AND ".join(where_clauses)

        sql = f"""
            SELECT * FROM daily_kline
            WHERE {where_sql}
            ORDER BY trade_date DESC
            LIMIT ?
        """
        params.append(limit)

        return db.fetchall(sql, tuple(params))

    def get_latest_price(self, stock_code: str) -> Optional[dict]:
        """获取最新价格"""
        sql = """
            SELECT * FROM daily_kline
            WHERE stock_code = ?
            ORDER BY trade_date DESC
            LIMIT 2
        """
        rows = db.fetchall(sql, (stock_code,))

        if not rows:
            return None

        latest = rows[0]
        result = {
            "stock_code": stock_code,
            "trade_date": latest["trade_date"],
            "close_price": latest["close_price"],
            "change": None,
            "pct_change": None,
        }

        if len(rows) > 1:
            prev = rows[1]
            change = latest["close_price"] - prev["close_price"]
            pct_change = (change / prev["close_price"]) * 100 if prev["close_price"] else 0
            result["change"] = round(change, 4)
            result["pct_change"] = round(pct_change, 2)

        return result

    def get_financial_data(self, stock_code: str, report_period: Optional[str] = None) -> List[dict]:
        """获取财务数据"""
        where_clauses = ["stock_code = ?"]
        params = [stock_code]

        if report_period:
            where_clauses.append("report_period = ?")
            params.append(report_period)

        where_sql = " AND ".join(where_clauses)

        sql = f"""
            SELECT * FROM financial_reports
            WHERE {where_sql}
            ORDER BY report_period DESC
        """

        return db.fetchall(sql, tuple(params))

    def get_stock_industry(self, stock_code: str) -> Optional[dict]:
        """获取股票所属行业"""
        sql = """
            SELECT s.stock_code, s.stock_name, i.industry_code, i.industry_name
            FROM stocks s
            LEFT JOIN industries i ON s.industry_code = i.industry_code
            WHERE s.stock_code = ?
        """
        return db.fetchone(sql, (stock_code,))

    def _summary_table_counts(self) -> dict:
        def _build():
            return {
                "total_stocks": int(self._scalar("SELECT COUNT(*) AS value FROM stocks WHERE status = 1") or 0),
                "total_stock_records": int(self._scalar("SELECT COUNT(*) AS value FROM stocks") or 0),
                "inactive_stock_records": int(self._scalar("SELECT COUNT(*) AS value FROM stocks WHERE status != 1") or 0),
                "total_indices": int(self._scalar("SELECT COUNT(*) AS value FROM indices WHERE status = 1") or 0),
                "total_kline_records": int(self._scalar("SELECT COUNT(*) AS value FROM daily_kline") or 0),
            }

        return self._cached("sync_status_summary_counts", 15, _build) or {}

    def get_sync_status(self) -> dict:
        """获取同步状态"""
        running_tasks = get_task_lock_states()
        self._reconcile_stale_sync_logs(running_tasks)

        latest_sync_map = self._get_latest_sync_map()
        last_sync = self._sync_status_last_sync_payload(latest_sync_map)
        running_task_payload = self._sync_status_running_tasks_payload(running_tasks)
        summary_counts = self._summary_table_counts()
        total_stocks = int(summary_counts.get("total_stocks") or 0)
        payload = {
            "last_sync": last_sync,
            "total_stocks": total_stocks,
            "total_stock_records": int(summary_counts.get("total_stock_records") or 0),
            "inactive_stock_records": int(summary_counts.get("inactive_stock_records") or 0),
            "total_indices": int(summary_counts.get("total_indices") or 0),
            "total_kline_records": int(summary_counts.get("total_kline_records") or 0),
            "running_tasks": running_task_payload,
        }
        return payload
