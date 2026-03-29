from __future__ import annotations

from datetime import datetime

from database.connection import db
from sync.task_dispatcher import spawn_sync_task
from sync.task_locks import get_task_lock_status


MARKET_OVERVIEW_REFRESH_TASK = "market_overview_refresh"
MARKET_OVERVIEW_BENCHMARKS = (
    "000300",
    "000905",
    "399300",
    "399905",
)


def _to_float(value):
    if value is None or value == "":
        return None
    return float(value)


def _round_or_none(value, digits: int = 4):
    if value is None:
        return None
    return round(value, digits)


def _clip_int(value: float, lower: int = 0, upper: int = 100) -> int:
    return max(lower, min(int(round(value)), upper))


class MarketOverviewService:
    def _scalar(self, sql: str, params: tuple = ()):
        row = db.fetchone(sql, params)
        if not row:
            return None
        return row.get("value")

    def _latest_kline_trade_date(self) -> str | None:
        return self._scalar("SELECT MAX(trade_date) AS value FROM daily_kline")

    def _overview_state(self) -> dict:
        return db.fetchone(
            """
            WITH latest AS (
                SELECT MAX(trade_date) AS trade_date
                FROM market_sentiment_daily
            )
            SELECT
                latest.trade_date AS trade_date,
                (
                    SELECT updated_at
                    FROM market_sentiment_daily
                    WHERE trade_date = latest.trade_date
                    LIMIT 1
                ) AS updated_at,
                (
                    SELECT COUNT(*)
                    FROM market_sentiment_daily
                ) AS total_days,
                (
                    SELECT COUNT(*)
                    FROM sector_strength_daily
                    WHERE trade_date = latest.trade_date
                ) AS sector_count,
                (
                    SELECT COUNT(*)
                    FROM stock_event_signals_daily
                    WHERE trade_date = latest.trade_date
                ) AS event_count,
                (
                    SELECT COUNT(*)
                    FROM market_fund_flow_daily
                    WHERE trade_date <= latest.trade_date
                ) AS market_flow_count,
                (
                    SELECT COUNT(*)
                    FROM sector_fund_flow_daily
                    WHERE trade_date <= latest.trade_date
                ) AS sector_flow_count
            FROM latest
            """
        ) or {
            "trade_date": None,
            "updated_at": None,
            "total_days": 0,
            "sector_count": 0,
            "event_count": 0,
            "market_flow_count": 0,
            "sector_flow_count": 0,
        }

    def _is_overview_current(self, state: dict, latest_trade_date: str | None) -> bool:
        return bool(
            latest_trade_date
            and state.get("trade_date") == latest_trade_date
            and int(state.get("total_days") or 0) > 0
            and int(state.get("sector_count") or 0) > 0
        )

    def _status_payload(self, state: dict, latest_trade_date: str | None, **extra) -> dict:
        payload = {
            "trade_date": state.get("trade_date"),
            "latest_market_trade_date": latest_trade_date,
            "updated_at": state.get("updated_at"),
            "total_days": int(state.get("total_days") or 0),
            "sector_count": int(state.get("sector_count") or 0),
            "event_count": int(state.get("event_count") or 0),
            "is_current": self._is_overview_current(state, latest_trade_date),
            "is_stale": bool(int(state.get("total_days") or 0) > 0) and not self._is_overview_current(state, latest_trade_date),
            "pending_refresh": False,
            "refresh_reason": None,
            "refresh_started_at": None,
            "refresh_pid": None,
        }
        payload.update(extra)
        return payload

    def _schedule_refresh(self) -> dict:
        refresh_status = get_task_lock_status(MARKET_OVERVIEW_REFRESH_TASK)
        refresh_metadata = refresh_status.get("metadata", {})
        if refresh_status.get("is_running"):
            return {
                "pending_refresh": True,
                "refresh_reason": "market_overview_refresh_running",
                "refresh_started_at": refresh_metadata.get("started_at"),
                "refresh_pid": refresh_metadata.get("pid"),
            }

        daily_status = get_task_lock_status("daily_kline")
        daily_metadata = daily_status.get("metadata", {})
        if daily_status.get("is_running"):
            return {
                "pending_refresh": True,
                "refresh_reason": "daily_kline_running",
                "refresh_started_at": daily_metadata.get("started_at"),
                "refresh_pid": daily_metadata.get("pid"),
            }

        result = spawn_sync_task(MARKET_OVERVIEW_REFRESH_TASK)
        if result.get("spawned"):
            return {
                "pending_refresh": True,
                "refresh_reason": "scheduled",
                "refresh_started_at": datetime.now().isoformat(timespec="seconds"),
                "refresh_pid": result.get("pid"),
            }

        return {
            "pending_refresh": False,
            "refresh_reason": "schedule_failed",
            "refresh_started_at": result.get("started_at"),
            "refresh_pid": result.get("pid"),
        }

    def ensure_overview_current(self) -> dict:
        latest_trade_date = self._latest_kline_trade_date()
        if not latest_trade_date:
            return self._status_payload(
                self._overview_state(),
                latest_trade_date,
                refresh_reason="no_kline_data",
            )

        state = self._overview_state()
        if self._is_overview_current(state, latest_trade_date):
            return self._status_payload(state, latest_trade_date)

        refresh_status = self._schedule_refresh()
        return self._status_payload(state, latest_trade_date, **refresh_status)

    def _benchmark_snapshot(self, trade_date: str | None, limit: int = 4) -> list[dict]:
        if not trade_date:
            return []

        items: list[dict] = []
        seen_names: set[str] = set()
        sql = """
            SELECT
                dk.trade_date,
                dk.close_price,
                dk.pct_change,
                COALESCE(i.index_name, dk.stock_code) AS index_name
            FROM daily_kline dk
            LEFT JOIN indices i
              ON i.index_code = dk.stock_code
            WHERE dk.stock_code = ?
              AND dk.trade_date <= ?
            ORDER BY dk.trade_date DESC
            LIMIT 6
        """
        for index_code in MARKET_OVERVIEW_BENCHMARKS:
            rows = db.fetchall(sql, (index_code, trade_date))
            if not rows:
                continue
            rows.reverse()
            index_name = rows[-1].get("index_name") or index_code
            if index_name in seen_names:
                continue
            seen_names.add(index_name)

            closes = [_to_float(row.get("close_price")) for row in rows]
            return_5d = None
            if len(closes) > 5 and closes[-1] is not None and closes[-6] not in (None, 0):
                return_5d = (closes[-1] / closes[-6]) - 1

            latest = rows[-1]
            items.append(
                {
                    "index_code": index_code,
                    "index_name": index_name,
                    "trade_date": latest.get("trade_date"),
                    "close_price": _round_or_none(_to_float(latest.get("close_price"))),
                    "pct_change": _round_or_none(_to_float(latest.get("pct_change")), 4),
                    "return_5d": _round_or_none(return_5d, 6),
                }
            )
            if len(items) >= limit:
                break
        return items

    def _sentiment_payload(self, trade_date: str | None) -> dict | None:
        if not trade_date:
            return None
        latest = db.fetchone(
            """
            SELECT *
            FROM market_sentiment_daily
            WHERE trade_date = ?
            """,
            (trade_date,),
        )
        if not latest:
            return None

        history = db.fetchall(
            """
            SELECT trade_date, sentiment_score, advancing_ratio, above_ma20_ratio, limit_up_count, failed_limit_count
            FROM market_sentiment_daily
            WHERE trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT 5
            """,
            (trade_date,),
        )
        score_series = [int(row.get("sentiment_score") or 0) for row in history]
        score_avg_5d = (sum(score_series) / len(score_series)) if score_series else None
        previous_score = int(history[1].get("sentiment_score") or 0) if len(history) > 1 else None
        latest_score = int(latest.get("sentiment_score") or 0)

        return {
            "trade_date": latest.get("trade_date"),
            "sample_size": int(latest.get("sample_size") or 0),
            "rising_count": int(latest.get("rising_count") or 0),
            "falling_count": int(latest.get("falling_count") or 0),
            "flat_count": int(latest.get("flat_count") or 0),
            "strong_up_count": int(latest.get("strong_up_count") or 0),
            "strong_down_count": int(latest.get("strong_down_count") or 0),
            "limit_up_count": int(latest.get("limit_up_count") or 0),
            "limit_down_count": int(latest.get("limit_down_count") or 0),
            "failed_limit_count": int(latest.get("failed_limit_count") or 0),
            "above_ma20_count": int(latest.get("above_ma20_count") or 0),
            "advancing_ratio": _to_float(latest.get("advancing_ratio")),
            "above_ma20_ratio": _to_float(latest.get("above_ma20_ratio")),
            "limit_up_ratio": _to_float(latest.get("limit_up_ratio")),
            "failed_limit_ratio": _to_float(latest.get("failed_limit_ratio")),
            "avg_pct_change": _to_float(latest.get("avg_pct_change")),
            "sentiment_score": latest_score,
            "sentiment_label": latest.get("sentiment_label"),
            "summary": latest.get("summary"),
            "score_change_1d": (latest_score - previous_score) if previous_score is not None else None,
            "score_avg_5d": _round_or_none(score_avg_5d, 2),
        }

    def _sector_strength_payload(self, trade_date: str | None, limit: int = 6) -> list[dict]:
        if not trade_date:
            return []
        rows = db.fetchall(
            """
            SELECT
                ss.trade_date,
                ss.sector_name,
                ss.stock_count,
                ss.rising_count,
                ss.limit_up_count,
                ss.avg_pct_change,
                ss.avg_return_5d,
                ss.above_ma20_ratio,
                ss.strength_score,
                ss.leading_stock_code,
                ss.leading_stock_name,
                sfd.trade_date AS fund_flow_trade_date,
                sfd.main_net_inflow,
                sfd.main_net_inflow_ratio
            FROM sector_strength_daily ss
            LEFT JOIN sector_fund_flow_daily sfd
              ON sfd.trade_date = (
                    SELECT MAX(inner_sfd.trade_date)
                    FROM sector_fund_flow_daily inner_sfd
                    WHERE inner_sfd.trade_date <= ss.trade_date
                      AND inner_sfd.sector_type = '行业资金流'
                      AND inner_sfd.sector_name = ss.sector_name
                )
             AND sfd.sector_type = '行业资金流'
             AND sfd.sector_name = ss.sector_name
            WHERE ss.trade_date = ?
            ORDER BY
                ss.strength_score DESC,
                COALESCE(ss.avg_return_5d, -999) DESC,
                COALESCE(ss.avg_pct_change, -999) DESC,
                ss.stock_count DESC,
                ss.sector_name ASC
            LIMIT ?
            """,
            (trade_date, limit),
        )
        return [
            {
                "sector_name": row.get("sector_name"),
                "stock_count": int(row.get("stock_count") or 0),
                "rising_count": int(row.get("rising_count") or 0),
                "limit_up_count": int(row.get("limit_up_count") or 0),
                "avg_pct_change": _to_float(row.get("avg_pct_change")),
                "avg_return_5d": _to_float(row.get("avg_return_5d")),
                "above_ma20_ratio": _to_float(row.get("above_ma20_ratio")),
                "strength_score": int(row.get("strength_score") or 0),
                "leading_stock_code": row.get("leading_stock_code"),
                "leading_stock_name": row.get("leading_stock_name"),
                "fund_flow_trade_date": row.get("fund_flow_trade_date"),
                "main_net_inflow": _to_float(row.get("main_net_inflow")),
                "main_net_inflow_ratio": _to_float(row.get("main_net_inflow_ratio")),
            }
            for row in rows
        ]

    def _event_payload(self, trade_date: str | None, limit: int = 5) -> dict:
        if not trade_date:
            return {
                "leaders": [],
                "failed_limits": [],
                "active_limit_ups": [],
                "max_consecutive_days": 0,
                "consecutive_count": 0,
            }

        leaders = db.fetchall(
            """
            SELECT *
            FROM stock_event_signals_daily
            WHERE trade_date = ?
              AND event_type = 'consecutive_limit_up'
            ORDER BY consecutive_days DESC, rank_no ASC, pct_change DESC, stock_code ASC
            LIMIT ?
            """,
            (trade_date, limit),
        )
        failed_limits = db.fetchall(
            """
            SELECT *
            FROM stock_event_signals_daily
            WHERE trade_date = ?
              AND event_type = 'failed_limit_up'
            ORDER BY rank_no ASC, pct_change DESC, stock_code ASC
            LIMIT ?
            """,
            (trade_date, limit),
        )
        active_limit_ups = db.fetchall(
            """
            SELECT *
            FROM stock_event_signals_daily
            WHERE trade_date = ?
              AND event_type = 'limit_up'
            ORDER BY consecutive_days DESC, rank_no ASC, pct_change DESC, stock_code ASC
            LIMIT ?
            """,
            (trade_date, limit),
        )
        max_streak = self._scalar(
            """
            SELECT MAX(consecutive_days) AS value
            FROM stock_event_signals_daily
            WHERE trade_date = ?
              AND event_type = 'consecutive_limit_up'
            """,
            (trade_date,),
        )
        consecutive_count = self._scalar(
            """
            SELECT COUNT(*) AS value
            FROM stock_event_signals_daily
            WHERE trade_date = ?
              AND event_type = 'consecutive_limit_up'
            """,
            (trade_date,),
        )

        def _normalize(rows: list[dict]) -> list[dict]:
            return [
                {
                    "stock_code": row.get("stock_code"),
                    "stock_name": row.get("stock_name"),
                    "sector_name": row.get("sector_name"),
                    "event_type": row.get("event_type"),
                    "event_label": row.get("event_label"),
                    "event_value": _to_float(row.get("event_value")),
                    "pct_change": _to_float(row.get("pct_change")),
                    "consecutive_days": int(row.get("consecutive_days") or 0),
                    "note": row.get("note"),
                }
                for row in rows
            ]

        return {
            "leaders": _normalize(leaders),
            "failed_limits": _normalize(failed_limits),
            "active_limit_ups": _normalize(active_limit_ups),
            "max_consecutive_days": int(max_streak or 0),
            "consecutive_count": int(consecutive_count or 0),
        }

    def _fund_flow_payload(self, trade_date: str | None, limit: int = 5) -> dict:
        if not trade_date:
            return {
                "market": None,
                "industries": [],
            }

        market = db.fetchone(
            """
            SELECT *
            FROM market_fund_flow_daily
            WHERE trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            (trade_date,),
        )
        industry_rows = db.fetchall(
            """
            WITH latest_flow AS (
                SELECT MAX(trade_date) AS trade_date
                FROM sector_fund_flow_daily
                WHERE trade_date <= ?
                  AND sector_type = '行业资金流'
            )
            SELECT *
            FROM sector_fund_flow_daily
            WHERE trade_date = (SELECT trade_date FROM latest_flow)
              AND sector_type = '行业资金流'
            ORDER BY rank_no ASC, COALESCE(main_net_inflow, -999999999999) DESC, sector_name ASC
            LIMIT ?
            """,
            (trade_date, limit),
        )

        market_payload = None
        if market:
            score = 50.0
            main_ratio = _to_float(market.get("main_net_inflow_ratio")) or 0.0
            sh_pct = _to_float(market.get("sh_pct_change")) or 0.0
            sz_pct = _to_float(market.get("sz_pct_change")) or 0.0
            score += main_ratio * 6
            score += (sh_pct + sz_pct) * 5
            market_payload = {
                "trade_date": market.get("trade_date"),
                "sh_close": _to_float(market.get("sh_close")),
                "sh_pct_change": _to_float(market.get("sh_pct_change")),
                "sz_close": _to_float(market.get("sz_close")),
                "sz_pct_change": _to_float(market.get("sz_pct_change")),
                "main_net_inflow": _to_float(market.get("main_net_inflow")),
                "main_net_inflow_ratio": _to_float(market.get("main_net_inflow_ratio")),
                "super_large_net_inflow": _to_float(market.get("super_large_net_inflow")),
                "super_large_net_inflow_ratio": _to_float(market.get("super_large_net_inflow_ratio")),
                "large_net_inflow": _to_float(market.get("large_net_inflow")),
                "large_net_inflow_ratio": _to_float(market.get("large_net_inflow_ratio")),
                "mid_net_inflow": _to_float(market.get("mid_net_inflow")),
                "mid_net_inflow_ratio": _to_float(market.get("mid_net_inflow_ratio")),
                "small_net_inflow": _to_float(market.get("small_net_inflow")),
                "small_net_inflow_ratio": _to_float(market.get("small_net_inflow_ratio")),
                "flow_score": _clip_int(score),
            }

        return {
            "market": market_payload,
            "industries": [
                {
                    "trade_date": row.get("trade_date"),
                    "sector_name": row.get("sector_name"),
                    "rank_no": int(row.get("rank_no") or 0),
                    "pct_change": _to_float(row.get("pct_change")),
                    "main_net_inflow": _to_float(row.get("main_net_inflow")),
                    "main_net_inflow_ratio": _to_float(row.get("main_net_inflow_ratio")),
                    "leading_stock_name": row.get("leading_stock_name"),
                }
                for row in industry_rows
            ],
        }

    def get_overview(self) -> dict:
        overview_status = self.ensure_overview_current()
        state = self._overview_state()
        trade_date = state.get("trade_date")
        payload = {
            "trade_date": trade_date,
            "latest_market_trade_date": overview_status.get("latest_market_trade_date"),
            "updated_at": state.get("updated_at"),
            "is_current": bool(overview_status.get("is_current")),
            "is_stale": bool(overview_status.get("is_stale")),
            "pending_refresh": bool(overview_status.get("pending_refresh")),
            "refresh_reason": overview_status.get("refresh_reason"),
            "refresh_started_at": overview_status.get("refresh_started_at"),
            "sentiment": self._sentiment_payload(trade_date),
            "benchmarks": self._benchmark_snapshot(trade_date),
            "sectors": self._sector_strength_payload(trade_date),
            "events": self._event_payload(trade_date),
            "fund_flow": self._fund_flow_payload(trade_date),
        }
        sentiment = payload.get("sentiment") or {}
        payload["summary"] = {
            "sentiment_score": int(sentiment.get("sentiment_score") or 0),
            "sentiment_label": sentiment.get("sentiment_label"),
            "sector_count": int(state.get("sector_count") or 0),
            "event_count": int(state.get("event_count") or 0),
            "market_flow_ready": bool(state.get("market_flow_count")),
            "sector_flow_ready": bool(state.get("sector_flow_count")),
        }
        return payload


market_overview_service = MarketOverviewService()
