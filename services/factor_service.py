from __future__ import annotations

import threading
from datetime import datetime

from database.connection import db
from sync.task_dispatcher import spawn_sync_task
from sync.task_locks import get_task_lock_status


SCORECARD_LOOKBACK_BARS = 120
SCORECARD_MODEL_VERSION = 2
MA_FAST_WINDOW = 5
MA_MID_WINDOW = 10
MA_SLOW_WINDOW = 20
RELATIVE_FAST_LOOKBACK = 5
RELATIVE_SLOW_LOOKBACK = 10
BREAKOUT_WINDOW = 20
VOLUME_FAST_WINDOW = 5
VOLUME_SLOW_WINDOW = 20
UP_VOLUME_WINDOW = 10
ATR_WINDOW = 5
ATR_HISTORY_WINDOW = 60
STOP_WINDOW = 5
AMOUNT_WINDOW = 20
WATCHLIST_MIN_SCORE = 6
PRIORITY_MIN_SCORE = 8
EXECUTE_MIN_SCORE = 9
MIN_AVG_AMOUNT = 300_000_000.0
MIN_TURNOVER_RATE = 1.0
MAX_TURNOVER_RATE = 15.0
MAX_EXTENSION_TO_MA5 = 0.06
MAX_STOP_DISTANCE = 0.05
BENCHMARK_CANDIDATES = (
    "399300",
    "000300",
    "399905",
    "000905",
)
SCORECARD_REFRESH_TASK = "scorecard_refresh"


def _to_float(value):
    if value is None or value == "":
        return None
    return float(value)


def _round_or_none(value, digits: int = 4):
    if value is None:
        return None
    return round(value, digits)


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _rolling_average(values: list[float | None], window: int) -> list[float | None]:
    series: list[float | None] = []
    for index in range(len(values)):
        if index + 1 < window:
            series.append(None)
            continue
        window_values = values[index - window + 1 : index + 1]
        if any(value is None for value in window_values):
            series.append(None)
            continue
        series.append(sum(window_values) / window)
    return series


def _return_ratio(values: list[float | None], lookback: int) -> float | None:
    if len(values) <= lookback:
        return None
    latest = values[-1]
    reference = values[-(lookback + 1)]
    if latest is None or reference in (None, 0):
        return None
    return (latest / reference) - 1


def _distance_ratio(value: float | None, reference: float | None) -> float | None:
    if value is None or reference in (None, 0):
        return None
    return (value / reference) - 1


def _max_in_window(values: list[float | None], start: int, end: int) -> float | None:
    window_values = [value for value in values[start:end] if value is not None]
    if not window_values:
        return None
    return max(window_values)


def _min_in_window(values: list[float | None], start: int, end: int) -> float | None:
    window_values = [value for value in values[start:end] if value is not None]
    if not window_values:
        return None
    return min(window_values)


def _percentile_rank(values: list[float | None], target: float | None) -> float | None:
    if target is None:
        return None
    valid_values = [value for value in values if value is not None]
    if not valid_values:
        return None
    less_or_equal = sum(1 for value in valid_values if value <= target)
    return less_or_equal / len(valid_values)


def _score_tier(total_score: int, trigger_ready: bool) -> str:
    if total_score >= EXECUTE_MIN_SCORE and trigger_ready:
        return "优先交易"
    if total_score >= PRIORITY_MIN_SCORE:
        return "重点关注"
    if total_score >= WATCHLIST_MIN_SCORE:
        return "观察"
    return "继续跟踪"


class FactorService:
    """一周内短线机会评分服务"""

    def __init__(self):
        self._refresh_lock = threading.Lock()

    def _scalar(self, sql: str, params: tuple = ()):
        row = db.fetchone(sql, params)
        if not row:
            return None
        return row.get("value")

    def _scorecard_state(self) -> dict:
        return db.fetchone(
            """
            SELECT
                COUNT(*) AS total,
                MAX(trade_date) AS trade_date,
                MAX(updated_at) AS updated_at,
                MAX(COALESCE(model_version, 0)) AS model_version
            FROM stock_factor_scores
            """
        ) or {"total": 0, "trade_date": None, "updated_at": None, "model_version": 0}

    def _latest_kline_trade_date(self) -> str | None:
        return self._scalar("SELECT MAX(trade_date) AS value FROM daily_kline")

    def _is_scorecard_current(self, state: dict, latest_trade_date: str | None) -> bool:
        current_version = int(state.get("model_version") or 0)
        return bool(
            latest_trade_date
            and int(state.get("total") or 0) > 0
            and state.get("trade_date") == latest_trade_date
            and current_version == SCORECARD_MODEL_VERSION
        )

    def _scorecard_status_payload(self, state: dict, latest_trade_date: str | None, **extra) -> dict:
        payload = {
            "trade_date": state.get("trade_date"),
            "latest_market_trade_date": latest_trade_date,
            "updated_at": state.get("updated_at"),
            "total": int(state.get("total") or 0),
            "watchlist_count": self.watchlist_count(min_score=WATCHLIST_MIN_SCORE),
            "is_current": self._is_scorecard_current(state, latest_trade_date),
            "is_stale": bool(int(state.get("total") or 0) > 0) and not self._is_scorecard_current(state, latest_trade_date),
            "pending_refresh": False,
            "refresh_reason": None,
            "refresh_started_at": None,
            "refresh_pid": None,
        }
        payload.update(extra)
        return payload

    def _schedule_scorecard_refresh(self) -> dict:
        refresh_status = get_task_lock_status(SCORECARD_REFRESH_TASK)
        refresh_metadata = refresh_status.get("metadata", {})
        if refresh_status.get("is_running"):
            return {
                "pending_refresh": True,
                "refresh_reason": "scorecard_refresh_running",
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

        result = spawn_sync_task(SCORECARD_REFRESH_TASK)
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

    def _load_benchmark_snapshot(self, latest_trade_date: str | None) -> dict:
        if not latest_trade_date:
            return {}

        sql = """
            SELECT
                dk.trade_date,
                dk.close_price,
                COALESCE(i.index_name, dk.stock_code) AS benchmark_name
            FROM daily_kline dk
            LEFT JOIN indices i
              ON i.index_code = dk.stock_code
            WHERE dk.stock_code = ?
              AND dk.trade_date <= ?
            ORDER BY dk.trade_date DESC
            LIMIT ?
        """
        for benchmark_code in BENCHMARK_CANDIDATES:
            rows = db.fetchall(sql, (benchmark_code, latest_trade_date, SCORECARD_LOOKBACK_BARS))
            if not rows:
                continue
            rows.reverse()
            closes = [_to_float(row.get("close_price")) for row in rows]
            return {
                "benchmark_code": benchmark_code,
                "benchmark_name": rows[-1].get("benchmark_name") or benchmark_code,
                "benchmark_trade_date": rows[-1].get("trade_date"),
                "return_5d": _return_ratio(closes, RELATIVE_FAST_LOOKBACK),
                "return_10d": _return_ratio(closes, RELATIVE_SLOW_LOOKBACK),
            }
        return {}

    def ensure_scorecard_current(self) -> dict:
        latest_trade_date = self._latest_kline_trade_date()
        if not latest_trade_date:
            return {
                "refreshed": False,
                "trade_date": None,
                "latest_market_trade_date": None,
                "updated_at": None,
                "total": 0,
                "watchlist_count": 0,
                "is_current": False,
                "is_stale": False,
                "pending_refresh": False,
                "refresh_reason": "no_kline_data",
                "refresh_started_at": None,
                "refresh_pid": None,
            }

        state = self._scorecard_state()
        if self._is_scorecard_current(state, latest_trade_date):
            return self._scorecard_status_payload(state, latest_trade_date, refreshed=False)

        with self._refresh_lock:
            state = self._scorecard_state()
            if self._is_scorecard_current(state, latest_trade_date):
                return self._scorecard_status_payload(state, latest_trade_date, refreshed=False)

            refresh_status = self._schedule_scorecard_refresh()
            return self._scorecard_status_payload(
                state,
                latest_trade_date,
                refreshed=False,
                pending_refresh=refresh_status.get("pending_refresh", False),
                refresh_reason=refresh_status.get("refresh_reason"),
                refresh_started_at=refresh_status.get("refresh_started_at"),
                refresh_pid=refresh_status.get("refresh_pid"),
            )

    def refresh_scorecard(self) -> dict:
        latest_trade_date = self._latest_kline_trade_date()
        updated_at = datetime.now().isoformat(timespec="seconds")
        benchmark = self._load_benchmark_snapshot(latest_trade_date)
        score_rows: list[dict] = []

        sql = """
            WITH ranked AS (
                SELECT
                    dk.stock_code,
                    s.stock_name,
                    s.market_type,
                    dk.trade_date,
                    dk.close_price,
                    dk.high_price,
                    dk.low_price,
                    dk.pre_close,
                    dk.volume,
                    dk.amount,
                    dk.turnover_rate,
                    ROW_NUMBER() OVER (
                        PARTITION BY dk.stock_code
                        ORDER BY dk.trade_date DESC
                    ) AS rn
                FROM daily_kline dk
                INNER JOIN stocks s
                  ON s.stock_code = dk.stock_code
                WHERE s.status = 1
                  AND COALESCE(s.is_st_current, 0) = 0
            )
            SELECT
                stock_code,
                stock_name,
                market_type,
                trade_date,
                close_price,
                high_price,
                low_price,
                pre_close,
                volume,
                amount,
                turnover_rate
            FROM ranked
            WHERE rn <= ?
            ORDER BY stock_code ASC, trade_date ASC
        """

        with db.get_connection() as conn:
            current_code = None
            stock_rows: list[dict] = []
            for raw_row in conn.execute(sql, (SCORECARD_LOOKBACK_BARS,)):
                row = dict(raw_row)
                stock_code = row["stock_code"]
                if current_code is not None and stock_code != current_code:
                    score_rows.append(self._build_score_row(stock_rows, updated_at, benchmark))
                    stock_rows = []
                current_code = stock_code
                stock_rows.append(row)

            if stock_rows:
                score_rows.append(self._build_score_row(stock_rows, updated_at, benchmark))

            conn.execute("DELETE FROM stock_factor_scores")
            if score_rows:
                conn.executemany(
                    """
                    INSERT INTO stock_factor_scores (
                        stock_code,
                        stock_name,
                        market_type,
                        trade_date,
                        bars_count,
                        model_version,
                        total_score,
                        is_watchlist,
                        tier_label,
                        trigger_ready,
                        setup_score,
                        relative_strength_score,
                        breakout_score,
                        volume_score,
                        risk_score,
                        liquidity_score,
                        close_price,
                        ma5,
                        ma10,
                        ma20,
                        return_5d,
                        return_10d,
                        benchmark_code,
                        benchmark_name,
                        benchmark_return_5d,
                        benchmark_return_10d,
                        excess_return_5d,
                        excess_return_10d,
                        breakout_level,
                        distance_to_breakout,
                        avg_volume_5,
                        avg_volume_20,
                        volume_ratio,
                        latest_volume_ratio,
                        up_day_volume_ratio_10,
                        atr5,
                        atr5_pct,
                        stop_distance,
                        avg_amount_20,
                        turnover_rate,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row["stock_code"],
                            row["stock_name"],
                            row["market_type"],
                            row["trade_date"],
                            row["bars_count"],
                            row["model_version"],
                            row["total_score"],
                            row["is_watchlist"],
                            row["tier_label"],
                            row["trigger_ready"],
                            row["setup_score"],
                            row["relative_strength_score"],
                            row["breakout_score"],
                            row["volume_score"],
                            row["risk_score"],
                            row["liquidity_score"],
                            row["close_price"],
                            row["ma5"],
                            row["ma10"],
                            row["ma20"],
                            row["return_5d"],
                            row["return_10d"],
                            row["benchmark_code"],
                            row["benchmark_name"],
                            row["benchmark_return_5d"],
                            row["benchmark_return_10d"],
                            row["excess_return_5d"],
                            row["excess_return_10d"],
                            row["breakout_level"],
                            row["distance_to_breakout"],
                            row["avg_volume_5"],
                            row["avg_volume_20"],
                            row["volume_ratio"],
                            row["latest_volume_ratio"],
                            row["up_day_volume_ratio_10"],
                            row["atr5"],
                            row["atr5_pct"],
                            row["stop_distance"],
                            row["avg_amount_20"],
                            row["turnover_rate"],
                            row["updated_at"],
                        )
                        for row in score_rows
                    ],
                )

        return {
            "refreshed": True,
            "trade_date": latest_trade_date,
            "updated_at": updated_at,
            "total": len(score_rows),
            "watchlist_count": sum(1 for row in score_rows if row["is_watchlist"]),
        }

    def _build_score_row(self, stock_rows: list[dict], updated_at: str, benchmark: dict) -> dict:
        latest = stock_rows[-1]
        closes = [_to_float(row.get("close_price")) for row in stock_rows]
        highs = [_to_float(row.get("high_price")) for row in stock_rows]
        lows = [_to_float(row.get("low_price")) for row in stock_rows]
        volumes = [_to_float(row.get("volume")) for row in stock_rows]
        amounts = [_to_float(row.get("amount")) for row in stock_rows]
        latest_close = closes[-1]
        latest_volume = volumes[-1]
        latest_turnover_rate = _to_float(latest.get("turnover_rate"))
        latest_pre_close = _to_float(latest.get("pre_close"))
        latest_up_day = bool(
            latest_close is not None
            and latest_pre_close is not None
            and latest_close >= latest_pre_close
        )

        ma5_series = _rolling_average(closes, MA_FAST_WINDOW)
        ma10_series = _rolling_average(closes, MA_MID_WINDOW)
        ma20_series = _rolling_average(closes, MA_SLOW_WINDOW)
        ma5 = ma5_series[-1]
        ma10 = ma10_series[-1]
        ma20 = ma20_series[-1]
        ma10_up = bool(
            ma10 is not None
            and len(ma10_series) > 3
            and ma10_series[-4] is not None
            and ma10 > ma10_series[-4]
        )
        ma20_up = bool(
            ma20 is not None
            and len(ma20_series) > 5
            and ma20_series[-6] is not None
            and ma20 > ma20_series[-6]
        )

        setup_score = 0
        if (
            latest_close is not None
            and ma5 is not None
            and ma10 is not None
            and ma20 is not None
        ):
            if latest_close > ma5 > ma10 > ma20 and ma10_up and ma20_up:
                setup_score = 2
            elif latest_close > ma10 > ma20 and (ma10_up or ma20_up):
                setup_score = 1

        return_5d = _return_ratio(closes, RELATIVE_FAST_LOOKBACK)
        return_10d = _return_ratio(closes, RELATIVE_SLOW_LOOKBACK)
        benchmark_return_5d = benchmark.get("return_5d")
        benchmark_return_10d = benchmark.get("return_10d")
        excess_return_5d = (
            return_5d - benchmark_return_5d
            if return_5d is not None and benchmark_return_5d is not None
            else None
        )
        excess_return_10d = (
            return_10d - benchmark_return_10d
            if return_10d is not None and benchmark_return_10d is not None
            else None
        )

        relative_strength_score = 0
        if (
            return_5d is not None
            and excess_return_5d is not None
            and return_5d >= 0.03
            and excess_return_5d >= 0.02
            and (excess_return_10d is None or excess_return_10d >= 0.03)
        ):
            relative_strength_score = 2
        elif (
            return_5d is not None
            and excess_return_5d is not None
            and return_5d > 0
            and excess_return_5d > 0
        ):
            relative_strength_score = 1
        elif (
            return_10d is not None
            and excess_return_10d is not None
            and return_10d > 0
            and excess_return_10d > 0
        ):
            relative_strength_score = 1

        breakout_level = _max_in_window(highs, -BREAKOUT_WINDOW - 1, -1) if len(highs) > BREAKOUT_WINDOW else None
        distance_to_breakout = _distance_ratio(latest_close, breakout_level)
        distance_to_ma5 = _distance_ratio(latest_close, ma5)

        breakout_score = 0
        if (
            breakout_level is not None
            and distance_to_breakout is not None
            and distance_to_ma5 is not None
            and distance_to_ma5 <= MAX_EXTENSION_TO_MA5
        ):
            if distance_to_breakout >= 0 and setup_score >= 1:
                breakout_score = 2
            elif distance_to_breakout >= -0.03:
                breakout_score = 1

        avg_volume_5 = (
            _average([value for value in volumes[-VOLUME_FAST_WINDOW:] if value is not None])
            if len(volumes) >= VOLUME_FAST_WINDOW
            else None
        )
        avg_volume_20 = (
            _average([value for value in volumes[-VOLUME_SLOW_WINDOW:] if value is not None])
            if len(volumes) >= VOLUME_SLOW_WINDOW
            else None
        )
        volume_ratio = None
        if avg_volume_5 is not None and avg_volume_20 not in (None, 0):
            volume_ratio = avg_volume_5 / avg_volume_20
        latest_volume_ratio = None
        if latest_volume is not None and avg_volume_5 not in (None, 0):
            latest_volume_ratio = latest_volume / avg_volume_5

        recent_rows = stock_rows[-UP_VOLUME_WINDOW:] if len(stock_rows) >= UP_VOLUME_WINDOW else stock_rows
        up_day_volume = 0.0
        total_day_volume = 0.0
        for row in recent_rows:
            volume = _to_float(row.get("volume"))
            close_price = _to_float(row.get("close_price"))
            pre_close = _to_float(row.get("pre_close"))
            if volume is None:
                continue
            total_day_volume += volume
            if close_price is not None and pre_close is not None and close_price >= pre_close:
                up_day_volume += volume
        up_day_volume_ratio_10 = (up_day_volume / total_day_volume) if total_day_volume > 0 else None

        volume_score = 0
        if latest_up_day:
            if (
                latest_volume_ratio is not None
                and latest_volume_ratio >= 1.5
                and (volume_ratio is None or volume_ratio >= 1.05)
            ):
                volume_score = 2
            elif (
                latest_volume_ratio is not None
                and latest_volume_ratio >= 1.2
            ) or (
                volume_ratio is not None
                and volume_ratio >= 1.05
                and up_day_volume_ratio_10 is not None
                and up_day_volume_ratio_10 >= 0.55
            ):
                volume_score = 1

        true_ranges: list[float] = []
        previous_close = None
        for index, row in enumerate(stock_rows):
            high = highs[index]
            low = lows[index]
            close = closes[index]
            if high is None or low is None or close is None:
                previous_close = close
                continue
            reference_close = _to_float(row.get("pre_close"))
            if reference_close is None:
                reference_close = previous_close if previous_close is not None else close
            true_ranges.append(max(high - low, abs(high - reference_close), abs(low - reference_close)))
            previous_close = close

        atr_series: list[float] = []
        if len(true_ranges) >= ATR_WINDOW:
            atr_series = [
                sum(true_ranges[index - ATR_WINDOW + 1 : index + 1]) / ATR_WINDOW
                for index in range(ATR_WINDOW - 1, len(true_ranges))
            ]
        atr5 = atr_series[-1] if atr_series else None
        atr5_pct = (atr5 / latest_close) if atr5 is not None and latest_close not in (None, 0) else None

        low_5 = _min_in_window(lows, -STOP_WINDOW, len(lows)) if len(lows) >= STOP_WINDOW else None
        support_candidates = [
            value
            for value in (ma10, low_5)
            if value is not None and latest_close is not None and value <= latest_close
        ]
        support_level = max(support_candidates) if support_candidates else None
        stop_distance = _distance_ratio(latest_close, support_level)

        risk_score = int(
            atr5_pct is not None
            and atr5_pct <= 0.045
            and stop_distance is not None
            and stop_distance <= MAX_STOP_DISTANCE
            and (distance_to_ma5 is None or distance_to_ma5 <= MAX_EXTENSION_TO_MA5)
        )

        avg_amount_20 = (
            _average([value for value in amounts[-AMOUNT_WINDOW:] if value is not None])
            if len(amounts) >= AMOUNT_WINDOW
            else None
        )
        liquidity_score = int(
            avg_amount_20 is not None
            and avg_amount_20 >= MIN_AVG_AMOUNT
            and latest_turnover_rate is not None
            and MIN_TURNOVER_RATE <= latest_turnover_rate <= MAX_TURNOVER_RATE
        )

        total_score = (
            setup_score
            + relative_strength_score
            + breakout_score
            + volume_score
            + risk_score
            + liquidity_score
        )
        trigger_ready = int(
            total_score >= PRIORITY_MIN_SCORE
            and setup_score >= 1
            and breakout_score >= 1
            and volume_score >= 1
            and risk_score == 1
        )
        tier_label = _score_tier(total_score, bool(trigger_ready))

        return {
            "stock_code": latest["stock_code"],
            "stock_name": latest["stock_name"],
            "market_type": latest["market_type"],
            "trade_date": latest.get("trade_date"),
            "bars_count": len(stock_rows),
            "model_version": SCORECARD_MODEL_VERSION,
            "total_score": total_score,
            "is_watchlist": int(total_score >= WATCHLIST_MIN_SCORE),
            "tier_label": tier_label,
            "trigger_ready": trigger_ready,
            "setup_score": setup_score,
            "relative_strength_score": relative_strength_score,
            "breakout_score": breakout_score,
            "volume_score": volume_score,
            "risk_score": risk_score,
            "liquidity_score": liquidity_score,
            "close_price": _round_or_none(latest_close),
            "ma5": _round_or_none(ma5),
            "ma10": _round_or_none(ma10),
            "ma20": _round_or_none(ma20),
            "return_5d": _round_or_none(return_5d, 6),
            "return_10d": _round_or_none(return_10d, 6),
            "benchmark_code": benchmark.get("benchmark_code"),
            "benchmark_name": benchmark.get("benchmark_name"),
            "benchmark_return_5d": _round_or_none(benchmark_return_5d, 6),
            "benchmark_return_10d": _round_or_none(benchmark_return_10d, 6),
            "excess_return_5d": _round_or_none(excess_return_5d, 6),
            "excess_return_10d": _round_or_none(excess_return_10d, 6),
            "breakout_level": _round_or_none(breakout_level),
            "distance_to_breakout": _round_or_none(distance_to_breakout, 6),
            "avg_volume_5": _round_or_none(avg_volume_5),
            "avg_volume_20": _round_or_none(avg_volume_20),
            "volume_ratio": _round_or_none(volume_ratio, 6),
            "latest_volume_ratio": _round_or_none(latest_volume_ratio, 6),
            "up_day_volume_ratio_10": _round_or_none(up_day_volume_ratio_10, 6),
            "atr5": _round_or_none(atr5, 6),
            "atr5_pct": _round_or_none(atr5_pct, 6),
            "stop_distance": _round_or_none(stop_distance, 6),
            "avg_amount_20": _round_or_none(avg_amount_20),
            "turnover_rate": _round_or_none(latest_turnover_rate, 6),
            "updated_at": updated_at,
        }

    def watchlist_count(self, min_score: int = WATCHLIST_MIN_SCORE) -> int:
        value = self._scalar(
            """
            SELECT COUNT(*) AS value
            FROM stock_factor_scores
            WHERE total_score >= ?
              AND is_watchlist = 1
            """,
            (min_score,),
        )
        return int(value or 0)

    def get_watchlist(self, limit: int = 12, min_score: int = WATCHLIST_MIN_SCORE) -> dict:
        scorecard_status = self.ensure_scorecard_current()
        rows = db.fetchall(
            """
            SELECT
                stock_code,
                stock_name,
                market_type,
                trade_date,
                total_score,
                is_watchlist,
                tier_label,
                trigger_ready,
                setup_score,
                relative_strength_score,
                breakout_score,
                volume_score,
                risk_score,
                liquidity_score,
                return_5d,
                excess_return_5d,
                distance_to_breakout,
                volume_ratio,
                turnover_rate
            FROM stock_factor_scores
            WHERE total_score >= ?
              AND is_watchlist = 1
            ORDER BY
                total_score DESC,
                trigger_ready DESC,
                breakout_score DESC,
                relative_strength_score DESC,
                COALESCE(excess_return_5d, -999) DESC,
                COALESCE(distance_to_breakout, -999) DESC,
                stock_code ASC
            LIMIT ?
            """,
            (min_score, limit),
        )
        state = self._scorecard_state()
        return {
            "trade_date": state.get("trade_date"),
            "latest_market_trade_date": scorecard_status.get("latest_market_trade_date"),
            "updated_at": state.get("updated_at"),
            "min_score": min_score,
            "count": self.watchlist_count(min_score=min_score),
            "is_current": bool(scorecard_status.get("is_current")),
            "is_stale": bool(scorecard_status.get("is_stale")),
            "pending_refresh": bool(scorecard_status.get("pending_refresh")),
            "refresh_reason": scorecard_status.get("refresh_reason"),
            "refresh_started_at": scorecard_status.get("refresh_started_at"),
            "items": [self._summary_payload(row) for row in rows],
        }

    def get_stock_score(self, stock_code: str) -> dict | None:
        scorecard_status = self.ensure_scorecard_current()
        row = db.fetchone(
            """
            SELECT *
            FROM stock_factor_scores
            WHERE stock_code = ?
            """,
            (stock_code,),
        )
        if not row:
            if scorecard_status.get("pending_refresh") or int(scorecard_status.get("total") or 0) == 0:
                stock = db.fetchone(
                    """
                    SELECT stock_code, stock_name, market_type
                    FROM stocks
                    WHERE stock_code = ?
                    """,
                    (stock_code,),
                ) or {"stock_code": stock_code, "stock_name": stock_code, "market_type": None}
                return {
                    "stock_code": stock.get("stock_code") or stock_code,
                    "stock_name": stock.get("stock_name") or stock_code,
                    "market_type": stock.get("market_type"),
                    "trade_date": scorecard_status.get("trade_date"),
                    "latest_market_trade_date": scorecard_status.get("latest_market_trade_date"),
                    "updated_at": scorecard_status.get("updated_at"),
                    "pending_refresh": True,
                    "refresh_reason": scorecard_status.get("refresh_reason"),
                    "refresh_started_at": scorecard_status.get("refresh_started_at"),
                    "message": "短线机会评分正在后台刷新，请稍后重试。",
                }
            return None
        payload = self._detail_payload(row)
        payload.update(
            {
                "latest_market_trade_date": scorecard_status.get("latest_market_trade_date"),
                "is_current": bool(scorecard_status.get("is_current")),
                "is_stale": bool(scorecard_status.get("is_stale")),
                "pending_refresh": bool(scorecard_status.get("pending_refresh")),
                "refresh_reason": scorecard_status.get("refresh_reason"),
                "refresh_started_at": scorecard_status.get("refresh_started_at"),
            }
        )
        return payload

    def _summary_payload(self, row: dict) -> dict:
        return {
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name"],
            "market_type": row["market_type"],
            "trade_date": row.get("trade_date"),
            "total_score": int(row.get("total_score") or 0),
            "is_watchlist": bool(row.get("is_watchlist")),
            "tier_label": row.get("tier_label") or _score_tier(int(row.get("total_score") or 0), bool(row.get("trigger_ready"))),
            "trigger_ready": bool(row.get("trigger_ready")),
            "return_5d": _to_float(row.get("return_5d")),
            "excess_return_5d": _to_float(row.get("excess_return_5d")),
            "distance_to_breakout": _to_float(row.get("distance_to_breakout")),
            "volume_ratio": _to_float(row.get("volume_ratio")),
            "turnover_rate": _to_float(row.get("turnover_rate")),
            "passed_factors": self._passed_factor_labels(row),
        }

    def _detail_payload(self, row: dict) -> dict:
        total_score = int(row.get("total_score") or 0)
        trigger_ready = bool(row.get("trigger_ready"))
        breakout_level = _to_float(row.get("breakout_level"))
        distance_to_breakout = _to_float(row.get("distance_to_breakout"))
        trigger_text = "等待形态形成"
        if distance_to_breakout is not None and breakout_level is not None:
            if distance_to_breakout >= 0:
                trigger_text = f"已越过 20 日突破位 {breakout_level:.2f}"
            else:
                trigger_text = f"距离 20 日突破位 {breakout_level:.2f} 还有 {abs(distance_to_breakout) * 100:.1f}%"
        if trigger_ready:
            trigger_text = "结构已成熟，若次日继续确认可优先处理"

        return {
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name"],
            "market_type": row["market_type"],
            "trade_date": row.get("trade_date"),
            "bars_count": int(row.get("bars_count") or 0),
            "total_score": total_score,
            "max_score": 10,
            "is_watchlist": bool(row.get("is_watchlist")),
            "tier_label": row.get("tier_label") or _score_tier(total_score, trigger_ready),
            "trigger_ready": trigger_ready,
            "trigger_text": trigger_text,
            "updated_at": row.get("updated_at"),
            "factors": {
                "setup": {
                    "label": "背景趋势",
                    "score": int(row.get("setup_score") or 0),
                    "max_score": 2,
                    "close_price": _to_float(row.get("close_price")),
                    "ma5": _to_float(row.get("ma5")),
                    "ma10": _to_float(row.get("ma10")),
                    "ma20": _to_float(row.get("ma20")),
                },
                "relative_strength": {
                    "label": "相对强弱",
                    "score": int(row.get("relative_strength_score") or 0),
                    "max_score": 2,
                    "benchmark_name": row.get("benchmark_name"),
                    "return_5d": _to_float(row.get("return_5d")),
                    "return_10d": _to_float(row.get("return_10d")),
                    "benchmark_return_5d": _to_float(row.get("benchmark_return_5d")),
                    "benchmark_return_10d": _to_float(row.get("benchmark_return_10d")),
                    "excess_return_5d": _to_float(row.get("excess_return_5d")),
                    "excess_return_10d": _to_float(row.get("excess_return_10d")),
                },
                "breakout": {
                    "label": "突破位置",
                    "score": int(row.get("breakout_score") or 0),
                    "max_score": 2,
                    "breakout_level": breakout_level,
                    "distance_to_breakout": distance_to_breakout,
                    "distance_to_ma5": _distance_ratio(_to_float(row.get("close_price")), _to_float(row.get("ma5"))),
                },
                "volume_trigger": {
                    "label": "量价触发",
                    "score": int(row.get("volume_score") or 0),
                    "max_score": 2,
                    "avg_volume_5": _to_float(row.get("avg_volume_5")),
                    "avg_volume_20": _to_float(row.get("avg_volume_20")),
                    "volume_ratio": _to_float(row.get("volume_ratio")),
                    "latest_volume_ratio": _to_float(row.get("latest_volume_ratio")),
                    "up_day_volume_ratio_10": _to_float(row.get("up_day_volume_ratio_10")),
                },
                "risk_control": {
                    "label": "风险回撤",
                    "score": int(row.get("risk_score") or 0),
                    "max_score": 1,
                    "atr5": _to_float(row.get("atr5")),
                    "atr5_pct": _to_float(row.get("atr5_pct")),
                    "stop_distance": _to_float(row.get("stop_distance")),
                },
                "liquidity": {
                    "label": "流动性",
                    "score": int(row.get("liquidity_score") or 0),
                    "max_score": 1,
                    "avg_amount_20": _to_float(row.get("avg_amount_20")),
                    "turnover_rate": _to_float(row.get("turnover_rate")),
                },
            },
        }

    def _passed_factor_labels(self, row: dict) -> list[str]:
        labels = []
        if int(row.get("setup_score") or 0) > 0:
            labels.append("背景趋势")
        if int(row.get("relative_strength_score") or 0) > 0:
            labels.append("相对强弱")
        if int(row.get("breakout_score") or 0) > 0:
            labels.append("突破位置")
        if int(row.get("volume_score") or 0) > 0:
            labels.append("量价触发")
        if int(row.get("risk_score") or 0) > 0:
            labels.append("风险回撤")
        if int(row.get("liquidity_score") or 0) > 0:
            labels.append("流动性")
        return labels


factor_service = FactorService()
