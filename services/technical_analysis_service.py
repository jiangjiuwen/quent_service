from __future__ import annotations

import math

from database.connection import db


TECHNICAL_LOOKBACK_BARS = 500
MA_FAST_WINDOW = 20
MA_MID_WINDOW = 60
MA_SLOW_WINDOW = 120
ATR_WINDOW = 20
VOLUME_FAST_WINDOW = 5
VOLUME_SLOW_WINDOW = 20
SHORT_HIGH_WINDOW = 20
BREAKOUT_WINDOW = 60
POSITION_WINDOW = 120
ATR_HISTORY_WINDOW = 120
BENCHMARK_CANDIDATES = (
    "399300",
    "000300",
    "399905",
    "000905",
)


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


def _stddev(values: list[float]) -> float | None:
    if not values:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def _return_ratio(values: list[float | None], lookback: int) -> float | None:
    if len(values) <= lookback:
        return None
    latest = values[-1]
    reference = values[-(lookback + 1)]
    if latest is None or reference in (None, 0):
        return None
    return (latest / reference) - 1


def _range_extreme(values: list[float | None], window: int, mode: str) -> float | None:
    if len(values) < window:
        return None
    window_values = [value for value in values[-window:] if value is not None]
    if not window_values:
        return None
    if mode == "max":
        return max(window_values)
    return min(window_values)


def _percentile_rank(values: list[float | None], target: float | None) -> float | None:
    if target is None:
        return None
    valid_values = [value for value in values if value is not None]
    if not valid_values:
        return None
    less_or_equal = sum(1 for value in valid_values if value <= target)
    return less_or_equal / len(valid_values)


def _position_ratio(value: float | None, low: float | None, high: float | None) -> float | None:
    if value is None or low is None or high is None or high <= low:
        return None
    return (value - low) / (high - low)


def _distance_ratio(value: float | None, reference: float | None) -> float | None:
    if value is None or reference in (None, 0):
        return None
    return (value / reference) - 1


def _nearest_level(current_price: float | None, levels: list[tuple[str, float | None]], direction: str) -> tuple[str | None, float | None]:
    if current_price is None:
        return (None, None)
    if direction == "below":
        candidates = [(label, value) for label, value in levels if value is not None and value <= current_price]
        if not candidates:
            return (None, None)
        return max(candidates, key=lambda item: item[1])

    candidates = [(label, value) for label, value in levels if value is not None and value >= current_price]
    if not candidates:
        return (None, None)
    return min(candidates, key=lambda item: item[1])


def _format_price(value: float | None) -> str:
    if value is None:
        return "--"
    return f"{value:.2f}"


class TechnicalAnalysisService:
    """单股技术面深度分析服务"""

    def get_stock_analysis(self, stock_code: str) -> dict | None:
        sql = """
            SELECT
                dk.stock_code,
                COALESCE(s.stock_name, dk.stock_code) AS stock_name,
                s.market_type,
                s.industry_code,
                ind.industry_name,
                dk.trade_date,
                dk.open_price,
                dk.high_price,
                dk.low_price,
                dk.close_price,
                dk.pre_close,
                dk.pct_change,
                dk.volume,
                dk.amount,
                dk.turnover_rate,
                dk.pe_ratio,
                dk.pb_ratio
            FROM daily_kline dk
            LEFT JOIN stocks s
              ON s.stock_code = dk.stock_code
            LEFT JOIN industries ind
              ON ind.industry_code = s.industry_code
            WHERE dk.stock_code = ?
            ORDER BY dk.trade_date DESC
            LIMIT ?
        """

        with db.get_connection() as conn:
            raw_rows = conn.execute(sql, (stock_code, TECHNICAL_LOOKBACK_BARS)).fetchall()

        if not raw_rows:
            return None

        rows = [dict(row) for row in raw_rows]
        rows.reverse()

        closes = [_to_float(row.get("close_price")) for row in rows]
        highs = [_to_float(row.get("high_price")) for row in rows]
        lows = [_to_float(row.get("low_price")) for row in rows]
        volumes = [_to_float(row.get("volume")) for row in rows]
        latest = rows[-1]
        latest_close = closes[-1]

        ma20_series = _rolling_average(closes, MA_FAST_WINDOW)
        ma60_series = _rolling_average(closes, MA_MID_WINDOW)
        ma120_series = _rolling_average(closes, MA_SLOW_WINDOW)
        ma20 = ma20_series[-1]
        ma60 = ma60_series[-1]
        ma120 = ma120_series[-1]

        ma20_slope_up = bool(ma20 is not None and len(ma20_series) > 5 and ma20_series[-6] is not None and ma20 > ma20_series[-6])
        ma60_slope_up = bool(ma60 is not None and len(ma60_series) > 10 and ma60_series[-11] is not None and ma60 > ma60_series[-11])

        return_5d = _return_ratio(closes, VOLUME_FAST_WINDOW)
        return_20d = _return_ratio(closes, MA_FAST_WINDOW)
        return_60d = _return_ratio(closes, MA_MID_WINDOW)
        return_120d = _return_ratio(closes, MA_SLOW_WINDOW)

        avg_volume_5 = _average([value for value in volumes[-VOLUME_FAST_WINDOW:] if value is not None]) if len(volumes) >= VOLUME_FAST_WINDOW else None
        avg_volume_20 = _average([value for value in volumes[-VOLUME_SLOW_WINDOW:] if value is not None]) if len(volumes) >= VOLUME_SLOW_WINDOW else None
        volume_ratio = None
        if avg_volume_5 is not None and avg_volume_20 not in (None, 0):
            volume_ratio = avg_volume_5 / avg_volume_20

        recent_rows_20 = rows[-VOLUME_SLOW_WINDOW:] if len(rows) >= VOLUME_SLOW_WINDOW else rows
        up_day_volume = 0.0
        total_day_volume = 0.0
        for row in recent_rows_20:
            volume = _to_float(row.get("volume"))
            close_price = _to_float(row.get("close_price"))
            pre_close = _to_float(row.get("pre_close"))
            if volume is None:
                continue
            total_day_volume += volume
            if close_price is not None and pre_close is not None and close_price >= pre_close:
                up_day_volume += volume
        up_day_volume_ratio = (up_day_volume / total_day_volume) if total_day_volume > 0 else None

        true_ranges: list[float] = []
        previous_close = None
        for index, row in enumerate(rows):
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
        atr20 = atr_series[-1] if atr_series else None
        atr20_hist_avg = _average(atr_series)
        atr20_pct = (atr20 / latest_close) if atr20 is not None and latest_close not in (None, 0) else None
        atr_percentile = _percentile_rank(atr_series[-ATR_HISTORY_WINDOW:], atr20)

        boll_width = None
        if len(closes) >= MA_FAST_WINDOW:
            boll_window = closes[-MA_FAST_WINDOW:]
            if all(value is not None for value in boll_window):
                std = _stddev(boll_window)
                middle = _average(boll_window)
                if std is not None and middle not in (None, 0):
                    boll_width = (4 * std) / middle

        high_20 = _range_extreme(highs, SHORT_HIGH_WINDOW, "max")
        low_20 = _range_extreme(lows, SHORT_HIGH_WINDOW, "min")
        high_60 = _range_extreme(highs, BREAKOUT_WINDOW, "max")
        low_60 = _range_extreme(lows, BREAKOUT_WINDOW, "min")
        high_120 = _range_extreme(highs, POSITION_WINDOW, "max")
        low_120 = _range_extreme(lows, POSITION_WINDOW, "min")
        position_120d = _position_ratio(latest_close, low_120, high_120)
        distance_to_ma20 = _distance_ratio(latest_close, ma20)
        distance_to_60d_high = _distance_ratio(latest_close, high_60)
        distance_to_120d_high = _distance_ratio(latest_close, high_120)
        reference_dates = {
            "20d": rows[-(MA_FAST_WINDOW + 1)]["trade_date"] if len(rows) > MA_FAST_WINDOW else None,
            "60d": rows[-(MA_MID_WINDOW + 1)]["trade_date"] if len(rows) > MA_MID_WINDOW else None,
            "120d": rows[-(MA_SLOW_WINDOW + 1)]["trade_date"] if len(rows) > MA_SLOW_WINDOW else None,
        }
        benchmark_strength = self._load_benchmark_strength(
            latest_trade_date=latest.get("trade_date"),
            stock_return_20d=return_20d,
            stock_return_60d=return_60d,
            stock_return_120d=return_120d,
        )
        industry_strength = self._load_industry_strength(
            industry_code=latest.get("industry_code"),
            industry_name=latest.get("industry_name"),
            latest_trade_date=latest.get("trade_date"),
            reference_dates=reference_dates,
            stock_return_20d=return_20d,
            stock_return_60d=return_60d,
            stock_return_120d=return_120d,
        )

        close_above_ma20 = bool(latest_close is not None and ma20 is not None and latest_close > ma20)
        ma20_above_ma60 = bool(ma20 is not None and ma60 is not None and ma20 > ma60)
        ma60_above_ma120 = bool(ma60 is not None and ma120 is not None and ma60 > ma120)
        breakout_ready = bool(
            close_above_ma20
            and ma20_above_ma60
            and volume_ratio is not None
            and volume_ratio >= 1.0
            and distance_to_60d_high is not None
            and distance_to_60d_high >= -0.03
        )
        pullback_on_low_volume = bool(
            return_5d is not None
            and return_5d < 0
            and volume_ratio is not None
            and volume_ratio < 1.0
        )

        trend_label = self._trend_label(
            close_above_ma20=close_above_ma20,
            ma20_above_ma60=ma20_above_ma60,
            ma60_above_ma120=ma60_above_ma120,
            ma20_slope_up=ma20_slope_up,
            ma60_slope_up=ma60_slope_up,
        )
        strength_label = self._strength_label(
            return_20d=return_20d,
            return_60d=return_60d,
            distance_to_60d_high=distance_to_60d_high,
            position_120d=position_120d,
        )
        relative_strength_label = self._relative_strength_label(
            benchmark_excess_20d=benchmark_strength.get("excess_return_20d"),
            benchmark_excess_60d=benchmark_strength.get("excess_return_60d"),
            industry_excess_20d=industry_strength.get("excess_return_20d"),
            industry_excess_60d=industry_strength.get("excess_return_60d"),
            industry_percentile_20d=industry_strength.get("industry_percentile_20d"),
        )
        volume_label = self._volume_label(
            breakout_ready=breakout_ready,
            volume_ratio=volume_ratio,
            up_day_volume_ratio=up_day_volume_ratio,
            pullback_on_low_volume=pullback_on_low_volume,
        )
        risk_label = self._risk_label(
            atr20=atr20,
            atr20_hist_avg=atr20_hist_avg,
            atr_percentile=atr_percentile,
            distance_to_ma20=distance_to_ma20,
        )

        support_label, support_level = _nearest_level(
            latest_close,
            [
                ("MA20", ma20),
                ("MA60", ma60),
                ("20日低点", low_20),
                ("60日低点", low_60),
            ],
            "below",
        )
        resistance_label, resistance_level = _nearest_level(
            latest_close,
            [
                ("20日高点", high_20),
                ("60日高点", high_60),
                ("120日高点", high_120),
            ],
            "above",
        )

        signal_score, score_reasons = self._signal_score(
            close_above_ma20=close_above_ma20,
            ma20_above_ma60=ma20_above_ma60,
            ma20_slope_up=ma20_slope_up,
            return_20d=return_20d,
            return_60d=return_60d,
            position_120d=position_120d,
            volume_ratio=volume_ratio,
            up_day_volume_ratio=up_day_volume_ratio,
            atr20=atr20,
            atr20_hist_avg=atr20_hist_avg,
            atr_percentile=atr_percentile,
            distance_to_60d_high=distance_to_60d_high,
        )
        bias, verdict = self._signal_conclusion(signal_score, breakout_ready, len(rows))
        trigger = self._trigger_text(resistance_label, resistance_level, ma20)
        invalidation = self._invalidation_text(support_label, support_level, ma20, low_20, latest_close)

        trend_summary = "收盘已站上 MA20，且 MA20/MA60 保持多头顺序。" if close_above_ma20 and ma20_above_ma60 else "均线顺序尚未完全走成，多看结构确认。"
        if ma20_slope_up or ma60_slope_up:
            trend_summary += f" 其中 {'MA20' if ma20_slope_up else 'MA60'} 继续上行。"

        strength_summary = "当前版本先按个股自身区间强弱判断。"
        if return_20d is not None and return_60d is not None:
            strength_summary = f"近20日 {return_20d * 100:.1f}% 、近60日 {return_60d * 100:.1f}%，"
            if distance_to_60d_high is not None:
                strength_summary += f"距离60日高点 {distance_to_60d_high * 100:.1f}%。"
            else:
                strength_summary += "继续观察是否逼近阶段高点。"

        relative_parts = []
        benchmark_name = benchmark_strength.get("benchmark_name")
        benchmark_excess_20d = benchmark_strength.get("excess_return_20d")
        benchmark_excess_60d = benchmark_strength.get("excess_return_60d")
        if benchmark_name and benchmark_excess_20d is not None:
            relative_parts.append(f"近20日相对{benchmark_name} {benchmark_excess_20d * 100:+.1f}%")
        if benchmark_name and benchmark_excess_60d is not None:
            relative_parts.append(f"近60日相对{benchmark_name} {benchmark_excess_60d * 100:+.1f}%")

        industry_excess_20d = industry_strength.get("excess_return_20d")
        industry_excess_60d = industry_strength.get("excess_return_60d")
        if industry_excess_20d is not None:
            relative_parts.append(f"近20日相对行业 {industry_excess_20d * 100:+.1f}%")
        if industry_excess_60d is not None:
            relative_parts.append(f"近60日相对行业 {industry_excess_60d * 100:+.1f}%")

        relative_strength_summary = "相对强弱数据不足。"
        if relative_parts:
            relative_strength_summary = "，".join(relative_parts) + "。"
            if industry_strength.get("industry_percentile_20d") is not None:
                relative_strength_summary += f" 行业对比基于同业等权收益，20日强度位于行业 {industry_strength['industry_percentile_20d'] * 100:.0f}% 分位。"

        volume_summary = "量能暂未明显放大。"
        if breakout_ready:
            volume_summary = "价格已经逼近阶段高点，若继续放量更容易形成有效突破。"
        elif pullback_on_low_volume:
            volume_summary = "最近回踩伴随缩量，抛压暂不算重。"
        elif up_day_volume_ratio is not None and up_day_volume_ratio >= 0.55:
            volume_summary = "近20日上涨日成交量占优，量价配合偏正向。"

        risk_summary = "波动处于中性区间。"
        if atr20 is not None and atr20_hist_avg is not None and atr20 < atr20_hist_avg:
            risk_summary = "当前 ATR 低于历史均值，波动相对可控。"
        elif atr_percentile is not None and atr_percentile >= 0.8:
            risk_summary = "ATR 位于近阶段高分位，短线波动偏大。"

        support_text = f"{support_label} {_format_price(support_level)}" if support_level is not None and support_label else None
        resistance_text = f"{resistance_label} {_format_price(resistance_level)}" if resistance_level is not None and resistance_label else None
        if support_text and resistance_text:
            key_level_summary = f"下方先看 {support_text}，上方先看 {resistance_text}。"
        elif support_text:
            key_level_summary = f"当前下方先看 {support_text}。"
        elif resistance_text:
            key_level_summary = f"当前上方先看 {resistance_text}。"
        else:
            key_level_summary = "关键位不足，等待更多样本。"

        return {
            "stock_code": latest["stock_code"],
            "stock_name": latest.get("stock_name") or latest["stock_code"],
            "market_type": latest.get("market_type"),
            "trade_date": latest.get("trade_date"),
            "bars_count": len(rows),
            "trend": {
                "label": trend_label,
                "summary": trend_summary,
                "close_price": _round_or_none(latest_close),
                "close_above_ma20": close_above_ma20,
                "ma20": _round_or_none(ma20),
                "ma60": _round_or_none(ma60),
                "ma120": _round_or_none(ma120),
                "ma20_above_ma60": ma20_above_ma60,
                "ma60_above_ma120": ma60_above_ma120,
                "ma20_slope_up": ma20_slope_up,
                "ma60_slope_up": ma60_slope_up,
                "distance_to_ma20": _round_or_none(distance_to_ma20, 6),
            },
            "strength": {
                "label": strength_label,
                "summary": strength_summary,
                "return_20d": _round_or_none(return_20d, 6),
                "return_60d": _round_or_none(return_60d, 6),
                "return_120d": _round_or_none(return_120d, 6),
                "position_120d": _round_or_none(position_120d, 6),
                "distance_to_60d_high": _round_or_none(distance_to_60d_high, 6),
                "distance_to_120d_high": _round_or_none(distance_to_120d_high, 6),
            },
            "relative_strength": {
                "label": relative_strength_label,
                "summary": relative_strength_summary,
                "benchmark_code": benchmark_strength.get("benchmark_code"),
                "benchmark_name": benchmark_strength.get("benchmark_name"),
                "benchmark_trade_date": benchmark_strength.get("benchmark_trade_date"),
                "benchmark_return_20d": _round_or_none(benchmark_strength.get("benchmark_return_20d"), 6),
                "benchmark_return_60d": _round_or_none(benchmark_strength.get("benchmark_return_60d"), 6),
                "benchmark_return_120d": _round_or_none(benchmark_strength.get("benchmark_return_120d"), 6),
                "excess_return_20d": _round_or_none(benchmark_strength.get("excess_return_20d"), 6),
                "excess_return_60d": _round_or_none(benchmark_strength.get("excess_return_60d"), 6),
                "excess_return_120d": _round_or_none(benchmark_strength.get("excess_return_120d"), 6),
                "industry_code": industry_strength.get("industry_code"),
                "industry_name": industry_strength.get("industry_name"),
                "industry_peer_count": industry_strength.get("industry_peer_count"),
                "industry_return_20d": _round_or_none(industry_strength.get("industry_return_20d"), 6),
                "industry_return_60d": _round_or_none(industry_strength.get("industry_return_60d"), 6),
                "industry_return_120d": _round_or_none(industry_strength.get("industry_return_120d"), 6),
                "industry_excess_return_20d": _round_or_none(industry_strength.get("excess_return_20d"), 6),
                "industry_excess_return_60d": _round_or_none(industry_strength.get("excess_return_60d"), 6),
                "industry_excess_return_120d": _round_or_none(industry_strength.get("excess_return_120d"), 6),
                "industry_percentile_20d": _round_or_none(industry_strength.get("industry_percentile_20d"), 6),
            },
            "volume_price": {
                "label": volume_label,
                "summary": volume_summary,
                "avg_volume_5": _round_or_none(avg_volume_5),
                "avg_volume_20": _round_or_none(avg_volume_20),
                "volume_ratio": _round_or_none(volume_ratio, 6),
                "up_day_volume_ratio_20d": _round_or_none(up_day_volume_ratio, 6),
                "return_5d": _round_or_none(return_5d, 6),
                "breakout_ready": breakout_ready,
                "pullback_on_low_volume": pullback_on_low_volume,
            },
            "volatility_risk": {
                "label": risk_label,
                "summary": risk_summary,
                "atr20": _round_or_none(atr20, 6),
                "atr20_hist_avg": _round_or_none(atr20_hist_avg, 6),
                "atr20_pct": _round_or_none(atr20_pct, 6),
                "atr_percentile_120d": _round_or_none(atr_percentile, 6),
                "boll_width_20d": _round_or_none(boll_width, 6),
                "distance_to_ma20": _round_or_none(distance_to_ma20, 6),
            },
            "key_levels": {
                "label": "关键位置",
                "summary": key_level_summary,
                "support_label": support_label,
                "support_level": _round_or_none(support_level),
                "resistance_label": resistance_label,
                "resistance_level": _round_or_none(resistance_level),
                "low_20d": _round_or_none(low_20),
                "low_60d": _round_or_none(low_60),
                "high_20d": _round_or_none(high_20),
                "high_60d": _round_or_none(high_60),
                "high_120d": _round_or_none(high_120),
            },
            "signal_summary": {
                "score": signal_score,
                "max_score": 5,
                "bias": bias,
                "verdict": verdict,
                "trigger": trigger,
                "invalidation": invalidation,
                "reasons": score_reasons,
            },
        }

    def _trend_label(
        self,
        *,
        close_above_ma20: bool,
        ma20_above_ma60: bool,
        ma60_above_ma120: bool,
        ma20_slope_up: bool,
        ma60_slope_up: bool,
    ) -> str:
        if close_above_ma20 and ma20_above_ma60 and ma60_above_ma120 and ma20_slope_up and ma60_slope_up:
            return "多头趋势"
        if close_above_ma20 and ma20_above_ma60 and ma20_slope_up:
            return "上升趋势"
        if close_above_ma20 and ma20_above_ma60:
            return "趋势偏强"
        if close_above_ma20 or ma20_slope_up:
            return "短线修复"
        return "趋势偏弱"

    def _strength_label(
        self,
        *,
        return_20d: float | None,
        return_60d: float | None,
        distance_to_60d_high: float | None,
        position_120d: float | None,
    ) -> str:
        if (
            return_20d is not None
            and return_60d is not None
            and return_20d > 0.08
            and return_60d > 0.15
            and distance_to_60d_high is not None
            and distance_to_60d_high >= -0.05
        ):
            return "强势逼近前高"
        if return_20d is not None and return_60d is not None and return_20d > 0 and return_60d > 0:
            return "中短期偏强"
        if return_60d is not None and return_60d > 0 and position_120d is not None and position_120d >= 0.6:
            return "中期偏强"
        if return_20d is not None and return_20d > 0:
            return "短线修复"
        return "动能一般"

    def _relative_strength_label(
        self,
        *,
        benchmark_excess_20d: float | None,
        benchmark_excess_60d: float | None,
        industry_excess_20d: float | None,
        industry_excess_60d: float | None,
        industry_percentile_20d: float | None,
    ) -> str:
        if (
            benchmark_excess_20d is not None
            and benchmark_excess_60d is not None
            and industry_excess_20d is not None
            and benchmark_excess_20d > 0.03
            and benchmark_excess_60d > 0.05
            and industry_excess_20d > 0
        ):
            return "显著跑赢基准"
        if (
            benchmark_excess_20d is not None
            and benchmark_excess_20d > 0
            and industry_excess_20d is not None
            and industry_excess_20d > 0
        ):
            return "相对强于市场"
        if industry_percentile_20d is not None and industry_percentile_20d >= 0.7:
            return "行业内偏强"
        if (
            benchmark_excess_20d is not None
            and benchmark_excess_20d < 0
            and industry_excess_20d is not None
            and industry_excess_20d < 0
        ):
            return "相对偏弱"
        return "相对强弱中性"

    def _volume_label(
        self,
        *,
        breakout_ready: bool,
        volume_ratio: float | None,
        up_day_volume_ratio: float | None,
        pullback_on_low_volume: bool,
    ) -> str:
        if breakout_ready and volume_ratio is not None and volume_ratio >= 1.2:
            return "放量待突破"
        if pullback_on_low_volume:
            return "回踩缩量"
        if up_day_volume_ratio is not None and up_day_volume_ratio >= 0.55 and volume_ratio is not None and volume_ratio >= 1.0:
            return "量价配合良好"
        if volume_ratio is not None and volume_ratio < 0.85:
            return "缩量整理"
        return "量能中性"

    def _risk_label(
        self,
        *,
        atr20: float | None,
        atr20_hist_avg: float | None,
        atr_percentile: float | None,
        distance_to_ma20: float | None,
    ) -> str:
        if (
            atr20 is not None
            and atr20_hist_avg is not None
            and atr20 < atr20_hist_avg
            and distance_to_ma20 is not None
            and abs(distance_to_ma20) <= 0.08
        ):
            return "波动可控"
        if atr_percentile is not None and atr_percentile >= 0.8:
            return "波动偏大"
        return "波动中性"

    def _load_benchmark_strength(
        self,
        *,
        latest_trade_date: str | None,
        stock_return_20d: float | None,
        stock_return_60d: float | None,
        stock_return_120d: float | None,
    ) -> dict:
        if not latest_trade_date:
            return {}

        for benchmark_code in BENCHMARK_CANDIDATES:
            rows = db.fetchall(
                """
                SELECT
                    dk.stock_code,
                    COALESCE(i.index_name, dk.stock_code) AS index_name,
                    dk.trade_date,
                    dk.close_price
                FROM daily_kline dk
                LEFT JOIN indices i
                  ON i.index_code = dk.stock_code
                WHERE dk.stock_code = ?
                  AND dk.trade_date <= ?
                ORDER BY dk.trade_date DESC
                LIMIT ?
                """,
                (benchmark_code, latest_trade_date, TECHNICAL_LOOKBACK_BARS),
            )
            if not rows:
                continue

            rows.reverse()
            closes = [_to_float(row.get("close_price")) for row in rows]
            benchmark_return_20d = _return_ratio(closes, MA_FAST_WINDOW)
            benchmark_return_60d = _return_ratio(closes, MA_MID_WINDOW)
            benchmark_return_120d = _return_ratio(closes, MA_SLOW_WINDOW)
            return {
                "benchmark_code": benchmark_code,
                "benchmark_name": rows[-1].get("index_name") or benchmark_code,
                "benchmark_trade_date": rows[-1].get("trade_date"),
                "benchmark_return_20d": benchmark_return_20d,
                "benchmark_return_60d": benchmark_return_60d,
                "benchmark_return_120d": benchmark_return_120d,
                "excess_return_20d": (stock_return_20d - benchmark_return_20d) if stock_return_20d is not None and benchmark_return_20d is not None else None,
                "excess_return_60d": (stock_return_60d - benchmark_return_60d) if stock_return_60d is not None and benchmark_return_60d is not None else None,
                "excess_return_120d": (stock_return_120d - benchmark_return_120d) if stock_return_120d is not None and benchmark_return_120d is not None else None,
            }

        return {}

    def _load_industry_strength(
        self,
        *,
        industry_code: str | None,
        industry_name: str | None,
        latest_trade_date: str | None,
        reference_dates: dict,
        stock_return_20d: float | None,
        stock_return_60d: float | None,
        stock_return_120d: float | None,
    ) -> dict:
        if not industry_code or not latest_trade_date:
            return {}

        date_20d = reference_dates.get("20d")
        date_60d = reference_dates.get("60d")
        date_120d = reference_dates.get("120d")
        rows = db.fetchall(
            """
            SELECT
                dk.stock_code,
                MAX(CASE WHEN dk.trade_date = ? THEN dk.close_price END) AS latest_close,
                MAX(CASE WHEN dk.trade_date = ? THEN dk.close_price END) AS close_20d,
                MAX(CASE WHEN dk.trade_date = ? THEN dk.close_price END) AS close_60d,
                MAX(CASE WHEN dk.trade_date = ? THEN dk.close_price END) AS close_120d
            FROM daily_kline dk
            INNER JOIN stocks s
              ON s.stock_code = dk.stock_code
            WHERE s.status = 1
              AND s.industry_code = ?
              AND dk.trade_date IN (?, ?, ?, ?)
            GROUP BY dk.stock_code
            """,
            (
                latest_trade_date,
                date_20d,
                date_60d,
                date_120d,
                industry_code,
                latest_trade_date,
                date_20d,
                date_60d,
                date_120d,
            ),
        )

        return_20d_values = []
        return_60d_values = []
        return_120d_values = []
        for row in rows:
            latest_close = _to_float(row.get("latest_close"))
            close_20d = _to_float(row.get("close_20d"))
            close_60d = _to_float(row.get("close_60d"))
            close_120d = _to_float(row.get("close_120d"))

            if latest_close is not None and close_20d not in (None, 0):
                return_20d_values.append((latest_close / close_20d) - 1)
            if latest_close is not None and close_60d not in (None, 0):
                return_60d_values.append((latest_close / close_60d) - 1)
            if latest_close is not None and close_120d not in (None, 0):
                return_120d_values.append((latest_close / close_120d) - 1)

        industry_return_20d = _average(return_20d_values)
        industry_return_60d = _average(return_60d_values)
        industry_return_120d = _average(return_120d_values)

        return {
            "industry_code": industry_code,
            "industry_name": industry_name,
            "industry_peer_count": len(rows),
            "industry_return_20d": industry_return_20d,
            "industry_return_60d": industry_return_60d,
            "industry_return_120d": industry_return_120d,
            "excess_return_20d": (stock_return_20d - industry_return_20d) if stock_return_20d is not None and industry_return_20d is not None else None,
            "excess_return_60d": (stock_return_60d - industry_return_60d) if stock_return_60d is not None and industry_return_60d is not None else None,
            "excess_return_120d": (stock_return_120d - industry_return_120d) if stock_return_120d is not None and industry_return_120d is not None else None,
            "industry_percentile_20d": _percentile_rank(return_20d_values, stock_return_20d),
        }

    def _signal_score(
        self,
        *,
        close_above_ma20: bool,
        ma20_above_ma60: bool,
        ma20_slope_up: bool,
        return_20d: float | None,
        return_60d: float | None,
        position_120d: float | None,
        volume_ratio: float | None,
        up_day_volume_ratio: float | None,
        atr20: float | None,
        atr20_hist_avg: float | None,
        atr_percentile: float | None,
        distance_to_60d_high: float | None,
    ) -> tuple[int, list[str]]:
        checks = [
            (
                close_above_ma20 and ma20_above_ma60 and ma20_slope_up,
                "趋势结构顺畅",
            ),
            (
                return_20d is not None and return_60d is not None and return_20d > 0 and return_60d > 0,
                "中短期动能保持正向",
            ),
            (
                position_120d is not None and position_120d >= 0.65 and distance_to_60d_high is not None and distance_to_60d_high >= -0.06,
                "价格位置接近阶段强势区",
            ),
            (
                (volume_ratio is not None and volume_ratio >= 1.05) or (up_day_volume_ratio is not None and up_day_volume_ratio >= 0.55),
                "量价配合未走坏",
            ),
            (
                (
                    atr20 is not None
                    and atr20_hist_avg is not None
                    and atr20 <= atr20_hist_avg
                )
                or (atr_percentile is not None and atr_percentile <= 0.65),
                "波动风险可接受",
            ),
        ]
        reasons = [label for passed, label in checks if passed]
        return (len(reasons), reasons)

    def _signal_conclusion(self, score: int, breakout_ready: bool, bars_count: int) -> tuple[str, str]:
        if bars_count < MA_SLOW_WINDOW:
            return ("中性", "样本偏短，先看结构，不急着下强结论。")
        if score >= 4 and breakout_ready:
            return ("偏多", "趋势偏强，已接近突破触发区，可列入重点跟踪。")
        if score >= 4:
            return ("偏多", "技术结构完整，趋势占优，但仍需量能继续确认。")
        if score == 3:
            return ("中性偏强", "结构尚可，等待价格或量能给出下一步确认。")
        if score == 2:
            return ("中性", "多空信号分化，暂列观察，不适合追价。")
        return ("偏弱", "技术面暂不占优，优先等待结构修复。")

    def _trigger_text(self, resistance_label: str | None, resistance_level: float | None, ma20: float | None) -> str:
        if resistance_level is not None:
            return f"放量站上{resistance_label} {_format_price(resistance_level)}"
        if ma20 is not None:
            return f"重新站稳 MA20 {_format_price(ma20)}"
        return "等待价格重新走强"

    def _invalidation_text(
        self,
        support_label: str | None,
        support_level: float | None,
        ma20: float | None,
        low_20: float | None,
        latest_close: float | None,
    ) -> str:
        if support_level is not None:
            return f"收盘跌破{support_label} {_format_price(support_level)}"
        if ma20 is not None and latest_close is not None and latest_close > ma20:
            return f"收盘失守 MA20 {_format_price(ma20)}"
        if low_20 is not None:
            return f"跌破20日低点 {_format_price(low_20)}"
        return "跌破近期支撑位后重新评估"


technical_analysis_service = TechnicalAnalysisService()
