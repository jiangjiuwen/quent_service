from collections import defaultdict
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import date, datetime, timedelta
import io
import json
import os
import re
import signal
import threading
import time
from typing import Optional

from config.settings import BATCH_SIZE, rolling_history_start_text
from database.connection import db
from services.factor_service import factor_service
from sync.task_dispatcher import spawn_sync_task
from sync.task_locks import TaskAlreadyRunningError, task_lock
from utils.logger import logger

CHINEXT_REFORM_DATE = date(2020, 8, 24)
BAOSTOCK_SOCKET_TIMEOUT_SECONDS = 15
BAOSTOCK_QUERY_TIMEOUT_SECONDS = 30
SOURCE_CALL_TIMEOUT_SECONDS = 45
TASK_NO_PROGRESS_TIMEOUT_SECONDS = 300
TASK_WATCHDOG_POLL_SECONDS = 5
AKSHARE_MAX_RETRIES = 3
AKSHARE_RETRY_BASE_DELAY = 1.0
BAOSTOCK_SESSION_LOCK = threading.RLock()
SOURCE_COOLDOWNS: dict[str, float] = {}
TASK_WATCHDOG_SIGNAL = getattr(signal, "SIGUSR1", None)
BENCHMARK_INDEX_CODES = ("399300", "000300", "399905", "000905")
MARKET_OVERVIEW_RECENT_DAYS = 40
MARKET_OVERVIEW_WARMUP_EXTRA_DAYS = 20
MARKET_OVERVIEW_EVENT_STREAK_DAYS = 10
MARKET_OVERVIEW_MIN_SECTOR_SIZE = 8


class TaskProgressReporter:
    def __init__(self, sync_type: str, task_handle=None, log_id: Optional[int] = None, min_interval_seconds: float = 0.8):
        self.sync_type = sync_type
        self.task_handle = task_handle
        self.log_id = log_id
        self.min_interval_seconds = min_interval_seconds
        self.state = {"sync_type": sync_type}
        self._last_emit = 0.0
        self._lock = threading.Lock()

    def _checkpoint_payload(self) -> dict:
        payload = dict(self.state)
        total = payload.get("total")
        processed = payload.get("processed")
        if total not in (None, 0) and processed is not None:
            payload["progress_percent"] = round(min(max(processed / total * 100, 0), 100), 1)
        payload["updated_at"] = datetime.now().isoformat(timespec="seconds")
        return payload

    def update(self, force: bool = False, **updates):
        with self._lock:
            if updates:
                self.state.update(updates)
            payload = self._checkpoint_payload()
            now = time.time()
            if not force and now - self._last_emit < self.min_interval_seconds:
                self.state.update(payload)
                return

            self.state.update(payload)
            if self.task_handle is not None:
                self.task_handle.update(**payload)
            if self.log_id:
                update_sync_checkpoint(
                    self.log_id,
                    checkpoint=payload,
                    total=payload.get("total"),
                    success=payload.get("success"),
                    fail=payload.get("fail"),
                )
            self._last_emit = now


class TaskStalledError(RuntimeError):
    """任务长时间无进展时主动终止"""


def _call_with_timeout(task_label: str, func, timeout_seconds: Optional[float] = SOURCE_CALL_TIMEOUT_SECONDS):
    """对阻塞型外部调用施加硬超时，避免任务假活着卡死"""
    if timeout_seconds is None or timeout_seconds <= 0:
        return func()

    timeout_message = f"{task_label} 超时({timeout_seconds:.0f}s)"
    can_use_signal_alarm = (
        threading.current_thread() is threading.main_thread()
        and hasattr(signal, "SIGALRM")
        and hasattr(signal, "setitimer")
        and hasattr(signal, "ITIMER_REAL")
    )

    if can_use_signal_alarm:
        previous_handler = signal.getsignal(signal.SIGALRM)

        def _handle_timeout(_signum, _frame):
            raise TimeoutError(timeout_message)

        signal.signal(signal.SIGALRM, _handle_timeout)
        if hasattr(signal, "siginterrupt"):
            try:
                signal.siginterrupt(signal.SIGALRM, True)
            except Exception:
                pass
        signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
        try:
            return func()
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)

    done = threading.Event()
    outcome: dict[str, object] = {}

    def _runner():
        try:
            outcome["value"] = func()
        except BaseException as exc:
            outcome["error"] = exc
        finally:
            done.set()

    worker = threading.Thread(target=_runner, name=f"timeout-{task_label}", daemon=True)
    worker.start()
    if not done.wait(timeout_seconds):
        raise TimeoutError(timeout_message)
    if "error" in outcome:
        raise outcome["error"]
    return outcome.get("value")


class TaskProgressWatchdog:
    """监控任务是否长期无进展，必要时主动中断主线程"""

    def __init__(
        self,
        task_name: str,
        reporter: TaskProgressReporter,
        timeout_seconds: float = TASK_NO_PROGRESS_TIMEOUT_SECONDS,
        poll_seconds: float = TASK_WATCHDOG_POLL_SECONDS,
    ):
        self.task_name = task_name
        self.reporter = reporter
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = poll_seconds
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._tripped = threading.Event()
        self._last_progress_monotonic = time.monotonic()
        self._last_progress_at = datetime.now().isoformat(timespec="seconds")
        self._current_context = "初始化"
        self._trip_reason: Optional[str] = None
        self._thread = threading.Thread(target=self._run, name=f"{task_name}-watchdog", daemon=True)
        self._can_signal_main = (
            TASK_WATCHDOG_SIGNAL is not None
            and threading.current_thread() is threading.main_thread()
        )
        self._previous_signal_handler = None

    def start(self):
        if self._can_signal_main:
            self._previous_signal_handler = signal.getsignal(TASK_WATCHDOG_SIGNAL)

            def _handle_watchdog_signal(_signum, _frame):
                raise TaskStalledError(self._trip_reason or f"{self.task_name} 长时间无进展")

            signal.signal(TASK_WATCHDOG_SIGNAL, _handle_watchdog_signal)
            if hasattr(signal, "siginterrupt"):
                try:
                    signal.siginterrupt(TASK_WATCHDOG_SIGNAL, True)
                except Exception:
                    pass

        self.reporter.update(
            force=True,
            watchdog_enabled=True,
            stalled_after_seconds=self.timeout_seconds,
            last_progress_at=self._last_progress_at,
            stalled_idle_seconds=0,
        )
        self._thread.start()
        return self

    def stop(self):
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1)
        if self._can_signal_main and self._previous_signal_handler is not None:
            signal.signal(TASK_WATCHDOG_SIGNAL, self._previous_signal_handler)

    def set_context(self, context: str):
        with self._lock:
            self._current_context = context

    def mark_progress(self, context: Optional[str] = None, **reporter_updates):
        with self._lock:
            if context is not None:
                self._current_context = context
            self._last_progress_monotonic = time.monotonic()
            self._last_progress_at = datetime.now().isoformat(timespec="seconds")
            last_progress_at = self._last_progress_at
        self.reporter.update(
            last_progress_at=last_progress_at,
            stalled_after_seconds=self.timeout_seconds,
            stalled_idle_seconds=0,
            **reporter_updates,
        )

    def raise_if_tripped(self):
        if self._tripped.is_set():
            raise TaskStalledError(self._trip_reason or f"{self.task_name} 长时间无进展")

    def _trip(self, idle_seconds: float, last_progress_at: str, context: str):
        self._trip_reason = (
            f"{self.task_name} 超过{int(self.timeout_seconds)}s无进展"
            f"，当前环节: {context or 'unknown'}"
            f"，最近进展时间: {last_progress_at}"
        )
        self._tripped.set()
        logger.error(self._trip_reason)
        try:
            self.reporter.update(
                force=True,
                watchdog_tripped=True,
                error_message=self._trip_reason,
                last_progress_at=last_progress_at,
                stalled_after_seconds=self.timeout_seconds,
                stalled_idle_seconds=round(idle_seconds, 1),
            )
        except Exception as exc:
            logger.error(f"{self.task_name} 看门狗上报失败: {exc}")

        if self._can_signal_main:
            try:
                os.kill(os.getpid(), TASK_WATCHDOG_SIGNAL)
            except Exception as exc:
                logger.error(f"{self.task_name} 看门狗发送中断信号失败: {exc}")

    def _run(self):
        while not self._stop_event.wait(self.poll_seconds):
            with self._lock:
                idle_seconds = time.monotonic() - self._last_progress_monotonic
                last_progress_at = self._last_progress_at
                context = self._current_context

            if idle_seconds < self.timeout_seconds:
                continue

            self._trip(idle_seconds, last_progress_at, context)
            return


def _retry_call(
    task_label: str,
    func,
    retries: int = AKSHARE_MAX_RETRIES,
    base_delay: float = AKSHARE_RETRY_BASE_DELAY,
    timeout_seconds: Optional[float] = SOURCE_CALL_TIMEOUT_SECONDS,
):
    """对易抖动的数据抓取调用做有限次重试"""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return _call_with_timeout(task_label, func, timeout_seconds=timeout_seconds)
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                break
            wait_seconds = base_delay * attempt
            logger.warning(
                f"{task_label} 第{attempt}次失败，{wait_seconds:.1f}s后重试: {exc}"
            )
            time.sleep(wait_seconds)
    raise last_error or RuntimeError(f"{task_label} 执行失败")


def _baostock_login():
    """建立 baostock 会话"""
    import baostock as bs
    import baostock.common.context as bs_context

    login_result = _call_with_timeout(
        "baostock 登录",
        bs.login,
        timeout_seconds=BAOSTOCK_QUERY_TIMEOUT_SECONDS,
    )
    if login_result.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {login_result.error_msg}")

    default_socket = getattr(bs_context, "default_socket", None)
    if default_socket is not None:
        try:
            default_socket.settimeout(BAOSTOCK_SOCKET_TIMEOUT_SECONDS)
        except Exception as exc:
            logger.warning(f"设置 baostock socket 超时失败: {exc}")
    return bs


def _baostock_logout(bs_client):
    """安全关闭 baostock 会话"""
    try:
        bs_client.logout()
    except Exception:
        pass


@contextmanager
def _baostock_session():
    """baostock 依赖进程内全局 socket，上层必须串行使用"""
    with BAOSTOCK_SESSION_LOCK:
        bs_client = _baostock_login()
        try:
            yield bs_client
        finally:
            _baostock_logout(bs_client)


def _baostock_query(bs_client, method_name: str, *args, **kwargs):
    """执行 baostock 查询并在失效时自动重登"""
    result = _call_with_timeout(
        f"baostock {method_name}",
        lambda: getattr(bs_client, method_name)(*args, **kwargs),
        timeout_seconds=BAOSTOCK_QUERY_TIMEOUT_SECONDS,
    )
    if result.error_code == "10001001":
        login_result = _call_with_timeout(
            "baostock 重登",
            bs_client.login,
            timeout_seconds=BAOSTOCK_QUERY_TIMEOUT_SECONDS,
        )
        if login_result.error_code != "0":
            raise RuntimeError(f"baostock 重登失败: {login_result.error_msg}")
        result = _call_with_timeout(
            f"baostock {method_name}",
            lambda: getattr(bs_client, method_name)(*args, **kwargs),
            timeout_seconds=BAOSTOCK_QUERY_TIMEOUT_SECONDS,
        )
    return result


def _safe_text(value):
    """清洗空字符串"""
    if value is None:
        return None
    value = str(value).strip()
    if value.lower() in {"nan", "none", "nat"}:
        return None
    return value or None


def _normalize_date_text(value):
    """将常见日期格式统一为 YYYY-MM-DD"""
    value = _safe_text(value)
    if value is None:
        return None

    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return value


def _safe_float(value):
    """转为浮点数"""
    if isinstance(value, bool):
        return None
    value = _safe_text(value)
    if value is None:
        return None

    lowered = value.lower()
    if lowered in {"false", "true", "null"}:
        return None

    multiplier = 1.0
    cleaned = value.replace(",", "").replace(" ", "")
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    elif cleaned.endswith("万亿"):
        cleaned = cleaned[:-2]
        multiplier = 1e12
    elif cleaned.endswith("亿"):
        cleaned = cleaned[:-1]
        multiplier = 1e8
    elif cleaned.endswith("万"):
        cleaned = cleaned[:-1]
        multiplier = 1e4

    try:
        return float(cleaned) * multiplier
    except ValueError:
        match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
        if match:
            return float(match.group()) * multiplier
        raise


def _safe_int(value):
    """转为整数"""
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _to_baostock_code(stock_code: str) -> str:
    """将证券代码转换为 baostock 格式"""
    if "." in stock_code:
        return stock_code.lower()
    if stock_code.startswith("6"):
        return f"sh.{stock_code}"
    if stock_code.startswith(("0", "3")):
        return f"sz.{stock_code}"
    if stock_code.startswith(("4", "8")):
        return f"bj.{stock_code}"
    return stock_code.lower()


def _to_xueqiu_symbol(stock_code: str) -> str:
    """将证券代码转换为雪球格式"""
    code = _plain_stock_code(stock_code)
    if code.startswith("6"):
        return f"SH{code}"
    if code.startswith(("0", "3")):
        return f"SZ{code}"
    if code.startswith(("4", "8")):
        return f"BJ{code}"
    return code.upper()


def _plain_stock_code(stock_code: str) -> str:
    """提取纯证券代码"""
    return stock_code.split(".")[-1]


def _is_supported_a_share_code(stock_code: str) -> bool:
    """判断是否为当前支持的 A 股代码"""
    code = stock_code.lower()
    return code.startswith(
        (
            "sh.600",
            "sh.601",
            "sh.603",
            "sh.605",
            "sh.688",
            "sh.689",
            "sz.000",
            "sz.001",
            "sz.002",
            "sz.003",
            "sz.30",
        )
    )


def _infer_market_meta(stock_code: str):
    """根据代码推断交易所和板块"""
    code = _plain_stock_code(stock_code)
    if code.startswith(("688", "689")):
        return {
            "exchange": "SH",
            "board": "STAR",
            "sec_type": "A_STOCK",
            "market_type": "SH",
        }
    if code.startswith(("600", "601", "603", "605")):
        return {
            "exchange": "SH",
            "board": "MAIN",
            "sec_type": "A_STOCK",
            "market_type": "SH",
        }
    if code.startswith("30"):
        return {
            "exchange": "SZ",
            "board": "CHINEXT",
            "sec_type": "A_STOCK",
            "market_type": "SZ",
        }
    if code.startswith(("000", "001", "002", "003")):
        return {
            "exchange": "SZ",
            "board": "MAIN",
            "sec_type": "A_STOCK",
            "market_type": "SZ",
        }
    return {
        "exchange": None,
        "board": None,
        "sec_type": "A_STOCK",
        "market_type": None,
    }


def _to_sina_symbol(stock_code: str) -> str:
    """将证券代码转换为新浪财报接口格式"""
    code = _plain_stock_code(stock_code)
    if code.startswith("6"):
        return f"sh{code}"
    if code.startswith(("0", "3")):
        return f"sz{code}"
    return code.lower()


def _normalize_index_code(index_code: str) -> Optional[str]:
    """统一指数代码格式为 000001 / 399001"""
    code = _safe_text(index_code)
    if code is None:
        return None

    normalized = code.lower().replace(".", "")
    if normalized.startswith(("sh", "sz")) and len(normalized) == 8:
        return normalized[2:]

    if len(normalized) == 6 and normalized.isdigit():
        return normalized
    return normalized


def _infer_index_meta(index_code: str) -> dict:
    """根据指数代码推断交易所和市场"""
    normalized = _normalize_index_code(index_code) or _plain_stock_code(index_code)
    exchange = "SZ" if normalized.startswith("399") else "SH"
    return {
        "index_code": normalized,
        "exchange": exchange,
        "market_type": exchange,
        "index_type": "A_INDEX",
    }


def _industry_key(industry_source: str, industry_name: str) -> str:
    """构造行业编码"""
    return f"{industry_source}:{industry_name}"


def _compute_limit_ratio(board: str, is_st: int, trade_date: str) -> float:
    """计算涨跌停比例"""
    if is_st:
        return 0.05
    if board == "BSE":
        return 0.30
    if board == "STAR":
        return 0.20
    if board == "CHINEXT":
        current_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
        return 0.20 if current_date >= CHINEXT_REFORM_DATE else 0.10
    return 0.10


def _compute_limit_prices(pre_close, board: str, is_st: int, trade_date: str):
    """根据昨收和板块计算涨跌停价格"""
    pre_close_value = _safe_float(pre_close)
    if not pre_close_value:
        return None, None
    limit_ratio = _compute_limit_ratio(board, is_st, trade_date)
    limit_up = round(pre_close_value * (1 + limit_ratio), 2)
    limit_down = round(pre_close_value * (1 - limit_ratio), 2)
    return limit_up, limit_down


def _is_source_in_cooldown(source_key: str) -> bool:
    cooldown_until = SOURCE_COOLDOWNS.get(source_key)
    if cooldown_until is None:
        return False
    if cooldown_until <= time.time():
        SOURCE_COOLDOWNS.pop(source_key, None)
        return False
    return True


def _activate_source_cooldown(source_key: str, cooldown_seconds: int, reason: Exception):
    SOURCE_COOLDOWNS[source_key] = time.time() + cooldown_seconds
    logger.warning(f"{source_key} 进入冷却 {cooldown_seconds}s: {reason}")


def _is_empty_payload_error(exc: Exception) -> bool:
    message = str(exc)
    return "Expecting value: line 1 column 1" in message or "char 0" in message


def _parse_report_period(value) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """将报告日解析为 YYYYQn / 报告期末"""
    report_period_end = _normalize_date_text(value)
    if report_period_end is None:
        return None, None, None

    try:
        period_date = datetime.strptime(report_period_end, "%Y-%m-%d").date()
    except ValueError:
        return None, None, None

    report_type = (period_date.month - 1) // 3 + 1
    return f"{period_date.year}Q{report_type}", report_type, period_date.isoformat()


def _normalize_currency(value: str) -> Optional[str]:
    """统一币种展示"""
    text = _safe_text(value)
    if text is None:
        return None

    normalized = text.upper()
    if normalized in {"CNY", "RMB"} or "人民币" in text:
        return "CNY"
    return normalized


def _pick_row_value(row, exact_names: tuple[str, ...], fuzzy_groups: tuple[tuple[str, ...], ...] = (), normalizer=_safe_float):
    """按候选列名从单行报表中提取字段"""
    mapping = row.to_dict() if hasattr(row, "to_dict") else dict(row)

    for name in exact_names:
        if name not in mapping:
            continue
        value = normalizer(mapping.get(name))
        if value is not None:
            return value

    for keywords in fuzzy_groups:
        for key, raw_value in mapping.items():
            key_text = str(key)
            if all(keyword in key_text for keyword in keywords):
                value = normalizer(raw_value)
                if value is not None:
                    return value
    return None


def _financial_record_template(stock_code: str, report_period: str, report_type: int, report_period_end: str) -> dict:
    return {
        "stock_code": stock_code,
        "report_period": report_period,
        "report_type": report_type,
        "announce_date": None,
        "report_period_end": report_period_end,
        "statement_type": None,
        "currency": None,
        "total_assets": None,
        "total_liabilities": None,
        "net_assets": None,
        "revenue": None,
        "net_profit": None,
        "eps": None,
        "roe": None,
        "gross_margin": None,
        "debt_ratio": None,
    }


def _fetch_trade_calendar_with_baostock(start_date: str, end_date: str, bs_client=None):
    """同步交易日历"""
    managed_client = bs_client is None
    if bs_client is None:
        bs_client = _baostock_login()

    try:
        result = _baostock_query(
            bs_client,
            "query_trade_dates",
            start_date=start_date,
            end_date=end_date,
        )
        if result.error_code != "0":
            raise RuntimeError(f"baostock 交易日历同步失败: {result.error_msg}")

        rows = []
        while result.next():
            rows.append(result.get_row_data())

        open_dates = [row[0] for row in rows if row[1] == "1"]
        prev_open = None
        next_open_map = {}
        for open_date in open_dates:
            if prev_open is not None:
                next_open_map[prev_open] = open_date
            prev_open = open_date

        records = []
        last_open = None
        for calendar_date, is_trading_day in rows:
            if is_trading_day == "1":
                prev_trade_date = last_open
                next_trade_date = next_open_map.get(calendar_date)
                last_open = calendar_date
            else:
                prev_trade_date = last_open
                next_trade_date = next(
                    (trade_date for trade_date in open_dates if trade_date > calendar_date),
                    None,
                )

            records.append(
                {
                    "trade_date": calendar_date,
                    "exchange": "CN_A",
                    "is_open": int(is_trading_day),
                    "prev_trade_date": prev_trade_date,
                    "next_trade_date": next_trade_date,
                }
            )
        return records
    finally:
        if managed_client:
            _baostock_logout(bs_client)


def _fetch_stock_list_with_akshare():
    """从 akshare 获取股票列表"""
    import akshare as ak

    stock_df = _retry_call("akshare 股票列表抓取", ak.stock_zh_a_spot_em)
    records = []
    for _, row in stock_df.iterrows():
        stock_code = str(row["代码"])
        if not stock_code.startswith(("6", "0", "3")):
            continue
        market_meta = _infer_market_meta(stock_code)
        records.append(
            {
                "stock_code": stock_code,
                "stock_name": row["名称"],
                "market_type": market_meta["market_type"],
                "exchange": market_meta["exchange"],
                "board": market_meta["board"],
                "sec_type": market_meta["sec_type"],
                "status": 1,
                "source": "akshare",
            }
        )
    return records


def _fetch_index_list_with_akshare():
    """从 akshare 获取指数池"""
    import akshare as ak

    index_df = _retry_call("akshare 指数池抓取", ak.stock_zh_index_spot_sina)
    records = {}
    for _, row in index_df.iterrows():
        index_code = _normalize_index_code(row["代码"])
        index_name = _safe_text(row["名称"])
        if index_code is None or index_name is None:
            continue
        if not (len(index_code) == 6 and index_code.isdigit()):
            continue

        meta = _infer_index_meta(index_code)
        records[index_code] = {
            "index_code": meta["index_code"],
            "index_name": index_name,
            "market_type": meta["market_type"],
            "exchange": meta["exchange"],
            "index_type": meta["index_type"],
            "list_date": None,
            "delist_date": None,
            "status": 1,
            "source": "akshare_sina",
        }
    return list(records.values())


def _benchmark_index_seed_records():
    """确保分析依赖的基准指数在指数池中有最小主数据"""
    fallback_names = {
        "399300": "沪深300",
        "000300": "沪深300",
        "399905": "中证500",
        "000905": "中证500",
    }
    records = []
    for index_code in BENCHMARK_INDEX_CODES:
        meta = _infer_index_meta(index_code)
        records.append(
            {
                "index_code": meta["index_code"],
                "index_name": fallback_names.get(meta["index_code"], meta["index_code"]),
                "market_type": meta["market_type"],
                "exchange": meta["exchange"],
                "index_type": meta["index_type"],
                "list_date": None,
                "delist_date": None,
                "status": 1,
                "source": "benchmark_seed",
            }
        )
    return records


def _resolve_benchmark_kline_fetch_window(index_code: str, start_date: Optional[str], end_date: Optional[str]) -> tuple[str, str]:
    """基准指数缺少足够历史样本时，自动补取一段窗口用于相对强弱分析"""
    effective_end_date = end_date or datetime.now().date().isoformat()
    effective_start_date = start_date or effective_end_date
    stats = db.fetchone(
        """
        SELECT COUNT(*) AS bar_count
        FROM daily_kline
        WHERE stock_code = ?
        """,
        (index_code,),
    ) or {"bar_count": 0}
    if int(stats.get("bar_count") or 0) < 130:
        bootstrap_start = (date.fromisoformat(effective_end_date) - timedelta(days=240)).isoformat()
        if bootstrap_start < effective_start_date:
            effective_start_date = bootstrap_start
    return effective_start_date, effective_end_date


def _fetch_index_daily_kline_with_akshare(index_code: str, start_date: str, end_date: str):
    """使用 akshare 抓取指数日线，静默掉进度条输出"""
    import akshare as ak

    def _call():
        sink = io.StringIO()
        with redirect_stdout(sink), redirect_stderr(sink):
            return ak.index_zh_a_hist(
                symbol=index_code,
                period="daily",
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
            )

    index_df = _retry_call(f"{index_code} 指数日线抓取", _call)
    if index_df is None or index_df.empty:
        return []

    records = []
    previous_close = None
    for _, row in index_df.iterrows():
        close_price = _safe_float(row.get("收盘"))
        pre_close = previous_close
        pct_change = _safe_float(row.get("涨跌幅"))
        if pct_change is None and previous_close not in (None, 0) and close_price is not None:
            pct_change = round((close_price - previous_close) / previous_close * 100, 6)

        volume = _safe_int(row.get("成交量"))
        if volume is None:
            volume = 0

        records.append(
            {
                "trade_date": str(row.get("日期")),
                "open_price": _safe_float(row.get("开盘")),
                "high_price": _safe_float(row.get("最高")),
                "low_price": _safe_float(row.get("最低")),
                "close_price": close_price,
                "pre_close": pre_close,
                "pct_change": pct_change,
                "volume": volume,
                "amount": _safe_float(row.get("成交额")),
                "turnover_rate": _safe_float(row.get("换手率", 0)),
                "pe_ratio": None,
                "pb_ratio": None,
                "ps_ttm": None,
                "pcf_ttm": None,
                "tradestatus": 1,
                "is_st": 0,
                "source": "akshare_index",
                "price_mode": "raw",
            }
        )
        previous_close = close_price
    return records


def _fetch_financial_balance_sheet_with_akshare(stock_code: str):
    """抓取资产负债表"""
    import akshare as ak
    source_key = "financial_sina"

    if _is_source_in_cooldown(source_key):
        raise RuntimeError("financial_sina source cooldown active")

    try:
        return _retry_call(
            f"{stock_code} 资产负债表抓取",
            lambda: ak.stock_financial_report_sina(stock=_to_sina_symbol(stock_code), symbol="资产负债表"),
        )
    except Exception as exc:
        if _is_empty_payload_error(exc):
            _activate_source_cooldown(source_key, cooldown_seconds=300, reason=exc)
        raise


def _fetch_financial_income_statement_with_akshare(stock_code: str):
    """抓取利润表"""
    import akshare as ak
    source_key = "financial_sina"

    if _is_source_in_cooldown(source_key):
        raise RuntimeError("financial_sina source cooldown active")

    try:
        return _retry_call(
            f"{stock_code} 利润表抓取",
            lambda: ak.stock_financial_report_sina(stock=_to_sina_symbol(stock_code), symbol="利润表"),
        )
    except Exception as exc:
        if _is_empty_payload_error(exc):
            _activate_source_cooldown(source_key, cooldown_seconds=300, reason=exc)
        raise


def _fetch_financial_abstract_with_akshare(stock_code: str):
    """抓取财务摘要"""
    import akshare as ak
    source_key = "financial_sina"

    if _is_source_in_cooldown(source_key):
        raise RuntimeError("financial_sina source cooldown active")

    try:
        return _retry_call(
            f"{stock_code} 财务摘要抓取",
            lambda: ak.stock_financial_abstract(symbol=_plain_stock_code(stock_code)),
        )
    except Exception as exc:
        if _is_empty_payload_error(exc):
            _activate_source_cooldown(source_key, cooldown_seconds=300, reason=exc)
        raise


def _fetch_financial_abstract_with_ths(stock_code: str):
    """从同花顺抓取财务摘要作为回退源"""
    import akshare as ak

    return _retry_call(
        f"{stock_code} 同花顺财务摘要抓取",
        lambda: ak.stock_financial_abstract_ths(symbol=_plain_stock_code(stock_code)),
        retries=2,
        base_delay=0.5,
    )


def _merge_financial_balance_rows(stock_code: str, balance_df, records_by_period: dict):
    for _, row in balance_df.iterrows():
        report_period, report_type, report_period_end = _parse_report_period(row.get("报告日"))
        if report_period is None or report_type is None or report_period_end is None:
            continue

        record = records_by_period.setdefault(
            report_period,
            _financial_record_template(stock_code, report_period, report_type, report_period_end),
        )
        record["report_period_end"] = record.get("report_period_end") or report_period_end
        record["announce_date"] = record.get("announce_date") or _normalize_date_text(row.get("公告日期"))
        record["currency"] = record.get("currency") or _normalize_currency(row.get("币种")) or "CNY"
        record["statement_type"] = record.get("statement_type") or _safe_text(row.get("类型"))
        total_assets = _pick_row_value(
            row,
            ("资产总计", "负债及股东权益总计"),
            normalizer=_safe_float,
        )
        if total_assets is not None:
            record["total_assets"] = total_assets

        total_liabilities = _pick_row_value(
            row,
            ("负债合计",),
            normalizer=_safe_float,
        )
        if total_liabilities is not None:
            record["total_liabilities"] = total_liabilities

        net_assets = _pick_row_value(
            row,
            (
                "归属于母公司股东的权益",
                "归属于母公司股东权益合计",
                "归属于母公司所有者权益合计",
                "归属于母公司所有者权益",
                "股东权益合计(净资产)",
            ),
            (
                ("归属于母公司", "权益"),
                ("股东权益",),
            ),
            normalizer=_safe_float,
        )
        if net_assets is not None:
            record["net_assets"] = net_assets


def _merge_financial_income_rows(stock_code: str, income_df, records_by_period: dict):
    for _, row in income_df.iterrows():
        report_period, report_type, report_period_end = _parse_report_period(row.get("报告日"))
        if report_period is None or report_type is None or report_period_end is None:
            continue

        record = records_by_period.setdefault(
            report_period,
            _financial_record_template(stock_code, report_period, report_type, report_period_end),
        )
        record["report_period_end"] = record.get("report_period_end") or report_period_end
        record["announce_date"] = record.get("announce_date") or _normalize_date_text(row.get("公告日期"))
        record["currency"] = record.get("currency") or _normalize_currency(row.get("币种")) or "CNY"
        record["statement_type"] = record.get("statement_type") or _safe_text(row.get("类型"))
        revenue = _pick_row_value(
            row,
            ("营业总收入", "营业收入"),
            (
                ("营业总收入",),
                ("营业收入",),
            ),
            normalizer=_safe_float,
        )
        if revenue is not None:
            record["revenue"] = revenue

        net_profit = _pick_row_value(
            row,
            (
                "归属于母公司所有者的净利润",
                "归属于母公司的净利润",
                "归属于母公司股东的净利润",
                "归母净利润",
                "净利润",
            ),
            (
                ("归属于母公司", "净利润"),
                ("归母", "净利润"),
            ),
            normalizer=_safe_float,
        )
        if net_profit is not None:
            record["net_profit"] = net_profit


def _merge_financial_abstract_rows(stock_code: str, abstract_df, records_by_period: dict):
    if abstract_df is None or abstract_df.empty:
        return

    section_rank = {
        "常用指标": 0,
        "每股指标": 1,
        "盈利能力": 2,
        "财务风险": 3,
        "成长能力": 4,
    }
    selected_metric_rows = {}

    for _, row in abstract_df.iterrows():
        metric_name = _safe_text(row.get("指标"))
        if metric_name is None:
            continue
        rank = section_rank.get(_safe_text(row.get("选项")) or "", 99)
        existing = selected_metric_rows.get(metric_name)
        if existing is None or rank < existing[0]:
            selected_metric_rows[metric_name] = (rank, row)

    metric_mapping = {
        "net_assets": ("股东权益合计(净资产)",),
        "revenue": ("营业总收入", "营业收入"),
        "net_profit": ("归母净利润", "净利润"),
        "eps": ("基本每股收益",),
        "roe": ("净资产收益率(ROE)", "净资产收益率"),
        "gross_margin": ("毛利率",),
        "debt_ratio": ("资产负债率",),
    }
    period_columns = [column for column in abstract_df.columns if re.fullmatch(r"\d{8}", str(column))]

    for field_name, metric_candidates in metric_mapping.items():
        metric_row = None
        for metric_name in metric_candidates:
            metric_row = selected_metric_rows.get(metric_name)
            if metric_row is not None:
                metric_row = metric_row[1]
                break
        if metric_row is None:
            continue

        for column in period_columns:
            report_period, report_type, report_period_end = _parse_report_period(column)
            if report_period is None or report_type is None or report_period_end is None:
                continue

            value = _safe_float(metric_row.get(column))
            if value is None:
                continue

            record = records_by_period.setdefault(
                report_period,
                _financial_record_template(stock_code, report_period, report_type, report_period_end),
            )
            record["report_period_end"] = record.get("report_period_end") or report_period_end
            record["currency"] = record.get("currency") or "CNY"
            if record.get(field_name) is None:
                record[field_name] = value


def _merge_financial_abstract_ths_rows(stock_code: str, abstract_df, records_by_period: dict):
    if abstract_df is None or abstract_df.empty:
        return

    for _, row in abstract_df.iterrows():
        report_period, report_type, report_period_end = _parse_report_period(row.get("报告期"))
        if report_period is None or report_type is None or report_period_end is None:
            continue

        record = records_by_period.setdefault(
            report_period,
            _financial_record_template(stock_code, report_period, report_type, report_period_end),
        )
        record["report_period_end"] = record.get("report_period_end") or report_period_end
        record["currency"] = record.get("currency") or "CNY"
        record["statement_type"] = record.get("statement_type") or "ths_abstract"

        revenue = _safe_float(row.get("营业总收入"))
        net_profit = _safe_float(row.get("净利润"))
        eps = _safe_float(row.get("基本每股收益"))
        gross_margin = _safe_float(row.get("销售毛利率"))
        debt_ratio = _safe_float(row.get("资产负债率"))
        roe = _safe_float(row.get("净资产收益率"))
        if roe is None:
            roe = _safe_float(row.get("净资产收益率-摊薄"))

        if record.get("revenue") is None and revenue is not None:
            record["revenue"] = revenue
        if record.get("net_profit") is None and net_profit is not None:
            record["net_profit"] = net_profit
        if record.get("eps") is None and eps is not None:
            record["eps"] = eps
        if record.get("gross_margin") is None and gross_margin is not None:
            record["gross_margin"] = gross_margin
        if record.get("debt_ratio") is None and debt_ratio is not None:
            record["debt_ratio"] = debt_ratio
        if record.get("roe") is None and roe is not None:
            record["roe"] = roe


def _build_financial_report_rows(stock_code: str):
    """组合单只股票的财务报表记录"""
    records_by_period = {}
    errors = []

    try:
        balance_df = _fetch_financial_balance_sheet_with_akshare(stock_code)
        _merge_financial_balance_rows(stock_code, balance_df, records_by_period)
    except Exception as exc:
        errors.append(f"balance={exc}")

    try:
        income_df = _fetch_financial_income_statement_with_akshare(stock_code)
        _merge_financial_income_rows(stock_code, income_df, records_by_period)
    except Exception as exc:
        errors.append(f"income={exc}")

    try:
        abstract_df = _fetch_financial_abstract_with_akshare(stock_code)
        _merge_financial_abstract_rows(stock_code, abstract_df, records_by_period)
    except Exception as exc:
        errors.append(f"abstract={exc}")
        try:
            ths_df = _fetch_financial_abstract_with_ths(stock_code)
            _merge_financial_abstract_ths_rows(stock_code, ths_df, records_by_period)
        except Exception as ths_exc:
            errors.append(f"abstract_ths={ths_exc}")

    value_fields = (
        "total_assets",
        "total_liabilities",
        "net_assets",
        "revenue",
        "net_profit",
        "eps",
        "roe",
        "gross_margin",
        "debt_ratio",
    )
    records = [
        record
        for record in records_by_period.values()
        if any(record.get(field) is not None for field in value_fields)
    ]
    records.sort(key=lambda item: item.get("report_period_end") or "", reverse=True)

    if records:
        return records
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def _cleanup_index_code_formats():
    """清理历史 sh000001 / sz399001 等旧格式代码，统一并合并到六位代码"""
    rows = db.fetchall("SELECT * FROM indices")
    if not rows:
        return

    sql = """
        INSERT INTO indices (
            index_code, index_name, market_type, exchange, index_type,
            list_date, delist_date, status, source, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(index_code) DO UPDATE SET
            index_name = excluded.index_name,
            market_type = COALESCE(excluded.market_type, indices.market_type),
            exchange = COALESCE(excluded.exchange, indices.exchange),
            index_type = COALESCE(excluded.index_type, indices.index_type),
            list_date = COALESCE(excluded.list_date, indices.list_date),
            delist_date = COALESCE(excluded.delist_date, indices.delist_date),
            status = COALESCE(excluded.status, indices.status),
            source = COALESCE(excluded.source, indices.source),
            created_at = COALESCE(indices.created_at, excluded.created_at),
            updated_at = COALESCE(excluded.updated_at, indices.updated_at)
    """

    with db.get_connection() as conn:
        for row in rows:
            old_code = row["index_code"]
            new_code = _normalize_index_code(old_code)
            if new_code is None or new_code == old_code:
                continue

            existing = conn.execute("SELECT * FROM indices WHERE index_code = ?", (new_code,)).fetchone()
            preferred = row
            fallback = dict(existing) if existing else {}

            merged = {
                "index_code": new_code,
                "index_name": preferred["index_name"] or fallback.get("index_name"),
                "market_type": preferred["market_type"] or fallback.get("market_type"),
                "exchange": preferred["exchange"] or fallback.get("exchange"),
                "index_type": preferred["index_type"] or fallback.get("index_type"),
                "list_date": preferred["list_date"] or fallback.get("list_date"),
                "delist_date": preferred["delist_date"] or fallback.get("delist_date"),
                "status": preferred["status"] if preferred["status"] is not None else fallback.get("status"),
                "source": preferred["source"] or fallback.get("source"),
                "created_at": fallback.get("created_at") or preferred["created_at"],
                "updated_at": preferred["updated_at"] or fallback.get("updated_at"),
            }

            conn.execute("DELETE FROM indices WHERE index_code IN (?, ?)", (old_code, new_code))
            conn.execute(
                sql,
                (
                    merged["index_code"],
                    merged["index_name"],
                    merged["market_type"],
                    merged["exchange"],
                    merged["index_type"],
                    merged["list_date"],
                    merged["delist_date"],
                    merged["status"],
                    merged["source"],
                    merged["created_at"],
                    merged["updated_at"],
                ),
            )


def _fetch_stock_list_with_baostock(bs_client=None):
    """从 baostock 获取股票列表"""
    managed_client = bs_client is None
    if bs_client is None:
        bs_client = _baostock_login()

    try:
        rows = []
        for offset in range(0, 10):
            day = (datetime.now().date() - timedelta(days=offset)).isoformat()
            result = _baostock_query(bs_client, "query_all_stock", day=day)
            if result.error_code != "0":
                continue

            current_rows = []
            while result.next():
                current_rows.append(result.get_row_data())
            if current_rows:
                rows = current_rows
                break

        if not rows:
            raise RuntimeError("baostock 未返回股票列表")

        records = []
        for code, trade_status, name in rows:
            if not _is_supported_a_share_code(code):
                continue
            market_meta = _infer_market_meta(code)
            records.append(
                {
                    "stock_code": _plain_stock_code(code),
                    "stock_name": name,
                    "market_type": market_meta["market_type"],
                    "exchange": market_meta["exchange"],
                    "board": market_meta["board"],
                    "sec_type": market_meta["sec_type"],
                    "status": int(trade_status or 1),
                    "source": "baostock",
                }
            )
        return records
    finally:
        if managed_client:
            _baostock_logout(bs_client)


def _fetch_stock_basics_with_baostock(bs_client=None):
    """获取股票基础资料"""
    managed_client = bs_client is None
    if bs_client is None:
        bs_client = _baostock_login()

    try:
        result = _baostock_query(bs_client, "query_stock_basic")
        if result.error_code != "0":
            raise RuntimeError(f"baostock 基础资料同步失败: {result.error_msg}")

        records = {}
        while result.next():
            code, code_name, ipo_date, out_date, sec_type, status = result.get_row_data()
            if sec_type != "1" or not _is_supported_a_share_code(code):
                continue
            market_meta = _infer_market_meta(code)
            records[_plain_stock_code(code)] = {
                "stock_code": _plain_stock_code(code),
                "stock_name": code_name,
                "market_type": market_meta["market_type"],
                "exchange": market_meta["exchange"],
                "board": market_meta["board"],
                "sec_type": "A_STOCK" if sec_type == "1" else f"TYPE_{sec_type}",
                "list_date": _safe_text(ipo_date),
                "delist_date": _safe_text(out_date),
                "status": _safe_int(status) if _safe_int(status) is not None else 1,
                "source": "baostock",
            }
        return records
    finally:
        if managed_client:
            _baostock_logout(bs_client)


def _fetch_stock_industries_with_baostock(bs_client=None):
    """获取股票行业归属"""
    managed_client = bs_client is None
    if bs_client is None:
        bs_client = _baostock_login()

    try:
        result = _baostock_query(bs_client, "query_stock_industry")
        if result.error_code != "0":
            raise RuntimeError(f"baostock 行业同步失败: {result.error_msg}")

        industries = {}
        memberships = []
        stock_industry_map = {}

        while result.next():
            update_date, code, code_name, industry_name, industry_source = result.get_row_data()
            if not _is_supported_a_share_code(code):
                continue

            industry_name = _safe_text(industry_name)
            industry_source = _safe_text(industry_source) or "baostock"
            if not industry_name:
                continue

            industry_code = _industry_key(industry_source, industry_name)
            industries[industry_code] = {
                "industry_code": industry_code,
                "industry_name": industry_name,
                "industry_source": industry_source,
                "parent_code": None,
                "level": 1,
            }
            memberships.append(
                {
                    "stock_code": _plain_stock_code(code),
                    "industry_source": industry_source,
                    "industry_code": industry_code,
                    "industry_name": industry_name,
                    "level": 1,
                    "effective_date": update_date or datetime.now().date().isoformat(),
                    "expire_date": None,
                }
            )
            stock_industry_map[_plain_stock_code(code)] = industry_code

        return list(industries.values()), memberships, stock_industry_map
    finally:
        if managed_client:
            _baostock_logout(bs_client)


def _fetch_stock_profile_with_akshare(stock_code: str, retries: int = 3, retry_interval: float = 1.0):
    """获取当前股票详情快照，优先使用雪球，东财作为补充"""
    from akshare.stock.stock_xq import stock_individual_spot_xq

    result = {
        "stock_code": stock_code,
        "list_date": None,
        "total_shares": None,
        "float_shares": None,
        "market_cap": None,
        "float_market_cap": None,
        "industry_name": None,
        "source": None,
    }
    errors = []

    try:
        profile_df = stock_individual_spot_xq(symbol=_to_xueqiu_symbol(stock_code), timeout=15)
        profile = dict(zip(profile_df["item"], profile_df["value"]))
        result.update(
            {
                "total_shares": _safe_float(profile.get("基金份额/总股本")),
                "float_shares": _safe_float(profile.get("流通股")),
                "market_cap": _safe_float(profile.get("资产净值/总市值")),
                "float_market_cap": _safe_float(profile.get("流通值")),
                "source": "xueqiu",
            }
        )
    except Exception as exc:
        errors.append(f"xueqiu={exc}")

    try:
        import akshare as ak

        last_error = None
        profile_df = None
        for attempt in range(1, retries + 1):
            try:
                profile_df = ak.stock_individual_info_em(symbol=stock_code)
                break
            except Exception as exc:
                last_error = exc
                if attempt < retries:
                    time.sleep(retry_interval * attempt)

        if profile_df is None:
            raise last_error or RuntimeError(f"{stock_code} 东财详情抓取失败")

        profile = dict(zip(profile_df["item"], profile_df["value"]))
        result["list_date"] = result["list_date"] or _normalize_date_text(profile.get("上市时间"))
        result["total_shares"] = result["total_shares"] or _safe_float(profile.get("总股本"))
        result["float_shares"] = result["float_shares"] or _safe_float(profile.get("流通股"))
        result["market_cap"] = result["market_cap"] or _safe_float(profile.get("总市值"))
        result["float_market_cap"] = result["float_market_cap"] or _safe_float(profile.get("流通市值"))
        result["industry_name"] = result["industry_name"] or _safe_text(profile.get("行业"))
        result["source"] = result["source"] or "akshare"
    except Exception as exc:
        errors.append(f"eastmoney={exc}")

    if not any(
        result.get(field) is not None
        for field in (
            "list_date",
            "total_shares",
            "float_shares",
            "market_cap",
            "float_market_cap",
            "industry_name",
        )
    ):
        raise RuntimeError("; ".join(errors) or f"{stock_code} 股票详情抓取失败")

    return result


def _fetch_daily_kline_with_akshare(
    stock_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """从 akshare 回退获取原始日线数据"""
    import akshare as ak
    import pandas as pd

    start_date = start_date or "2024-01-01"
    end_date = end_date or datetime.now().date().isoformat()
    start_dt = date.fromisoformat(start_date)
    end_dt = date.fromisoformat(end_date)
    chunk_frames = []
    cursor = start_dt

    while cursor <= end_dt:
        chunk_end = min(cursor + timedelta(days=540), end_dt)
        chunk_label = (
            f"{stock_code} akshare 日线抓取 {cursor.isoformat()}~{chunk_end.isoformat()}"
            if cursor != start_dt or chunk_end != end_dt
            else f"{stock_code} akshare 日线抓取"
        )
        chunk_df = _retry_call(
            chunk_label,
            lambda start=cursor, end=chunk_end: ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start.isoformat().replace("-", ""),
                end_date=end.isoformat().replace("-", ""),
                adjust="",
            ),
        )
        if chunk_df is not None and not chunk_df.empty:
            chunk_frames.append(chunk_df)
        cursor = chunk_end + timedelta(days=1)

    if not chunk_frames:
        return []

    kline_df = pd.concat(chunk_frames, ignore_index=True) if len(chunk_frames) > 1 else chunk_frames[0]
    if "日期" in kline_df.columns:
        kline_df = kline_df.drop_duplicates(subset=["日期"]).sort_values(by="日期").reset_index(drop=True)

    records = []
    previous_close = None
    for _, row in kline_df.iterrows():
        close_price = _safe_float(row["收盘"])
        pre_close = previous_close
        pct_change = None
        if previous_close not in (None, 0):
            pct_change = round((close_price - previous_close) / previous_close * 100, 6)

        volume = _safe_int(row["成交量"])
        if volume is None:
            volume = 0

        records.append(
            {
                "trade_date": str(row["日期"]),
                "open_price": _safe_float(row["开盘"]),
                "high_price": _safe_float(row["最高"]),
                "low_price": _safe_float(row["最低"]),
                "close_price": close_price,
                "pre_close": pre_close,
                "pct_change": pct_change,
                "volume": volume,
                "amount": _safe_float(row["成交额"]),
                "turnover_rate": _safe_float(row.get("换手率", 0)),
                "pe_ratio": None,
                "pb_ratio": None,
                "ps_ttm": None,
                "pcf_ttm": None,
                "tradestatus": 1,
                "is_st": None,
                "source": "akshare",
                "price_mode": "raw",
            }
        )
        previous_close = close_price
    return records


def _fetch_daily_kline_with_baostock(
    stock_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    bs_client=None,
):
    """从 baostock 获取不复权日线数据"""
    managed_client = bs_client is None
    if bs_client is None:
        bs_client = _baostock_login()

    start_date = start_date or "2024-01-01"
    end_date = end_date or datetime.now().date().isoformat()
    try:
        result = _baostock_query(
            bs_client,
            "query_history_k_data_plus",
            _to_baostock_code(stock_code),
            "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,tradestatus,isST",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3",
        )
        if result.error_code != "0":
            raise RuntimeError(f"baostock 日线查询失败: {result.error_msg}")

        records = []
        while result.next():
            row = result.get_row_data()
            tradestatus = _safe_int(row[15]) if _safe_int(row[15]) is not None else 1
            volume = _safe_int(row[7])
            amount = _safe_float(row[8])
            turnover_rate = _safe_float(row[9])
            pct_change = _safe_float(row[10])

            # 停牌日会返回空成交量等字段，入库前统一修正为 0，避免整只股票回填失败。
            if volume is None and tradestatus != 1:
                volume = 0
                amount = amount if amount is not None else 0.0
                turnover_rate = turnover_rate if turnover_rate is not None else 0.0
                pct_change = pct_change if pct_change is not None else 0.0

            records.append(
                {
                    "trade_date": row[0],
                    "open_price": _safe_float(row[2]),
                    "high_price": _safe_float(row[3]),
                    "low_price": _safe_float(row[4]),
                    "close_price": _safe_float(row[5]),
                    "pre_close": _safe_float(row[6]),
                    "volume": volume,
                    "amount": amount,
                    "turnover_rate": turnover_rate,
                    "pct_change": pct_change,
                    "pe_ratio": _safe_float(row[11]),
                    "pb_ratio": _safe_float(row[12]),
                    "ps_ttm": _safe_float(row[13]),
                    "pcf_ttm": _safe_float(row[14]),
                    "tradestatus": tradestatus,
                    "is_st": _safe_int(row[16]) if _safe_int(row[16]) is not None else 0,
                    "source": "baostock",
                    "price_mode": "raw",
                }
            )
        return records
    finally:
        if managed_client:
            _baostock_logout(bs_client)


def _fetch_adjust_factors_with_baostock(stock_code: str, bs_client=None):
    """获取复权因子"""
    managed_client = bs_client is None
    if bs_client is None:
        bs_client = _baostock_login()

    try:
        result = _baostock_query(
            bs_client,
            "query_adjust_factor",
            code=_to_baostock_code(stock_code),
            start_date=rolling_history_start_text(),
            end_date=datetime.now().date().isoformat(),
        )
        if result.error_code != "0":
            raise RuntimeError(f"baostock 复权因子同步失败: {result.error_msg}")

        records = []
        while result.next():
            code, trade_date, forward_factor, backward_factor, _ = result.get_row_data()
            records.append(
                {
                    "stock_code": _plain_stock_code(code),
                    "trade_date": trade_date,
                    "forward_factor": _safe_float(forward_factor),
                    "backward_factor": _safe_float(backward_factor),
                    "source": "baostock",
                }
            )
        return records
    finally:
        if managed_client:
            _baostock_logout(bs_client)


def _fetch_corporate_actions_with_baostock(stock_code: str, years_back: int = 3, bs_client=None):
    """获取股息分红数据"""
    managed_client = bs_client is None
    if bs_client is None:
        bs_client = _baostock_login()

    try:
        current_year = datetime.now().year
        records = []
        for year in range(current_year - years_back + 1, current_year + 1):
            result = _baostock_query(
                bs_client,
                "query_dividend_data",
                code=_to_baostock_code(stock_code),
                year=str(year),
                yearType="report",
            )
            if result.error_code != "0":
                raise RuntimeError(f"baostock 分红同步失败: {result.error_msg}")

            while result.next():
                row = result.get_row_data()
                ex_date = _safe_text(row[6])
                if not ex_date:
                    continue
                records.append(
                    {
                        "stock_code": stock_code,
                        "ex_date": ex_date,
                        "action_type": "dividend",
                        "report_year": str(year),
                        "cash_dividend_pre_tax": _safe_float(row[9]),
                        "cash_dividend_after_tax": _safe_float(row[10]),
                        "stock_dividend_ratio": _safe_float(row[11]),
                        "reserve_to_stock_ratio": _safe_float(row[13]),
                        "plan_announce_date": _safe_text(row[2]),
                        "register_date": _safe_text(row[5]),
                        "pay_date": _safe_text(row[7]),
                        "source": "baostock",
                        "raw_plan": _safe_text(row[12]),
                    }
                )
        return records
    finally:
        if managed_client:
            _baostock_logout(bs_client)


def _upsert_trading_calendar_rows(calendar_rows):
    """批量写入交易日历"""
    sql = """
        INSERT INTO trading_calendar (trade_date, exchange, is_open, prev_trade_date, next_trade_date)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(trade_date) DO UPDATE SET
            exchange = excluded.exchange,
            is_open = excluded.is_open,
            prev_trade_date = excluded.prev_trade_date,
            next_trade_date = excluded.next_trade_date
    """
    params = [
        (
            row["trade_date"],
            row["exchange"],
            row["is_open"],
            row["prev_trade_date"],
            row["next_trade_date"],
        )
        for row in calendar_rows
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_stock_records(stock_records):
    """批量写入股票列表与主数据"""
    sql = """
        INSERT INTO stocks (
            stock_code, stock_name, market_type, exchange, board, sec_type,
            list_date, delist_date, status, is_st_current, total_shares,
            float_shares, industry_code, source, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code) DO UPDATE SET
            stock_name = excluded.stock_name,
            market_type = COALESCE(excluded.market_type, stocks.market_type),
            exchange = COALESCE(excluded.exchange, stocks.exchange),
            board = COALESCE(excluded.board, stocks.board),
            sec_type = COALESCE(excluded.sec_type, stocks.sec_type),
            list_date = COALESCE(excluded.list_date, stocks.list_date),
            delist_date = COALESCE(excluded.delist_date, stocks.delist_date),
            status = COALESCE(excluded.status, stocks.status),
            is_st_current = COALESCE(excluded.is_st_current, stocks.is_st_current),
            total_shares = COALESCE(excluded.total_shares, stocks.total_shares),
            float_shares = COALESCE(excluded.float_shares, stocks.float_shares),
            industry_code = COALESCE(excluded.industry_code, stocks.industry_code),
            source = COALESCE(excluded.source, stocks.source),
            updated_at = excluded.updated_at
    """
    params = [
        (
            row["stock_code"],
            row["stock_name"],
            row.get("market_type"),
            row.get("exchange"),
            row.get("board"),
            row.get("sec_type"),
            row.get("list_date"),
            row.get("delist_date"),
            row.get("status", 1),
            row.get("is_st_current", 0),
            row.get("total_shares"),
            row.get("float_shares"),
            row.get("industry_code"),
            row.get("source"),
            datetime.now(),
        )
        for row in stock_records
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_index_records(index_records):
    """批量写入指数池"""
    if not index_records:
        return
    sql = """
        INSERT INTO indices (
            index_code, index_name, market_type, exchange, index_type,
            list_date, delist_date, status, source, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(index_code) DO UPDATE SET
            index_name = excluded.index_name,
            market_type = COALESCE(excluded.market_type, indices.market_type),
            exchange = COALESCE(excluded.exchange, indices.exchange),
            index_type = COALESCE(excluded.index_type, indices.index_type),
            list_date = COALESCE(excluded.list_date, indices.list_date),
            delist_date = COALESCE(excluded.delist_date, indices.delist_date),
            status = COALESCE(excluded.status, indices.status),
            source = COALESCE(excluded.source, indices.source),
            updated_at = excluded.updated_at
    """
    now = datetime.now()
    params = [
        (
            row["index_code"],
            row["index_name"],
            row.get("market_type"),
            row.get("exchange"),
            row.get("index_type"),
            row.get("list_date"),
            row.get("delist_date"),
            row.get("status", 1),
            row.get("source"),
            now,
        )
        for row in index_records
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _update_stock_profile_fields(profile_rows):
    """更新股票补充字段，避免部分 upsert 命中非空约束"""
    if not profile_rows:
        return
    sql = """
        UPDATE stocks
        SET
            list_date = COALESCE(?, list_date),
            total_shares = COALESCE(?, total_shares),
            float_shares = COALESCE(?, float_shares),
            source = COALESCE(?, source),
            updated_at = ?
        WHERE stock_code = ?
    """
    now = datetime.now()
    params = [
        (
            row.get("list_date"),
            row.get("total_shares"),
            row.get("float_shares"),
            row.get("source"),
            now,
            row["stock_code"],
        )
        for row in profile_rows
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _update_stock_runtime_flags(flag_rows):
    """更新股票运行态字段"""
    if not flag_rows:
        return
    sql = """
        UPDATE stocks
        SET
            board = COALESCE(?, board),
            is_st_current = COALESCE(?, is_st_current),
            updated_at = ?
        WHERE stock_code = ?
    """
    now = datetime.now()
    params = [
        (
            row.get("board"),
            row.get("is_st_current"),
            now,
            row["stock_code"],
        )
        for row in flag_rows
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_industry_records(industry_records):
    """批量写入行业字典"""
    if not industry_records:
        return
    sql = """
        INSERT INTO industries (industry_code, industry_name, industry_source, parent_code, level)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(industry_code) DO UPDATE SET
            industry_name = excluded.industry_name,
            industry_source = excluded.industry_source,
            parent_code = excluded.parent_code,
            level = excluded.level
    """
    params = [
        (
            row["industry_code"],
            row["industry_name"],
            row["industry_source"],
            row["parent_code"],
            row["level"],
        )
        for row in industry_records
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_industry_membership_records(membership_records):
    """批量写入行业归属历史"""
    if not membership_records:
        return
    sql = """
        INSERT INTO industry_membership_history (
            stock_code, industry_source, industry_code, industry_name,
            level, effective_date, expire_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, industry_source, effective_date) DO UPDATE SET
            industry_code = excluded.industry_code,
            industry_name = excluded.industry_name,
            level = excluded.level,
            expire_date = excluded.expire_date
    """
    params = [
        (
            row["stock_code"],
            row["industry_source"],
            row["industry_code"],
            row["industry_name"],
            row["level"],
            row["effective_date"],
            row["expire_date"],
        )
        for row in membership_records
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_share_capital_snapshot(profile_records):
    """写入当前股本快照"""
    sql = """
        INSERT INTO share_capital_history (
            stock_code, effective_date, total_shares, float_shares,
            free_float_shares, source
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, effective_date) DO UPDATE SET
            total_shares = excluded.total_shares,
            float_shares = excluded.float_shares,
            free_float_shares = excluded.free_float_shares,
            source = excluded.source
    """
    today = datetime.now().date().isoformat()
    params = [
        (
            row["stock_code"],
            today,
            row.get("total_shares"),
            row.get("float_shares"),
            row.get("float_shares"),
            row.get("source"),
        )
        for row in profile_records
        if row.get("total_shares") is not None or row.get("float_shares") is not None
    ]
    if not params:
        return
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_valuation_snapshot_rows(valuation_rows):
    """批量写入估值快照"""
    if not valuation_rows:
        return
    sql = """
        INSERT INTO daily_valuation_snapshot (
            stock_code, trade_date, market_cap, float_market_cap,
            pe_ttm, pb_mrq, ps_ttm, pcf_ttm, dividend_yield, source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, trade_date) DO UPDATE SET
            market_cap = COALESCE(excluded.market_cap, daily_valuation_snapshot.market_cap),
            float_market_cap = COALESCE(excluded.float_market_cap, daily_valuation_snapshot.float_market_cap),
            pe_ttm = COALESCE(excluded.pe_ttm, daily_valuation_snapshot.pe_ttm),
            pb_mrq = COALESCE(excluded.pb_mrq, daily_valuation_snapshot.pb_mrq),
            ps_ttm = COALESCE(excluded.ps_ttm, daily_valuation_snapshot.ps_ttm),
            pcf_ttm = COALESCE(excluded.pcf_ttm, daily_valuation_snapshot.pcf_ttm),
            dividend_yield = COALESCE(excluded.dividend_yield, daily_valuation_snapshot.dividend_yield),
            source = COALESCE(excluded.source, daily_valuation_snapshot.source)
    """
    params = [
        (
            row["stock_code"],
            row["trade_date"],
            row.get("market_cap"),
            row.get("float_market_cap"),
            row.get("pe_ttm"),
            row.get("pb_mrq"),
            row.get("ps_ttm"),
            row.get("pcf_ttm"),
            row.get("dividend_yield"),
            row.get("source"),
        )
        for row in valuation_rows
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_daily_kline_rows(stock_code: str, kline_rows):
    """单只股票批量写入日线数据"""
    sql = """
        INSERT INTO daily_kline (
            stock_code, trade_date, open_price, high_price, low_price, close_price,
            pre_close, pct_change, volume, amount, turnover_rate,
            pe_ratio, pb_ratio, source, price_mode
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, trade_date) DO UPDATE SET
            open_price = excluded.open_price,
            high_price = excluded.high_price,
            low_price = excluded.low_price,
            close_price = excluded.close_price,
            pre_close = excluded.pre_close,
            pct_change = excluded.pct_change,
            volume = excluded.volume,
            amount = excluded.amount,
            turnover_rate = excluded.turnover_rate,
            pe_ratio = COALESCE(excluded.pe_ratio, daily_kline.pe_ratio),
            pb_ratio = COALESCE(excluded.pb_ratio, daily_kline.pb_ratio),
            source = excluded.source,
            price_mode = excluded.price_mode
    """
    params = [
        (
            stock_code,
            row["trade_date"],
            row["open_price"],
            row["high_price"],
            row["low_price"],
            row["close_price"],
            row.get("pre_close"),
            row.get("pct_change"),
            row["volume"],
            row["amount"],
            row.get("turnover_rate"),
            row.get("pe_ratio"),
            row.get("pb_ratio"),
            row.get("source"),
            row.get("price_mode"),
        )
        for row in kline_rows
        if row.get("trade_date")
        and row.get("open_price") is not None
        and row.get("high_price") is not None
        and row.get("low_price") is not None
        and row.get("close_price") is not None
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_daily_trade_flag_rows(flag_rows):
    """批量写入交易状态"""
    if not flag_rows:
        return
    sql = """
        INSERT INTO daily_trade_flags (
            stock_code, trade_date, is_suspended, is_st, is_limit_up,
            is_limit_down, limit_up_price, limit_down_price, board
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, trade_date) DO UPDATE SET
            is_suspended = excluded.is_suspended,
            is_st = excluded.is_st,
            is_limit_up = excluded.is_limit_up,
            is_limit_down = excluded.is_limit_down,
            limit_up_price = excluded.limit_up_price,
            limit_down_price = excluded.limit_down_price,
            board = excluded.board
    """
    params = [
        (
            row["stock_code"],
            row["trade_date"],
            row["is_suspended"],
            row["is_st"],
            row["is_limit_up"],
            row["is_limit_down"],
            row["limit_up_price"],
            row["limit_down_price"],
            row["board"],
        )
        for row in flag_rows
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _list_daily_kline_gap_candidates(
    limit: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """按真实缺口优先级选择需要补齐日线的股票"""
    try:
        reference_end = date.fromisoformat(end_date) if end_date else None
    except ValueError:
        reference_end = None

    window_start = start_date or rolling_history_start_text(reference=reference_end)
    window_end = end_date or datetime.now().date().isoformat()

    sql = """
        WITH stock_base AS (
            SELECT
                stock_code,
                stock_name,
                board,
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
        actual AS (
            SELECT
                stock_code,
                COUNT(*) AS actual_days,
                MAX(trade_date) AS last_date
            FROM daily_kline
            WHERE trade_date BETWEEN ? AND ?
            GROUP BY stock_code
        ),
        coverage AS (
            SELECT
                s.stock_code,
                s.stock_name,
                s.board,
                COALESCE(a.actual_days, 0) AS actual_days,
                a.last_date,
                (
                    SELECT COUNT(*)
                    FROM trading_calendar tc
                    WHERE tc.is_open = 1
                      AND tc.trade_date BETWEEN s.effective_start AND s.effective_end
                ) AS expected_days,
                CASE
                    WHEN a.last_date IS NULL THEN 0
                    ELSE (
                        SELECT COUNT(*)
                        FROM trading_calendar tc
                        WHERE tc.is_open = 1
                          AND tc.trade_date BETWEEN s.effective_start AND MIN(a.last_date, s.effective_end)
                    )
                END AS expected_to_last
            FROM stock_base s
            LEFT JOIN actual a
              ON a.stock_code = s.stock_code
            WHERE s.effective_start <= s.effective_end
        )
        SELECT
            stock_code,
            stock_name,
            board,
            actual_days,
            last_date,
            expected_days,
            expected_to_last,
            expected_days - actual_days AS missing_days,
            MAX(expected_to_last - actual_days, 0) AS history_missing_days,
            expected_days - expected_to_last AS recent_missing_days
        FROM coverage
        WHERE expected_days > actual_days
        ORDER BY
            missing_days DESC,
            history_missing_days DESC,
            recent_missing_days DESC,
            actual_days ASC,
            COALESCE(last_date, '0000-00-00') ASC,
            stock_code ASC
    """
    params: tuple = (window_start, window_start, window_end, window_end, window_start, window_end)
    if limit is not None:
        sql += " LIMIT ?"
        params = (*params, limit)

    rows = db.fetchall(sql, params)
    for row in rows:
        actual_days = int(row.get("actual_days") or 0)
        history_missing_days = int(row.get("history_missing_days") or 0)
        if actual_days <= 0:
            row["selection_reason"] = "no_kline"
        elif history_missing_days > 0:
            row["selection_reason"] = "history_gap"
        else:
            row["selection_reason"] = "recent_gap"
    return rows


def _resolve_daily_kline_fetch_window(
    stock_row: dict,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """为单只股票决定抓取窗口，历史缺口拉全量，近期滞后只拉近端窗口"""
    resolved_end = end_date or datetime.now().date().isoformat()
    if start_date:
        return start_date, resolved_end

    try:
        reference_end = date.fromisoformat(resolved_end)
    except ValueError:
        reference_end = datetime.now().date()
    history_start = rolling_history_start_text(reference=reference_end)

    actual_days = int(stock_row.get("actual_days") or 0)
    history_missing_days = int(stock_row.get("history_missing_days") or 0)
    last_date = stock_row.get("last_date")
    if actual_days <= 0 or history_missing_days > 0 or not last_date:
        return history_start, resolved_end

    try:
        last_trade_date = date.fromisoformat(last_date)
        history_start_date = date.fromisoformat(history_start)
    except ValueError:
        return history_start, resolved_end

    recent_start_date = max(last_trade_date - timedelta(days=15), history_start_date)
    return recent_start_date.isoformat(), resolved_end


def _upsert_adjust_factor_rows(adjust_rows):
    """批量写入复权因子"""
    if not adjust_rows:
        return
    sql = """
        INSERT INTO adjust_factors (stock_code, trade_date, forward_factor, backward_factor, source)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, trade_date) DO UPDATE SET
            forward_factor = excluded.forward_factor,
            backward_factor = excluded.backward_factor,
            source = excluded.source
    """
    params = [
        (
            row["stock_code"],
            row["trade_date"],
            row.get("forward_factor"),
            row.get("backward_factor"),
            row.get("source"),
        )
        for row in adjust_rows
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_corporate_action_rows(action_rows):
    """批量写入公司行为"""
    if not action_rows:
        return
    sql = """
        INSERT INTO corporate_actions (
            stock_code, ex_date, action_type, report_year, cash_dividend_pre_tax,
            cash_dividend_after_tax, stock_dividend_ratio, reserve_to_stock_ratio,
            plan_announce_date, register_date, pay_date, source, raw_plan
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, ex_date, action_type) DO UPDATE SET
            report_year = excluded.report_year,
            cash_dividend_pre_tax = excluded.cash_dividend_pre_tax,
            cash_dividend_after_tax = excluded.cash_dividend_after_tax,
            stock_dividend_ratio = excluded.stock_dividend_ratio,
            reserve_to_stock_ratio = excluded.reserve_to_stock_ratio,
            plan_announce_date = excluded.plan_announce_date,
            register_date = excluded.register_date,
            pay_date = excluded.pay_date,
            source = excluded.source,
            raw_plan = excluded.raw_plan
    """
    params = [
        (
            row["stock_code"],
            row["ex_date"],
            row["action_type"],
            row.get("report_year"),
            row.get("cash_dividend_pre_tax"),
            row.get("cash_dividend_after_tax"),
            row.get("stock_dividend_ratio"),
            row.get("reserve_to_stock_ratio"),
            row.get("plan_announce_date"),
            row.get("register_date"),
            row.get("pay_date"),
            row.get("source"),
            row.get("raw_plan"),
        )
        for row in action_rows
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def _upsert_financial_report_rows(report_rows):
    """批量写入财务报表"""
    if not report_rows:
        return
    sql = """
        INSERT INTO financial_reports (
            stock_code, report_period, report_type, announce_date, report_period_end,
            statement_type, currency, total_assets, total_liabilities, net_assets,
            revenue, net_profit, eps, roe, gross_margin, debt_ratio
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, report_period) DO UPDATE SET
            report_type = excluded.report_type,
            announce_date = COALESCE(excluded.announce_date, financial_reports.announce_date),
            report_period_end = COALESCE(excluded.report_period_end, financial_reports.report_period_end),
            statement_type = COALESCE(excluded.statement_type, financial_reports.statement_type),
            currency = COALESCE(excluded.currency, financial_reports.currency),
            total_assets = COALESCE(excluded.total_assets, financial_reports.total_assets),
            total_liabilities = COALESCE(excluded.total_liabilities, financial_reports.total_liabilities),
            net_assets = COALESCE(excluded.net_assets, financial_reports.net_assets),
            revenue = COALESCE(excluded.revenue, financial_reports.revenue),
            net_profit = COALESCE(excluded.net_profit, financial_reports.net_profit),
            eps = COALESCE(excluded.eps, financial_reports.eps),
            roe = COALESCE(excluded.roe, financial_reports.roe),
            gross_margin = COALESCE(excluded.gross_margin, financial_reports.gross_margin),
            debt_ratio = COALESCE(excluded.debt_ratio, financial_reports.debt_ratio)
    """
    params = [
        (
            row["stock_code"],
            row["report_period"],
            row["report_type"],
            row.get("announce_date"),
            row.get("report_period_end"),
            row.get("statement_type"),
            row.get("currency"),
            row.get("total_assets"),
            row.get("total_liabilities"),
            row.get("net_assets"),
            row.get("revenue"),
            row.get("net_profit"),
            row.get("eps"),
            row.get("roe"),
            row.get("gross_margin"),
            row.get("debt_ratio"),
        )
        for row in report_rows
    ]
    with db.get_connection() as conn:
        conn.executemany(sql, params)


def log_sync_start(sync_type: str) -> int:
    """记录同步开始"""
    sql = """
        INSERT INTO sync_logs (sync_type, start_time, status, total_count, success_count, fail_count)
        VALUES (?, ?, 'running', 0, 0, 0)
    """
    with db.get_connection() as conn:
        cursor = conn.execute(sql, (sync_type, datetime.now()))
        return cursor.lastrowid or 0


def update_sync_checkpoint(
    log_id: Optional[int],
    checkpoint: Optional[dict] = None,
    total: Optional[int] = None,
    success: Optional[int] = None,
    fail: Optional[int] = None,
):
    """更新同步过程中的阶段和进度"""
    if not log_id:
        return

    checkpoint_text = json.dumps(checkpoint, ensure_ascii=False) if checkpoint is not None else None
    sql = """
        UPDATE sync_logs
        SET checkpoint_info = COALESCE(?, checkpoint_info),
            total_count = COALESCE(?, total_count),
            success_count = COALESCE(?, success_count),
            fail_count = COALESCE(?, fail_count)
        WHERE log_id = ?
    """
    db.execute(sql, (checkpoint_text, total, success, fail, log_id))


def log_sync_end(log_id: int, status: str, total: int = 0, success: int = 0, fail: int = 0, error: str = None):
    """记录同步结束"""
    sql = """
        UPDATE sync_logs
        SET end_time = ?, status = ?, total_count = ?, success_count = ?, fail_count = ?, error_message = ?
        WHERE log_id = ?
    """
    db.execute(sql, (datetime.now(), status, total, success, fail, error, log_id))


def _task_running_message(task_label: str, exc: TaskAlreadyRunningError) -> str:
    detail_parts = []
    if exc.metadata.get("started_at"):
        detail_parts.append(f"started_at={exc.metadata['started_at']}")
    if exc.metadata.get("pid"):
        detail_parts.append(f"pid={exc.metadata['pid']}")
    detail_text = f" ({', '.join(detail_parts)})" if detail_parts else ""
    return f"{task_label}已在运行，跳过重复触发{detail_text}"


def _launch_async_stock_profile_sync(limit: Optional[int] = 500, only_missing: bool = True) -> bool:
    """异步触发股票详情补充，不阻塞股票池主同步收尾"""
    result = spawn_sync_task(
        "stock_profiles",
        limit=limit,
        only_missing=only_missing,
        manage_log=True,
    )
    if result.get("spawned"):
        return True

    logger.info("股票详情快照同步已在运行，跳过自动触发")
    return False


def _recent_open_trade_dates(limit: int, end_date: Optional[str] = None) -> list[str]:
    sql = """
        SELECT trade_date
        FROM trading_calendar
        WHERE is_open = 1
    """
    params: list = []
    if end_date:
        sql += " AND trade_date <= ?"
        params.append(end_date)
    sql += " ORDER BY trade_date DESC LIMIT ?"
    params.append(max(int(limit or 1), 1))
    rows = db.fetchall(sql, tuple(params))
    return [row["trade_date"] for row in reversed(rows)]


def _average(values: list[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _ratio(numerator: float, denominator: float) -> Optional[float]:
    if denominator in (None, 0):
        return None
    return numerator / denominator


def _round_or_none(value: Optional[float], digits: int = 6):
    if value is None:
        return None
    return round(value, digits)


def _sector_name_from_industry(industry_code: Optional[str]) -> str:
    text = _safe_text(industry_code)
    if not text:
        return "未分类"
    return text.split(":", 1)[-1]


def _sentiment_label_and_summary(
    score: int,
    advancing_ratio: Optional[float],
    above_ma20_ratio: Optional[float],
    avg_pct_change: Optional[float],
    limit_up_count: int,
    limit_down_count: int,
    failed_limit_ratio: Optional[float],
):
    if score >= 78:
        label = "强势扩散"
        summary = "上涨家数、趋势占比和涨停数量同步占优，短线情绪处于扩散阶段。"
    elif score >= 60:
        label = "偏强活跃"
        summary = "热点仍有承接，适合优先围绕强板块和前排个股观察。"
    elif score >= 45:
        label = "震荡分化"
        summary = "市场分化明显，只适合做资金与趋势共振最强的方向。"
    elif score >= 30:
        label = "偏弱谨慎"
        summary = "情绪承接一般，追高性价比偏低，仓位和节奏需要收紧。"
    else:
        label = "退潮防守"
        summary = "涨停承接弱、修复不足，优先防守并等待更明确的修复信号。"

    if failed_limit_ratio is not None and failed_limit_ratio >= 0.35 and limit_up_count > 0:
        summary = "炸板占比偏高，短线承接不足，尽量减少追高。"
    elif (
        advancing_ratio is not None
        and above_ma20_ratio is not None
        and avg_pct_change is not None
        and advancing_ratio >= 0.62
        and above_ma20_ratio >= 0.58
        and avg_pct_change >= 0.8
    ):
        summary = "普涨与趋势共振较强，强势方向更容易走出连续性。"
    elif limit_down_count >= max(6, limit_up_count // 2):
        summary = "跌停与负反馈仍在，强弱切换较快，节奏上宜更保守。"
    return label, summary


def _upsert_market_sentiment_rows(sentiment_rows: list[dict]):
    if not sentiment_rows:
        return

    sql = """
        INSERT INTO market_sentiment_daily (
            trade_date, sample_size, rising_count, falling_count, flat_count,
            strong_up_count, strong_down_count, limit_up_count, limit_down_count,
            failed_limit_count, above_ma20_count, advancing_ratio, above_ma20_ratio,
            limit_up_ratio, failed_limit_ratio, avg_pct_change, sentiment_score,
            sentiment_label, summary, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(trade_date) DO UPDATE SET
            sample_size = excluded.sample_size,
            rising_count = excluded.rising_count,
            falling_count = excluded.falling_count,
            flat_count = excluded.flat_count,
            strong_up_count = excluded.strong_up_count,
            strong_down_count = excluded.strong_down_count,
            limit_up_count = excluded.limit_up_count,
            limit_down_count = excluded.limit_down_count,
            failed_limit_count = excluded.failed_limit_count,
            above_ma20_count = excluded.above_ma20_count,
            advancing_ratio = excluded.advancing_ratio,
            above_ma20_ratio = excluded.above_ma20_ratio,
            limit_up_ratio = excluded.limit_up_ratio,
            failed_limit_ratio = excluded.failed_limit_ratio,
            avg_pct_change = excluded.avg_pct_change,
            sentiment_score = excluded.sentiment_score,
            sentiment_label = excluded.sentiment_label,
            summary = excluded.summary,
            updated_at = excluded.updated_at
    """
    with db.get_connection() as conn:
        conn.executemany(
            sql,
            [
                (
                    row["trade_date"],
                    row["sample_size"],
                    row["rising_count"],
                    row["falling_count"],
                    row["flat_count"],
                    row["strong_up_count"],
                    row["strong_down_count"],
                    row["limit_up_count"],
                    row["limit_down_count"],
                    row["failed_limit_count"],
                    row["above_ma20_count"],
                    row.get("advancing_ratio"),
                    row.get("above_ma20_ratio"),
                    row.get("limit_up_ratio"),
                    row.get("failed_limit_ratio"),
                    row.get("avg_pct_change"),
                    row["sentiment_score"],
                    row.get("sentiment_label"),
                    row.get("summary"),
                    row["updated_at"],
                )
                for row in sentiment_rows
            ],
        )


def _replace_sector_strength_rows(trade_date: str, sector_rows: list[dict]):
    with db.get_connection() as conn:
        conn.execute("DELETE FROM sector_strength_daily WHERE trade_date = ?", (trade_date,))
        if not sector_rows:
            return
        conn.executemany(
            """
            INSERT INTO sector_strength_daily (
                trade_date, sector_name, stock_count, rising_count, limit_up_count,
                avg_pct_change, avg_return_5d, above_ma20_ratio, strength_score,
                leading_stock_code, leading_stock_name, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["trade_date"],
                    row["sector_name"],
                    row["stock_count"],
                    row["rising_count"],
                    row["limit_up_count"],
                    row.get("avg_pct_change"),
                    row.get("avg_return_5d"),
                    row.get("above_ma20_ratio"),
                    row["strength_score"],
                    row.get("leading_stock_code"),
                    row.get("leading_stock_name"),
                    row["updated_at"],
                )
                for row in sector_rows
            ],
        )


def _replace_stock_event_rows(trade_date: str, event_rows: list[dict]):
    with db.get_connection() as conn:
        conn.execute("DELETE FROM stock_event_signals_daily WHERE trade_date = ?", (trade_date,))
        if not event_rows:
            return
        conn.executemany(
            """
            INSERT INTO stock_event_signals_daily (
                trade_date, stock_code, stock_name, sector_name, event_type,
                event_label, event_value, pct_change, consecutive_days, rank_no,
                note, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["trade_date"],
                    row["stock_code"],
                    row["stock_name"],
                    row.get("sector_name"),
                    row["event_type"],
                    row.get("event_label"),
                    row.get("event_value"),
                    row.get("pct_change"),
                    row.get("consecutive_days", 0),
                    row.get("rank_no", 0),
                    row.get("note"),
                    row["updated_at"],
                )
                for row in event_rows
            ],
        )


def _upsert_market_fund_flow_rows(fund_flow_rows: list[dict]):
    if not fund_flow_rows:
        return
    with db.get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO market_fund_flow_daily (
                trade_date, sh_close, sh_pct_change, sz_close, sz_pct_change,
                main_net_inflow, main_net_inflow_ratio, super_large_net_inflow,
                super_large_net_inflow_ratio, large_net_inflow, large_net_inflow_ratio,
                mid_net_inflow, mid_net_inflow_ratio, small_net_inflow,
                small_net_inflow_ratio, source, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date) DO UPDATE SET
                sh_close = excluded.sh_close,
                sh_pct_change = excluded.sh_pct_change,
                sz_close = excluded.sz_close,
                sz_pct_change = excluded.sz_pct_change,
                main_net_inflow = excluded.main_net_inflow,
                main_net_inflow_ratio = excluded.main_net_inflow_ratio,
                super_large_net_inflow = excluded.super_large_net_inflow,
                super_large_net_inflow_ratio = excluded.super_large_net_inflow_ratio,
                large_net_inflow = excluded.large_net_inflow,
                large_net_inflow_ratio = excluded.large_net_inflow_ratio,
                mid_net_inflow = excluded.mid_net_inflow,
                mid_net_inflow_ratio = excluded.mid_net_inflow_ratio,
                small_net_inflow = excluded.small_net_inflow,
                small_net_inflow_ratio = excluded.small_net_inflow_ratio,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            [
                (
                    row["trade_date"],
                    row.get("sh_close"),
                    row.get("sh_pct_change"),
                    row.get("sz_close"),
                    row.get("sz_pct_change"),
                    row.get("main_net_inflow"),
                    row.get("main_net_inflow_ratio"),
                    row.get("super_large_net_inflow"),
                    row.get("super_large_net_inflow_ratio"),
                    row.get("large_net_inflow"),
                    row.get("large_net_inflow_ratio"),
                    row.get("mid_net_inflow"),
                    row.get("mid_net_inflow_ratio"),
                    row.get("small_net_inflow"),
                    row.get("small_net_inflow_ratio"),
                    row.get("source"),
                    row["updated_at"],
                )
                for row in fund_flow_rows
            ],
        )


def _replace_sector_fund_flow_rows(trade_date: str, sector_type: str, rows: list[dict]):
    with db.get_connection() as conn:
        conn.execute(
            "DELETE FROM sector_fund_flow_daily WHERE trade_date = ? AND sector_type = ?",
            (trade_date, sector_type),
        )
        if not rows:
            return
        conn.executemany(
            """
            INSERT INTO sector_fund_flow_daily (
                trade_date, sector_type, sector_name, rank_no, pct_change,
                main_net_inflow, main_net_inflow_ratio, super_large_net_inflow,
                super_large_net_inflow_ratio, large_net_inflow, large_net_inflow_ratio,
                mid_net_inflow, mid_net_inflow_ratio, small_net_inflow,
                small_net_inflow_ratio, leading_stock_name, source, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["trade_date"],
                    row["sector_type"],
                    row["sector_name"],
                    row.get("rank_no"),
                    row.get("pct_change"),
                    row.get("main_net_inflow"),
                    row.get("main_net_inflow_ratio"),
                    row.get("super_large_net_inflow"),
                    row.get("super_large_net_inflow_ratio"),
                    row.get("large_net_inflow"),
                    row.get("large_net_inflow_ratio"),
                    row.get("mid_net_inflow"),
                    row.get("mid_net_inflow_ratio"),
                    row.get("small_net_inflow"),
                    row.get("small_net_inflow_ratio"),
                    row.get("leading_stock_name"),
                    row.get("source"),
                    row["updated_at"],
                )
                for row in rows
            ],
        )


def _fetch_market_fund_flow_with_akshare() -> list[dict]:
    import akshare as ak

    def _call():
        sink = io.StringIO()
        with redirect_stdout(sink), redirect_stderr(sink):
            return ak.stock_market_fund_flow()

    fund_flow_df = _retry_call("市场资金流抓取", _call)
    if fund_flow_df is None or fund_flow_df.empty:
        return []

    updated_at = datetime.now().isoformat(timespec="seconds")
    rows = []
    for _, row in fund_flow_df.iterrows():
        rows.append(
            {
                "trade_date": _normalize_date_text(row.get("日期")),
                "sh_close": _safe_float(row.get("上证-收盘价")),
                "sh_pct_change": _safe_float(row.get("上证-涨跌幅")),
                "sz_close": _safe_float(row.get("深证-收盘价")),
                "sz_pct_change": _safe_float(row.get("深证-涨跌幅")),
                "main_net_inflow": _safe_float(row.get("主力净流入-净额")),
                "main_net_inflow_ratio": _safe_float(row.get("主力净流入-净占比")),
                "super_large_net_inflow": _safe_float(row.get("超大单净流入-净额")),
                "super_large_net_inflow_ratio": _safe_float(row.get("超大单净流入-净占比")),
                "large_net_inflow": _safe_float(row.get("大单净流入-净额")),
                "large_net_inflow_ratio": _safe_float(row.get("大单净流入-净占比")),
                "mid_net_inflow": _safe_float(row.get("中单净流入-净额")),
                "mid_net_inflow_ratio": _safe_float(row.get("中单净流入-净占比")),
                "small_net_inflow": _safe_float(row.get("小单净流入-净额")),
                "small_net_inflow_ratio": _safe_float(row.get("小单净流入-净占比")),
                "source": "akshare_market_fund_flow",
                "updated_at": updated_at,
            }
        )
    return rows


def _fetch_sector_fund_flow_with_akshare(trade_date: str, sector_type: str) -> list[dict]:
    import akshare as ak

    def _call():
        sink = io.StringIO()
        with redirect_stdout(sink), redirect_stderr(sink):
            return ak.stock_sector_fund_flow_rank(indicator="今日", sector_type=sector_type)

    sector_df = _retry_call(f"{sector_type}抓取", _call)
    if sector_df is None or sector_df.empty:
        return []

    updated_at = datetime.now().isoformat(timespec="seconds")
    rows = []
    for _, row in sector_df.iterrows():
        rows.append(
            {
                "trade_date": trade_date,
                "sector_type": sector_type,
                "sector_name": _safe_text(row.get("名称")),
                "rank_no": _safe_int(row.get("序号")),
                "pct_change": _safe_float(row.get("今日涨跌幅")),
                "main_net_inflow": _safe_float(row.get("今日主力净流入-净额")),
                "main_net_inflow_ratio": _safe_float(row.get("今日主力净流入-净占比")),
                "super_large_net_inflow": _safe_float(row.get("今日超大单净流入-净额")),
                "super_large_net_inflow_ratio": _safe_float(row.get("今日超大单净流入-净占比")),
                "large_net_inflow": _safe_float(row.get("今日大单净流入-净额")),
                "large_net_inflow_ratio": _safe_float(row.get("今日大单净流入-净占比")),
                "mid_net_inflow": _safe_float(row.get("今日中单净流入-净额")),
                "mid_net_inflow_ratio": _safe_float(row.get("今日中单净流入-净占比")),
                "small_net_inflow": _safe_float(row.get("今日小单净流入-净额")),
                "small_net_inflow_ratio": _safe_float(row.get("今日小单净流入-净占比")),
                "leading_stock_name": _safe_text(row.get("今日主力净流入最大股")),
                "source": "akshare_sector_fund_flow",
                "updated_at": updated_at,
            }
        )
    return rows


def _compute_market_overview_snapshot(latest_trade_date: str) -> dict:
    target_dates = _recent_open_trade_dates(MARKET_OVERVIEW_RECENT_DAYS, latest_trade_date)
    if not target_dates:
        return {
            "trade_date": latest_trade_date,
            "sentiment_rows": [],
            "sector_rows": [],
            "event_rows": [],
        }

    warmup_dates = _recent_open_trade_dates(
        MARKET_OVERVIEW_RECENT_DAYS + MARKET_OVERVIEW_WARMUP_EXTRA_DAYS,
        latest_trade_date,
    )
    date_set = set(target_dates)
    latest_streak_dates = set(_recent_open_trade_dates(MARKET_OVERVIEW_EVENT_STREAK_DAYS, latest_trade_date))
    placeholders = ", ".join("?" for _ in warmup_dates)
    rows = db.fetchall(
        f"""
        SELECT
            dk.stock_code,
            s.stock_name,
            s.industry_code,
            dk.trade_date,
            dk.close_price,
            dk.high_price,
            dk.pct_change,
            COALESCE(dtf.is_suspended, 0) AS is_suspended,
            COALESCE(dtf.is_st, 0) AS is_st,
            COALESCE(dtf.is_limit_up, 0) AS is_limit_up,
            COALESCE(dtf.is_limit_down, 0) AS is_limit_down,
            dtf.limit_up_price
        FROM daily_kline dk
        JOIN stocks s
          ON s.stock_code = dk.stock_code
        LEFT JOIN daily_trade_flags dtf
          ON dtf.stock_code = dk.stock_code
         AND dtf.trade_date = dk.trade_date
        WHERE s.status = 1
          AND dk.trade_date IN ({placeholders})
        ORDER BY dk.stock_code ASC, dk.trade_date ASC
        """,
        tuple(warmup_dates),
    )

    updated_at = datetime.now().isoformat(timespec="seconds")
    sentiment_by_date = {
        trade_date: {
            "trade_date": trade_date,
            "sample_size": 0,
            "rising_count": 0,
            "falling_count": 0,
            "flat_count": 0,
            "strong_up_count": 0,
            "strong_down_count": 0,
            "limit_up_count": 0,
            "limit_down_count": 0,
            "failed_limit_count": 0,
            "above_ma20_count": 0,
            "pct_changes": [],
        }
        for trade_date in target_dates
    }
    sector_latest_metrics: dict[str, list[dict]] = defaultdict(list)
    limit_up_events: list[dict] = []
    consecutive_events: list[dict] = []
    failed_limit_events: list[dict] = []

    grouped_rows: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped_rows[row["stock_code"]].append(row)

    for stock_rows in grouped_rows.values():
        closes: list[Optional[float]] = []
        derived_rows: list[dict] = []
        for row in stock_rows:
            close_price = _safe_float(row.get("close_price"))
            high_price = _safe_float(row.get("high_price"))
            pct_change = _safe_float(row.get("pct_change"))
            limit_up_price = _safe_float(row.get("limit_up_price"))
            is_limit_up = int(row.get("is_limit_up") or 0)
            is_limit_down = int(row.get("is_limit_down") or 0)
            is_st = int(row.get("is_st") or 0)
            is_suspended = int(row.get("is_suspended") or 0)

            closes.append(close_price)
            ma20 = None
            if len(closes) >= 20:
                ma_window = closes[-20:]
                if all(value not in (None, 0) for value in ma_window):
                    ma20 = sum(ma_window) / 20

            return_5d = None
            if len(closes) >= 6 and close_price not in (None, 0) and closes[-6] not in (None, 0):
                return_5d = (close_price / closes[-6]) - 1

            failed_limit = bool(
                limit_up_price not in (None, 0)
                and high_price is not None
                and close_price is not None
                and high_price >= (limit_up_price - 0.011)
                and not is_limit_up
                and close_price < (limit_up_price - 0.011)
            )
            gap_to_limit = None
            if limit_up_price not in (None, 0) and close_price is not None:
                gap_to_limit = (close_price / limit_up_price) - 1

            derived = {
                "trade_date": row["trade_date"],
                "stock_code": row["stock_code"],
                "stock_name": row["stock_name"],
                "sector_name": _sector_name_from_industry(row.get("industry_code")),
                "close_price": close_price,
                "pct_change": pct_change,
                "ma20": ma20,
                "return_5d": return_5d,
                "is_limit_up": is_limit_up,
                "is_limit_down": is_limit_down,
                "is_st": is_st,
                "is_suspended": is_suspended,
                "failed_limit": failed_limit,
                "gap_to_limit": gap_to_limit,
            }
            derived_rows.append(derived)

            if row["trade_date"] not in date_set:
                continue
            if is_st or is_suspended or close_price is None:
                continue

            sentiment = sentiment_by_date[row["trade_date"]]
            sentiment["sample_size"] += 1
            sentiment["pct_changes"].append(pct_change or 0.0)
            if (pct_change or 0.0) > 0:
                sentiment["rising_count"] += 1
            elif (pct_change or 0.0) < 0:
                sentiment["falling_count"] += 1
            else:
                sentiment["flat_count"] += 1
            if (pct_change or 0.0) >= 3:
                sentiment["strong_up_count"] += 1
            if (pct_change or 0.0) <= -3:
                sentiment["strong_down_count"] += 1
            if is_limit_up:
                sentiment["limit_up_count"] += 1
            if is_limit_down:
                sentiment["limit_down_count"] += 1
            if failed_limit:
                sentiment["failed_limit_count"] += 1
            if ma20 not in (None, 0) and close_price > ma20:
                sentiment["above_ma20_count"] += 1

            if row["trade_date"] == latest_trade_date:
                sector_latest_metrics[derived["sector_name"]].append(derived)

        latest_row = derived_rows[-1] if derived_rows else None
        if (
            latest_row
            and latest_row["trade_date"] == latest_trade_date
            and not latest_row["is_st"]
            and not latest_row["is_suspended"]
        ):
            streak = 0
            for row in reversed(derived_rows):
                if row["trade_date"] not in latest_streak_dates:
                    continue
                if row["is_limit_up"]:
                    streak += 1
                    continue
                break

            if latest_row["is_limit_up"]:
                base_event = {
                    "trade_date": latest_trade_date,
                    "stock_code": latest_row["stock_code"],
                    "stock_name": latest_row["stock_name"],
                    "sector_name": latest_row["sector_name"],
                    "pct_change": _round_or_none(latest_row["pct_change"], 6),
                    "consecutive_days": streak,
                    "updated_at": updated_at,
                }
                limit_up_events.append(
                    {
                        **base_event,
                        "event_type": "limit_up",
                        "event_label": "涨停",
                        "event_value": 1.0,
                        "note": f"{streak} 连板" if streak >= 2 else "首板/反包",
                    }
                )
                if streak >= 2:
                    consecutive_events.append(
                        {
                            **base_event,
                            "event_type": "consecutive_limit_up",
                            "event_label": f"{streak} 连板",
                            "event_value": float(streak),
                            "note": f"{streak} 连板延续",
                        }
                    )

            if latest_row["failed_limit"]:
                gap_pct = None
                if latest_row["gap_to_limit"] is not None:
                    gap_pct = abs(latest_row["gap_to_limit"]) * 100
                failed_limit_events.append(
                    {
                        "trade_date": latest_trade_date,
                        "stock_code": latest_row["stock_code"],
                        "stock_name": latest_row["stock_name"],
                        "sector_name": latest_row["sector_name"],
                        "event_type": "failed_limit_up",
                        "event_label": "炸板",
                        "event_value": _round_or_none(latest_row["gap_to_limit"], 6),
                        "pct_change": _round_or_none(latest_row["pct_change"], 6),
                        "consecutive_days": 0,
                        "note": f"封板回落 {gap_pct:.2f}%" if gap_pct is not None else "冲板后回落",
                        "updated_at": updated_at,
                    }
                )

    sentiment_rows = []
    for trade_date in target_dates:
        item = sentiment_by_date[trade_date]
        sample_size = int(item["sample_size"] or 0)
        advancing_ratio = _ratio(item["rising_count"], sample_size)
        above_ma20_ratio = _ratio(item["above_ma20_count"], sample_size)
        limit_up_ratio = _ratio(item["limit_up_count"], sample_size)
        failed_base = item["limit_up_count"] + item["failed_limit_count"]
        failed_limit_ratio = _ratio(item["failed_limit_count"], failed_base)
        avg_pct_change = _average(item["pct_changes"])

        score_points = 0
        if advancing_ratio is not None and advancing_ratio >= 0.55:
            score_points += 1
        if advancing_ratio is not None and advancing_ratio >= 0.65:
            score_points += 1
        if above_ma20_ratio is not None and above_ma20_ratio >= 0.55:
            score_points += 1
        if above_ma20_ratio is not None and above_ma20_ratio >= 0.65:
            score_points += 1
        if avg_pct_change is not None and avg_pct_change > 0:
            score_points += 1
        if avg_pct_change is not None and avg_pct_change >= 1.0:
            score_points += 1
        if item["limit_up_count"] >= 40 and item["limit_up_count"] >= item["limit_down_count"] * 3:
            score_points += 1
        if failed_limit_ratio is not None and failed_limit_ratio <= 0.30 and failed_base >= 20:
            score_points += 1

        sentiment_score = min(score_points * 12, 96) if score_points else 0
        label, summary = _sentiment_label_and_summary(
            sentiment_score,
            advancing_ratio,
            above_ma20_ratio,
            avg_pct_change,
            item["limit_up_count"],
            item["limit_down_count"],
            failed_limit_ratio,
        )
        sentiment_rows.append(
            {
                "trade_date": trade_date,
                "sample_size": sample_size,
                "rising_count": item["rising_count"],
                "falling_count": item["falling_count"],
                "flat_count": item["flat_count"],
                "strong_up_count": item["strong_up_count"],
                "strong_down_count": item["strong_down_count"],
                "limit_up_count": item["limit_up_count"],
                "limit_down_count": item["limit_down_count"],
                "failed_limit_count": item["failed_limit_count"],
                "above_ma20_count": item["above_ma20_count"],
                "advancing_ratio": _round_or_none(advancing_ratio, 6),
                "above_ma20_ratio": _round_or_none(above_ma20_ratio, 6),
                "limit_up_ratio": _round_or_none(limit_up_ratio, 6),
                "failed_limit_ratio": _round_or_none(failed_limit_ratio, 6),
                "avg_pct_change": _round_or_none(avg_pct_change, 6),
                "sentiment_score": sentiment_score,
                "sentiment_label": label,
                "summary": summary,
                "updated_at": updated_at,
            }
        )

    sector_rows = []
    for sector_name, items in sector_latest_metrics.items():
        stock_count = len(items)
        if stock_count < MARKET_OVERVIEW_MIN_SECTOR_SIZE:
            continue
        rising_count = sum(1 for item in items if (item.get("pct_change") or 0.0) > 0)
        limit_up_count = sum(1 for item in items if item.get("is_limit_up"))
        avg_pct_change = _average([item.get("pct_change") or 0.0 for item in items])
        avg_return_5d = _average([item["return_5d"] for item in items if item.get("return_5d") is not None])
        above_ma20_ratio = _ratio(
            sum(1 for item in items if item.get("ma20") not in (None, 0) and item.get("close_price") is not None and item["close_price"] > item["ma20"]),
            stock_count,
        )
        advancing_ratio = _ratio(rising_count, stock_count) or 0.0
        strength_score = max(
            0,
            min(
                100,
                int(
                    round(
                        (avg_pct_change or 0.0) * 7
                        + (avg_return_5d or 0.0) * 160
                        + (above_ma20_ratio or 0.0) * 32
                        + advancing_ratio * 24
                        + min(limit_up_count, 6) * 3
                    )
                ),
            ),
        )
        leader = max(
            items,
            key=lambda item: (
                int(item.get("is_limit_up") or 0),
                int(item.get("consecutive_days") or 0),
                item.get("pct_change") or -999.0,
                item.get("return_5d") or -999.0,
            ),
        )
        sector_rows.append(
            {
                "trade_date": latest_trade_date,
                "sector_name": sector_name,
                "stock_count": stock_count,
                "rising_count": rising_count,
                "limit_up_count": limit_up_count,
                "avg_pct_change": _round_or_none(avg_pct_change, 6),
                "avg_return_5d": _round_or_none(avg_return_5d, 6),
                "above_ma20_ratio": _round_or_none(above_ma20_ratio, 6),
                "strength_score": strength_score,
                "leading_stock_code": leader.get("stock_code"),
                "leading_stock_name": leader.get("stock_name"),
                "updated_at": updated_at,
            }
        )

    sector_rows.sort(
        key=lambda row: (
            row["strength_score"],
            row.get("avg_return_5d") or -999.0,
            row.get("avg_pct_change") or -999.0,
            row["stock_count"],
        ),
        reverse=True,
    )

    limit_up_events.sort(
        key=lambda row: (
            row.get("consecutive_days") or 0,
            row.get("pct_change") or -999.0,
            row["stock_code"],
        ),
        reverse=True,
    )
    consecutive_events.sort(
        key=lambda row: (
            row.get("consecutive_days") or 0,
            row.get("pct_change") or -999.0,
            row["stock_code"],
        ),
        reverse=True,
    )
    failed_limit_events.sort(
        key=lambda row: (
            row.get("event_value") if row.get("event_value") is not None else -999.0,
            row.get("pct_change") or -999.0,
            row["stock_code"],
        )
    )

    for index, row in enumerate(limit_up_events, start=1):
        row["rank_no"] = index
    for index, row in enumerate(consecutive_events, start=1):
        row["rank_no"] = index
    for index, row in enumerate(failed_limit_events, start=1):
        row["rank_no"] = index

    return {
        "trade_date": latest_trade_date,
        "sentiment_rows": sentiment_rows,
        "sector_rows": sector_rows,
        "event_rows": limit_up_events + consecutive_events + failed_limit_events,
    }


def sync_scorecard_refresh(manage_log: bool = True):
    """异步刷新全市场短线机会评分卡"""
    try:
        with task_lock("scorecard_refresh") as task_handle:
            log_id = log_sync_start("scorecard_refresh") if manage_log else None
            reporter = TaskProgressReporter("scorecard_refresh", task_handle, log_id)
            reporter.update(
                force=True,
                stage="初始化短线评分刷新",
                processed=0,
                success=0,
                fail=0,
                total=None,
            )
            logger.info("开始刷新短线机会评分卡...")

            try:
                summary = factor_service.refresh_scorecard()
                total = int(summary.get("total") or 0)
                reporter.update(
                    force=True,
                    stage="短线评分刷新完成",
                    total=total,
                    processed=total,
                    success=total,
                    fail=0,
                    current_item=summary.get("trade_date"),
                )
                if manage_log:
                    log_sync_end(log_id, "success", total, total, 0)
                logger.info(f"短线机会评分卡刷新完成: 总数{total}, 观察池{summary.get('watchlist_count')}")
                return summary
            except Exception as e:
                reporter.update(force=True, stage="短线评分刷新失败", error_message=str(e))
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"短线机会评分卡刷新失败: {e}")
                if manage_log:
                    return {"total": 0, "watchlist_count": 0}
                raise
    except TaskAlreadyRunningError as e:
        message = _task_running_message("短线机会评分刷新", e)
        logger.warning(message)
        if manage_log:
            return {"total": 0, "watchlist_count": 0, "skipped": True}
        raise


def sync_market_overview_refresh(manage_log: bool = True):
    """刷新市场结构总览缓存"""
    try:
        with task_lock("market_overview_refresh") as task_handle:
            log_id = log_sync_start("market_overview_refresh") if manage_log else None
            reporter = TaskProgressReporter("market_overview_refresh", task_handle, log_id)
            reporter.update(
                force=True,
                stage="初始化市场结构刷新",
                processed=0,
                success=0,
                fail=0,
                total=4,
            )
            logger.info("开始刷新市场结构总览缓存...")

            try:
                latest_trade_date = db.fetchone(
                    """
                    SELECT MAX(trade_date) AS value
                    FROM daily_kline
                    """
                )
                resolved_trade_date = latest_trade_date.get("value") if latest_trade_date else None
                if not resolved_trade_date:
                    reporter.update(force=True, stage="无日线数据", total=0, processed=0, success=0, fail=0)
                    if manage_log:
                        log_sync_end(log_id, "success", 0, 0, 0)
                    return {"trade_date": None, "sentiment_days": 0, "sector_count": 0, "event_count": 0}

                reporter.update(
                    force=True,
                    stage="计算市场情绪与板块强度",
                    total=4,
                    processed=1,
                    success=1,
                    fail=0,
                    current_item=resolved_trade_date,
                )
                snapshot = _compute_market_overview_snapshot(resolved_trade_date)
                _upsert_market_sentiment_rows(snapshot["sentiment_rows"])
                _replace_sector_strength_rows(resolved_trade_date, snapshot["sector_rows"])
                _replace_stock_event_rows(resolved_trade_date, snapshot["event_rows"])

                reporter.update(
                    force=True,
                    stage="同步市场资金流",
                    total=4,
                    processed=2,
                    success=2,
                    fail=0,
                    current_item=resolved_trade_date,
                )
                fund_flow_warning = None
                try:
                    market_fund_flow_rows = _fetch_market_fund_flow_with_akshare()
                    _upsert_market_fund_flow_rows(market_fund_flow_rows)
                except Exception as exc:
                    fund_flow_warning = f"市场资金流刷新失败: {exc}"
                    logger.warning(fund_flow_warning)

                reporter.update(
                    force=True,
                    stage="同步行业/概念资金流",
                    total=4,
                    processed=3,
                    success=3,
                    fail=0,
                    current_item=resolved_trade_date,
                )
                sector_fund_flow_warning = None
                try:
                    for sector_type in ("行业资金流", "概念资金流"):
                        sector_fund_flow_rows = _fetch_sector_fund_flow_with_akshare(resolved_trade_date, sector_type)
                        if sector_fund_flow_rows:
                            _replace_sector_fund_flow_rows(resolved_trade_date, sector_type, sector_fund_flow_rows)
                except Exception as exc:
                    sector_fund_flow_warning = f"板块资金流刷新失败: {exc}"
                    logger.warning(sector_fund_flow_warning)

                reporter.update(
                    force=True,
                    stage="市场结构刷新完成",
                    total=4,
                    processed=4,
                    success=4,
                    fail=0,
                    current_item=resolved_trade_date,
                    sentiment_days=len(snapshot["sentiment_rows"]),
                    sector_count=len(snapshot["sector_rows"]),
                    event_count=len(snapshot["event_rows"]),
                )
                if manage_log:
                    log_sync_end(log_id, "success", 4, 4, 0)
                logger.info(
                    "市场结构总览缓存刷新完成: "
                    f"trade_date={resolved_trade_date}, "
                    f"sentiment_days={len(snapshot['sentiment_rows'])}, "
                    f"sector_count={len(snapshot['sector_rows'])}, "
                    f"event_count={len(snapshot['event_rows'])}"
                )
                return {
                    "trade_date": resolved_trade_date,
                    "sentiment_days": len(snapshot["sentiment_rows"]),
                    "sector_count": len(snapshot["sector_rows"]),
                    "event_count": len(snapshot["event_rows"]),
                    "fund_flow_warning": fund_flow_warning,
                    "sector_fund_flow_warning": sector_fund_flow_warning,
                }
            except Exception as e:
                reporter.update(force=True, stage="市场结构刷新失败", error_message=str(e))
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"市场结构总览缓存刷新失败: {e}")
                if manage_log:
                    return {"trade_date": None, "sentiment_days": 0, "sector_count": 0, "event_count": 0}
                raise
    except TaskAlreadyRunningError as e:
        message = _task_running_message("市场结构刷新", e)
        logger.warning(message)
        if manage_log:
            return {"trade_date": None, "sentiment_days": 0, "sector_count": 0, "event_count": 0, "skipped": True}
        raise


def sync_trade_calendar(manage_log: bool = True):
    """同步交易日历"""
    try:
        with task_lock("trading_calendar") as task_handle:
            log_id = log_sync_start("trading_calendar") if manage_log else None
            reporter = TaskProgressReporter("trading_calendar", task_handle, log_id)
            reporter.update(
                force=True,
                stage="初始化交易日历同步",
                total=None,
                processed=0,
                success=0,
                fail=0,
            )
            logger.info("开始同步交易日历...")

            try:
                reporter.update(force=True, stage="抓取交易日历")
                with _baostock_session() as bs_client:
                    start_date = rolling_history_start_text()
                    end_date = (datetime.now().date() + timedelta(days=366)).isoformat()
                    calendar_rows = _fetch_trade_calendar_with_baostock(start_date, end_date, bs_client)
                reporter.update(
                    force=True,
                    stage="写入交易日历",
                    total=len(calendar_rows),
                    processed=len(calendar_rows),
                    success=len(calendar_rows),
                    fail=0,
                    current_item=calendar_rows[-1]["trade_date"] if calendar_rows else None,
                )
                _upsert_trading_calendar_rows(calendar_rows)

                reporter.update(
                    force=True,
                    stage="交易日历同步完成",
                    total=len(calendar_rows),
                    processed=len(calendar_rows),
                    success=len(calendar_rows),
                    fail=0,
                )
                if manage_log:
                    log_sync_end(log_id, "success", len(calendar_rows), len(calendar_rows), 0)
                logger.info(f"交易日历同步完成: 总数{len(calendar_rows)}")
                return len(calendar_rows)
            except Exception as e:
                reporter.update(force=True, stage="交易日历同步失败", error_message=str(e))
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"交易日历同步失败: {e}")
                if manage_log:
                    return 0
                raise
    except TaskAlreadyRunningError as e:
        message = _task_running_message("交易日历同步", e)
        logger.warning(message)
        if manage_log:
            return 0
        raise


def sync_stock_profiles(
    limit: Optional[int] = 20,
    offset: int = 0,
    only_missing: bool = True,
    manage_log: bool = True,
):
    """补充股票详情快照"""
    try:
        with task_lock("stock_profiles") as task_handle:
            log_id = log_sync_start("stock_profiles") if manage_log else None
            reporter = TaskProgressReporter("stock_profiles", task_handle, log_id)
            reporter.update(
                force=True,
                stage="初始化详情快照同步",
                processed=0,
                success=0,
                fail=0,
                total=None,
                offset=offset,
                only_missing=only_missing,
            )
            logger.info(f"开始同步股票详情快照: limit={limit or 'all'}, offset={offset}, only_missing={only_missing}")

            try:
                if only_missing:
                    sql = """
                        SELECT stock_code, stock_name
                        FROM stocks
                        WHERE status = 1
                          AND (total_shares IS NULL OR float_shares IS NULL)
                        ORDER BY stock_code
                    """
                else:
                    sql = """
                        SELECT stock_code, stock_name
                        FROM stocks
                        WHERE status = 1
                        ORDER BY stock_code
                    """

                if limit is None:
                    if offset:
                        stocks = db.fetchall(f"{sql}\nLIMIT -1 OFFSET ?", (offset,))
                    else:
                        stocks = db.fetchall(sql)
                else:
                    stocks = db.fetchall(f"{sql}\nLIMIT ? OFFSET ?", (limit, offset))

                total = len(stocks)
                reporter.update(
                    force=True,
                    stage="抓取股票详情快照",
                    total=total,
                    processed=0,
                    success=0,
                    fail=0,
                )
                success = 0
                fail = 0
                profile_records = []
                stock_updates = []
                processed = 0

                for stock in stocks:
                    stock_code = stock["stock_code"]
                    try:
                        reporter.update(
                            stage="抓取股票详情快照",
                            current_item=stock_code,
                            current_name=stock.get("stock_name"),
                            processed=processed,
                            success=success,
                            fail=fail,
                        )
                        profile = _fetch_stock_profile_with_akshare(stock_code)
                        profile_records.append(profile)
                        stock_updates.append(
                            {
                                "stock_code": stock_code,
                                "stock_name": stock["stock_name"],
                                "market_type": None,
                                "exchange": None,
                                "board": None,
                                "sec_type": None,
                                "list_date": profile.get("list_date"),
                                "delist_date": None,
                                "status": None,
                                "is_st_current": None,
                                "total_shares": profile.get("total_shares"),
                                "float_shares": profile.get("float_shares"),
                                "industry_code": None,
                                "source": f"{profile.get('source')}_profile" if profile.get("source") else None,
                            }
                        )
                        success += 1
                        time.sleep(0.2)
                    except Exception as e:
                        logger.warning(f"{stock_code} 股票详情快照同步失败: {e}")
                        fail += 1
                    finally:
                        processed += 1
                        reporter.update(
                            current_item=stock_code,
                            current_name=stock.get("stock_name"),
                            processed=processed,
                            success=success,
                            fail=fail,
                        )

                if stock_updates:
                    reporter.update(force=True, stage="写入股票详情字段", processed=processed, success=success, fail=fail)
                    _update_stock_profile_fields(stock_updates)
                if profile_records:
                    reporter.update(force=True, stage="写入股本与估值快照", processed=processed, success=success, fail=fail)
                    _upsert_share_capital_snapshot(profile_records)
                    today = datetime.now().date().isoformat()
                    _upsert_valuation_snapshot_rows(
                        [
                            {
                                "stock_code": row["stock_code"],
                                "trade_date": today,
                                "market_cap": row.get("market_cap"),
                                "float_market_cap": row.get("float_market_cap"),
                                "pe_ttm": None,
                                "pb_mrq": None,
                                "ps_ttm": None,
                                "pcf_ttm": None,
                                "dividend_yield": None,
                                "source": row.get("source"),
                            }
                            for row in profile_records
                        ]
                    )

                reporter.update(
                    force=True,
                    stage="详情快照同步完成",
                    total=total,
                    processed=processed,
                    success=success,
                    fail=fail,
                )
                if manage_log:
                    log_sync_end(log_id, "success", total, success, fail)
                logger.info(f"股票详情快照同步完成: 总数{total}, 成功{success}, 失败{fail}")
                return {"total": total, "success": success, "fail": fail}
            except Exception as e:
                reporter.update(force=True, stage="详情快照同步失败", error_message=str(e))
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"股票详情快照同步失败: {e}")
                if manage_log:
                    return {"total": 0, "success": 0, "fail": 0}
                raise
    except TaskAlreadyRunningError as e:
        message = _task_running_message("股票详情快照同步", e)
        logger.warning(message)
        if manage_log:
            return {"total": 0, "success": 0, "fail": 0, "skipped": True}
        raise


def sync_stock_list(manage_log: bool = True, trigger_profile_sync: Optional[bool] = None):
    """同步股票列表、主数据、行业和交易日历"""
    if trigger_profile_sync is None:
        trigger_profile_sync = manage_log

    try:
        with task_lock("stock_list") as task_handle:
            log_id = log_sync_start("stock_list") if manage_log else None
            reporter = TaskProgressReporter("stock_list", task_handle, log_id)
            reporter.update(
                force=True,
                stage="初始化股票池同步",
                processed=0,
                success=0,
                fail=0,
                total=None,
            )
            logger.info("开始同步股票列表...")

            try:
                reporter.update(force=True, stage="同步交易日历")
                try:
                    sync_trade_calendar(manage_log=False)
                except Exception as e:
                    logger.warning(f"交易日历同步失败，继续股票池主数据同步: {e}")

                reporter.update(force=True, stage="抓取股票主数据")
                basic_records = {}
                industries = []
                memberships = []
                stock_industry_map = {}
                try:
                    with _baostock_session() as bs_client:
                        stock_list_records = _fetch_stock_list_with_baostock(bs_client)
                        source = "baostock"
                        try:
                            basic_records = _fetch_stock_basics_with_baostock(bs_client)
                        except Exception as e:
                            logger.warning(f"baostock 股票基础资料同步失败，保留股票列表主数据继续写入: {e}")
                        try:
                            industries, memberships, stock_industry_map = _fetch_stock_industries_with_baostock(bs_client)
                        except Exception as e:
                            logger.warning(f"baostock 行业资料同步失败，保留股票列表主数据继续写入: {e}")
                except Exception as e:
                    logger.warning(f"baostock 股票列表同步失败，回退到 akshare: {e}")
                    stock_list_records = _fetch_stock_list_with_akshare()
                    source = "akshare"
                merged_records = {}
                for row in stock_list_records:
                    merged_records[row["stock_code"]] = row.copy()

                for stock_code, row in basic_records.items():
                    merged = merged_records.get(stock_code, {}).copy()
                    merged.update(row)
                    merged_records[stock_code] = merged

                for stock_code, industry_code in stock_industry_map.items():
                    if stock_code in merged_records:
                        merged_records[stock_code]["industry_code"] = industry_code

                stock_records = list(merged_records.values())
                total = len(stock_records)
                reporter.update(
                    force=True,
                    stage="写入股票池与行业数据",
                    total=total,
                    processed=total,
                    success=total,
                    fail=0,
                    source=source,
                )
                _upsert_stock_records(stock_records)
                _upsert_industry_records(industries)
                _upsert_industry_membership_records(memberships)

                pool_total_records = int(db.fetchone("SELECT COUNT(*) AS value FROM stocks")["value"] or 0)
                active_records = int(
                    db.fetchone("SELECT COUNT(*) AS value FROM stocks WHERE status = 1")["value"] or 0
                )
                inactive_records = max(pool_total_records - active_records, 0)

                if manage_log:
                    log_sync_end(log_id, "success", total, total, 0)

                profile_sync_started = False
                if trigger_profile_sync:
                    try:
                        profile_sync_started = _launch_async_stock_profile_sync(limit=min(BATCH_SIZE * 5, 500))
                    except Exception as e:
                        logger.warning(f"异步触发股票详情快照补充失败: {e}")

                reporter.update(
                    force=True,
                    stage="股票池同步完成",
                    total=total,
                    processed=total,
                    success=total,
                    fail=0,
                    source=source,
                    fetched_records=total,
                    pool_total_records=pool_total_records,
                    active_records=active_records,
                    inactive_records=inactive_records,
                    profile_sync_started=profile_sync_started,
                )
                profile_note = "已异步触发详情补充" if profile_sync_started else "未触发详情补充"
                logger.info(
                    "股票列表同步完成: "
                    f"数据源{source}, 本次抓取{total}, 库内总量{pool_total_records}, "
                    f"活跃{active_records}, 非活跃{inactive_records}, 失败0, {profile_note}"
                )
                return {
                    "total": total,
                    "success": total,
                    "fail": 0,
                    "source": source,
                    "fetched_records": total,
                    "pool_total_records": pool_total_records,
                    "active_records": active_records,
                    "inactive_records": inactive_records,
                    "profile_sync_started": profile_sync_started,
                }
            except Exception as e:
                reporter.update(force=True, stage="股票池同步失败", error_message=str(e))
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"股票列表同步失败: {e}")
                if manage_log:
                    return {"total": 0, "success": 0, "fail": 0}
                raise
    except TaskAlreadyRunningError as e:
        message = _task_running_message("股票列表同步", e)
        logger.warning(message)
        if manage_log:
            return {"total": 0, "success": 0, "fail": 0, "skipped": True}
        raise


def sync_adjust_factors(limit: Optional[int] = BATCH_SIZE, offset: int = 0, manage_log: bool = True):
    """同步复权因子"""
    try:
        with task_lock("adjust_factors") as task_handle:
            log_id = log_sync_start("adjust_factors") if manage_log else None
            reporter = TaskProgressReporter("adjust_factors", task_handle, log_id)
            reporter.update(
                force=True,
                stage="初始化复权因子同步",
                processed=0,
                success=0,
                fail=0,
                total=None,
                offset=offset,
                limit=limit,
            )
            logger.info(f"开始同步复权因子: limit={limit or 'all'}, offset={offset}")

            try:
                if limit is None:
                    if offset:
                        stocks = db.fetchall(
                            "SELECT stock_code FROM stocks WHERE status = 1 ORDER BY stock_code LIMIT -1 OFFSET ?",
                            (offset,),
                        )
                    else:
                        stocks = db.fetchall(
                            "SELECT stock_code FROM stocks WHERE status = 1 ORDER BY stock_code"
                        )
                else:
                    stocks = db.fetchall(
                        "SELECT stock_code FROM stocks WHERE status = 1 ORDER BY stock_code LIMIT ? OFFSET ?",
                        (limit, offset),
                    )
                total = len(stocks)
                success = 0
                fail = 0
                processed = 0
                reporter.update(force=True, stage="抓取复权因子", total=total, processed=0, success=0, fail=0)

                with _baostock_session() as bs_client:
                    for stock in stocks:
                        stock_code = stock["stock_code"]
                        try:
                            reporter.update(
                                current_item=stock_code,
                                processed=processed,
                                success=success,
                                fail=fail,
                            )
                            adjust_rows = _fetch_adjust_factors_with_baostock(stock_code, bs_client)
                            _upsert_adjust_factor_rows(adjust_rows)
                            success += 1
                        except Exception as e:
                            logger.error(f"{stock_code} 复权因子同步失败: {e}")
                            fail += 1
                        finally:
                            processed += 1
                            reporter.update(
                                current_item=stock_code,
                                processed=processed,
                                success=success,
                                fail=fail,
                            )

                reporter.update(
                    force=True,
                    stage="复权因子同步完成",
                    total=total,
                    processed=processed,
                    success=success,
                    fail=fail,
                )
                if manage_log:
                    log_sync_end(log_id, "success", total, success, fail)
                logger.info(f"复权因子同步完成: 总数{total}, 成功{success}, 失败{fail}")
                return {"total": total, "success": success, "fail": fail}
            except Exception as e:
                reporter.update(force=True, stage="复权因子同步失败", error_message=str(e))
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"复权因子同步失败: {e}")
                if manage_log:
                    return {"total": 0, "success": 0, "fail": 0}
                raise
    except TaskAlreadyRunningError as e:
        message = _task_running_message("复权因子同步", e)
        logger.warning(message)
        if manage_log:
            return {"total": 0, "success": 0, "fail": 0, "skipped": True}
        raise


def sync_corporate_actions(
    limit: Optional[int] = 20,
    offset: int = 0,
    years_back: int = 3,
    manage_log: bool = True,
):
    """同步公司行为"""
    try:
        with task_lock("corporate_actions") as task_handle:
            log_id = log_sync_start("corporate_actions") if manage_log else None
            reporter = TaskProgressReporter("corporate_actions", task_handle, log_id)
            reporter.update(
                force=True,
                stage="初始化公司行为同步",
                processed=0,
                success=0,
                fail=0,
                total=None,
                offset=offset,
                limit=limit,
                years_back=years_back,
            )
            logger.info(f"开始同步公司行为: limit={limit or 'all'}, offset={offset}, years_back={years_back}")

            try:
                if limit is None:
                    if offset:
                        stocks = db.fetchall(
                            "SELECT stock_code FROM stocks WHERE status = 1 ORDER BY stock_code LIMIT -1 OFFSET ?",
                            (offset,),
                        )
                    else:
                        stocks = db.fetchall(
                            "SELECT stock_code FROM stocks WHERE status = 1 ORDER BY stock_code"
                        )
                else:
                    stocks = db.fetchall(
                        "SELECT stock_code FROM stocks WHERE status = 1 ORDER BY stock_code LIMIT ? OFFSET ?",
                        (limit, offset),
                    )
                total = len(stocks)
                success = 0
                fail = 0
                processed = 0
                reporter.update(force=True, stage="抓取公司行为", total=total, processed=0, success=0, fail=0)

                with _baostock_session() as bs_client:
                    for stock in stocks:
                        stock_code = stock["stock_code"]
                        try:
                            reporter.update(
                                current_item=stock_code,
                                processed=processed,
                                success=success,
                                fail=fail,
                            )
                            action_rows = _fetch_corporate_actions_with_baostock(
                                stock_code,
                                years_back=years_back,
                                bs_client=bs_client,
                            )
                            _upsert_corporate_action_rows(action_rows)
                            success += 1
                        except Exception as e:
                            logger.error(f"{stock_code} 公司行为同步失败: {e}")
                            fail += 1
                        finally:
                            processed += 1
                            reporter.update(
                                current_item=stock_code,
                                processed=processed,
                                success=success,
                                fail=fail,
                            )

                reporter.update(
                    force=True,
                    stage="公司行为同步完成",
                    total=total,
                    processed=processed,
                    success=success,
                    fail=fail,
                )
                if manage_log:
                    log_sync_end(log_id, "success", total, success, fail)
                logger.info(f"公司行为同步完成: 总数{total}, 成功{success}, 失败{fail}")
                return {"total": total, "success": success, "fail": fail}
            except Exception as e:
                reporter.update(force=True, stage="公司行为同步失败", error_message=str(e))
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"公司行为同步失败: {e}")
                if manage_log:
                    return {"total": 0, "success": 0, "fail": 0}
                raise
    except TaskAlreadyRunningError as e:
        message = _task_running_message("公司行为同步", e)
        logger.warning(message)
        if manage_log:
            return {"total": 0, "success": 0, "fail": 0, "skipped": True}
        raise


def sync_benchmark_index_kline(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    manage_log: bool = True,
):
    """同步评分卡和技术分析依赖的基准指数日线"""
    try:
        with task_lock("benchmark_index_kline") as task_handle:
            log_id = log_sync_start("benchmark_index_kline") if manage_log else None
            reporter = TaskProgressReporter("benchmark_index_kline", task_handle, log_id)
            reporter.update(
                force=True,
                stage="初始化基准指数日线同步",
                processed=0,
                success=0,
                fail=0,
                total=len(BENCHMARK_INDEX_CODES),
                start_date=start_date,
                end_date=end_date,
            )
            logger.info(
                f"开始同步基准指数日线: start_date={start_date or 'auto'}, end_date={end_date or 'today'}"
            )

            try:
                _upsert_index_records(_benchmark_index_seed_records())
                total = len(BENCHMARK_INDEX_CODES)
                success = 0
                fail = 0
                processed = 0

                for index_code in BENCHMARK_INDEX_CODES:
                    fetch_start_date, fetch_end_date = _resolve_benchmark_kline_fetch_window(
                        index_code,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    try:
                        reporter.update(
                            stage="抓取并写入基准指数日线",
                            current_item=index_code,
                            processed=processed,
                            success=success,
                            fail=fail,
                            fetch_start_date=fetch_start_date,
                            fetch_end_date=fetch_end_date,
                            error_message=None,
                        )
                        kline_rows = _fetch_index_daily_kline_with_akshare(
                            index_code,
                            start_date=fetch_start_date,
                            end_date=fetch_end_date,
                        )
                        if not kline_rows:
                            raise RuntimeError("未获取到指数日线数据")

                        _upsert_daily_kline_rows(index_code, kline_rows)
                        success += 1
                    except Exception as e:
                        logger.error(f"{index_code} 基准指数日线同步失败: {e}")
                        fail += 1
                        reporter.update(current_item=index_code, error_message=str(e))
                    finally:
                        processed += 1
                        reporter.update(
                            current_item=index_code,
                            processed=processed,
                            success=success,
                            fail=fail,
                        )

                reporter.update(
                    force=True,
                    stage="基准指数日线同步完成",
                    total=total,
                    processed=processed,
                    success=success,
                    fail=fail,
                )
                if manage_log:
                    log_sync_end(log_id, "success", total, success, fail)
                logger.info(f"基准指数日线同步完成: 总数{total}, 成功{success}, 失败{fail}")
                return {"total": total, "success": success, "fail": fail}
            except Exception as e:
                reporter.update(force=True, stage="基准指数日线同步失败", error_message=str(e))
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"基准指数日线同步失败: {e}")
                if manage_log:
                    return {"total": 0, "success": 0, "fail": 0}
                raise
    except TaskAlreadyRunningError as e:
        message = _task_running_message("基准指数日线同步", e)
        logger.warning(message)
        if manage_log:
            return {"total": 0, "success": 0, "fail": 0, "skipped": True}
        raise


def sync_daily_kline(
    limit: Optional[int] = None,
    offset: int = 0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    manage_log: bool = True,
):
    """同步日线数据、交易状态和估值快照"""
    try:
        with task_lock("daily_kline") as task_handle:
            log_id = log_sync_start("daily_kline") if manage_log else None
            reporter = TaskProgressReporter("daily_kline", task_handle, log_id)
            watchdog = TaskProgressWatchdog("daily_kline", reporter).start()
            reporter.update(
                force=True,
                stage="初始化日线同步",
                processed=0,
                success=0,
                fail=0,
                total=None,
                offset=offset,
                limit=limit,
                start_date=start_date,
                end_date=end_date,
            )
            selection_mode = "gaps_first" if offset == 0 and start_date is None and end_date is None else "sequential"
            logger.info(
                f"开始同步日线数据: strategy={selection_mode}, limit={limit or 'all'}, offset={offset}, "
                f"start_date={start_date or 'default'}, end_date={end_date or 'today'}"
            )

            bs_client = None
            baostock_lock_acquired = False
            try:
                gap_candidates = None
                if selection_mode == "gaps_first":
                    gap_candidates = _list_daily_kline_gap_candidates(limit=limit)
                    total = len(gap_candidates)
                else:
                    total_result = db.fetchone("SELECT COUNT(*) as cnt FROM stocks WHERE status = 1")
                    total = total_result["cnt"] if total_result else 0
                    if offset:
                        total = max(total - offset, 0)
                    if limit is not None:
                        total = min(total, limit)
                success = 0
                fail = 0
                skipped = 0
                processed = 0
                watchdog.mark_progress(
                    context="准备日线数据源",
                    force=True,
                    stage="准备数据源",
                    total=total,
                    processed=0,
                    success=0,
                    fail=0,
                    skipped=0,
                    selection_mode=selection_mode,
                )

                try:
                    BAOSTOCK_SESSION_LOCK.acquire()
                    baostock_lock_acquired = True
                    bs_client = _baostock_login()
                    preferred_source = "baostock"
                except Exception as e:
                    if baostock_lock_acquired:
                        BAOSTOCK_SESSION_LOCK.release()
                        baostock_lock_acquired = False
                    logger.warning(f"baostock 日线同步不可用，回退到 akshare: {e}")
                    preferred_source = "akshare"

                watchdog.mark_progress(
                    context=f"日线数据源就绪:{preferred_source}",
                    force=True,
                    stage="准备数据源",
                    total=total,
                    processed=processed,
                    success=success,
                    fail=fail,
                    skipped=skipped,
                    source=preferred_source,
                    selection_mode=selection_mode,
                )
                current_offset = offset
                while True:
                    watchdog.raise_if_tripped()
                    remaining = total - processed
                    if remaining <= 0:
                        break

                    batch_limit = min(BATCH_SIZE, remaining)
                    if selection_mode == "gaps_first":
                        stocks = (gap_candidates or [])[processed : processed + batch_limit]
                        batch_offset = processed
                    else:
                        stocks = db.fetchall(
                            """
                            SELECT stock_code, stock_name, board
                            FROM stocks
                            WHERE status = 1
                            ORDER BY stock_code
                            LIMIT ? OFFSET ?
                            """,
                            (batch_limit, current_offset),
                        )
                        batch_offset = current_offset
                    if not stocks:
                        break

                    watchdog.set_context(f"处理日线批次 mode={selection_mode} offset={batch_offset}")
                    logger.info(
                        f"开始处理日线批次: mode={selection_mode}, offset={batch_offset}, batch_size={len(stocks)}"
                    )
                    for stock in stocks:
                        watchdog.raise_if_tripped()
                        stock_code = stock["stock_code"]
                        stock_name = stock["stock_name"]
                        board = stock["board"] or _infer_market_meta(stock_code)["board"]
                        fetch_start_date, fetch_end_date = _resolve_daily_kline_fetch_window(
                            stock,
                            start_date=start_date,
                            end_date=end_date,
                        )
                        try:
                            watchdog.set_context(f"{stock_code} 准备抓取日线")
                            reporter.update(
                                stage="抓取并写入日线",
                                current_item=stock_code,
                                current_name=stock_name,
                                current_batch_offset=batch_offset,
                                current_batch_size=len(stocks),
                                processed=processed,
                                success=success,
                                fail=fail,
                                skipped=skipped,
                                selection_mode=selection_mode,
                                gap_reason=stock.get("selection_reason"),
                                missing_days=stock.get("missing_days"),
                                history_missing_days=stock.get("history_missing_days"),
                                recent_missing_days=stock.get("recent_missing_days"),
                                fetch_start_date=fetch_start_date,
                                fetch_end_date=fetch_end_date,
                                error_message=None,
                            )
                            if preferred_source == "baostock":
                                try:
                                    watchdog.set_context(f"{stock_code} baostock 日线抓取")
                                    kline_rows = _fetch_daily_kline_with_baostock(
                                        stock_code,
                                        start_date=fetch_start_date,
                                        end_date=fetch_end_date,
                                        bs_client=bs_client,
                                    )
                                    source = "baostock"
                                except Exception as e:
                                    if isinstance(e, TaskStalledError):
                                        raise
                                    logger.warning(f"{stock_code} baostock 日线同步失败，回退到 akshare: {e}")
                                    watchdog.set_context(f"{stock_code} akshare 日线回退")
                                    kline_rows = _fetch_daily_kline_with_akshare(
                                        stock_code,
                                        start_date=fetch_start_date,
                                        end_date=fetch_end_date,
                                    )
                                    source = "akshare"
                            else:
                                try:
                                    kline_rows = _fetch_daily_kline_with_akshare(
                                        stock_code,
                                        start_date=fetch_start_date,
                                        end_date=fetch_end_date,
                                    )
                                    source = "akshare"
                                except Exception as e:
                                    if isinstance(e, TaskStalledError):
                                        raise
                                    logger.warning(f"{stock_code} akshare 日线同步失败，回退到 baostock: {e}")
                                    if bs_client is None:
                                        if not baostock_lock_acquired:
                                            BAOSTOCK_SESSION_LOCK.acquire()
                                            baostock_lock_acquired = True
                                        try:
                                            bs_client = _baostock_login()
                                        except Exception:
                                            if baostock_lock_acquired:
                                                BAOSTOCK_SESSION_LOCK.release()
                                                baostock_lock_acquired = False
                                            raise
                                    watchdog.set_context(f"{stock_code} baostock 日线回退")
                                    kline_rows = _fetch_daily_kline_with_baostock(
                                        stock_code,
                                        start_date=fetch_start_date,
                                        end_date=fetch_end_date,
                                        bs_client=bs_client,
                                    )
                                    source = "baostock"

                            if not kline_rows:
                                skipped += 1
                                processed += 1
                                watchdog.mark_progress(
                                    context=f"{stock_code} 无日线数据",
                                    current_item=stock_code,
                                    current_name=stock_name,
                                    processed=processed,
                                    success=success,
                                    fail=fail,
                                    skipped=skipped,
                                    source=source,
                                    selection_mode=selection_mode,
                                    rows_written=0,
                                    error_message=None,
                                )
                                continue

                            watchdog.set_context(f"{stock_code} 写入日线与衍生指标")
                            _upsert_daily_kline_rows(stock_code, kline_rows)

                            trade_flag_rows = []
                            valuation_rows = []
                            latest_is_st = 0
                            for row in kline_rows:
                                is_st = row.get("is_st") or 0
                                latest_is_st = is_st
                                limit_up_price, limit_down_price = _compute_limit_prices(
                                    row.get("pre_close"),
                                    board,
                                    is_st,
                                    row["trade_date"],
                                )
                                close_price = _safe_float(row.get("close_price"))
                                is_limit_up = 0
                                is_limit_down = 0
                                if close_price is not None and limit_up_price is not None:
                                    is_limit_up = int(abs(close_price - limit_up_price) < 0.011)
                                    is_limit_down = int(abs(close_price - limit_down_price) < 0.011)

                                trade_flag_rows.append(
                                    {
                                        "stock_code": stock_code,
                                        "trade_date": row["trade_date"],
                                        "is_suspended": int((row.get("tradestatus") or 1) != 1),
                                        "is_st": is_st,
                                        "is_limit_up": is_limit_up,
                                        "is_limit_down": is_limit_down,
                                        "limit_up_price": limit_up_price,
                                        "limit_down_price": limit_down_price,
                                        "board": board,
                                    }
                                )
                                valuation_rows.append(
                                    {
                                        "stock_code": stock_code,
                                        "trade_date": row["trade_date"],
                                        "market_cap": None,
                                        "float_market_cap": None,
                                        "pe_ttm": row.get("pe_ratio"),
                                        "pb_mrq": row.get("pb_ratio"),
                                        "ps_ttm": row.get("ps_ttm"),
                                        "pcf_ttm": row.get("pcf_ttm"),
                                        "dividend_yield": None,
                                        "source": source,
                                    }
                                )

                            _upsert_daily_trade_flag_rows(trade_flag_rows)
                            _upsert_valuation_snapshot_rows(valuation_rows)
                            _update_stock_runtime_flags(
                                [
                                    {
                                        "stock_code": stock_code,
                                        "board": board,
                                        "is_st_current": latest_is_st,
                                    }
                                ]
                            )

                            success += 1
                            processed += 1
                            watchdog.mark_progress(
                                context=f"{stock_code} 日线写入完成",
                                current_item=stock_code,
                                current_name=stock_name,
                                processed=processed,
                                success=success,
                                fail=fail,
                                skipped=skipped,
                                source=source,
                                selection_mode=selection_mode,
                                rows_written=len(kline_rows),
                                error_message=None,
                            )
                            logger.info(f"{stock_code} 日线同步完成，数据源{source}，记录数{len(kline_rows)}")
                            time.sleep(0.05)
                        except Exception as e:
                            if isinstance(e, TaskStalledError):
                                raise
                            logger.error(f"同步 {stock_code} 日线数据失败: {e}")
                            fail += 1
                            processed += 1
                            watchdog.mark_progress(
                                context=f"{stock_code} 日线失败",
                                current_item=stock_code,
                                current_name=stock_name,
                                processed=processed,
                                success=success,
                                fail=fail,
                                skipped=skipped,
                                selection_mode=selection_mode,
                                error_message=str(e),
                            )

                    if selection_mode == "sequential":
                        current_offset += len(stocks)

                if bs_client is not None:
                    _baostock_logout(bs_client)
                if baostock_lock_acquired:
                    BAOSTOCK_SESSION_LOCK.release()

                try:
                    watchdog.mark_progress(
                        context="刷新基准指数日线",
                        force=True,
                        stage="刷新基准指数日线",
                        total=total,
                        processed=processed,
                        success=success,
                        fail=fail,
                        skipped=skipped,
                        selection_mode=selection_mode,
                    )
                    sync_benchmark_index_kline(
                        start_date=start_date,
                        end_date=end_date,
                        manage_log=False,
                    )
                except Exception as benchmark_error:
                    logger.warning(f"基准指数日线刷新失败，继续刷新评分卡: {benchmark_error}")

                scorecard_summary = None
                try:
                    watchdog.mark_progress(
                        context="刷新短线机会评分",
                        force=True,
                        stage="刷新短线机会评分",
                        total=total,
                        processed=processed,
                        success=success,
                        fail=fail,
                        skipped=skipped,
                        selection_mode=selection_mode,
                    )
                    scorecard_summary = factor_service.refresh_scorecard()
                except Exception as scorecard_error:
                    logger.warning(f"短线机会评分刷新失败，保留日线同步结果: {scorecard_error}")

                market_overview_summary = None
                try:
                    watchdog.mark_progress(
                        context="刷新市场结构缓存",
                        force=True,
                        stage="刷新市场结构缓存",
                        total=total,
                        processed=processed,
                        success=success,
                        fail=fail,
                        skipped=skipped,
                        selection_mode=selection_mode,
                    )
                    market_overview_summary = sync_market_overview_refresh(manage_log=False)
                except Exception as market_overview_error:
                    logger.warning(f"市场结构缓存刷新失败，保留日线同步结果: {market_overview_error}")

                watchdog.mark_progress(
                    context="日线同步完成",
                    force=True,
                    stage="日线同步完成",
                    total=total,
                    processed=processed,
                    success=success,
                    fail=fail,
                    skipped=skipped,
                    selection_mode=selection_mode,
                    factor_scorecard_updated_at=(scorecard_summary or {}).get("updated_at"),
                    factor_watchlist_count=(scorecard_summary or {}).get("watchlist_count"),
                    market_overview_trade_date=(market_overview_summary or {}).get("trade_date"),
                    market_overview_sector_count=(market_overview_summary or {}).get("sector_count"),
                )
                if manage_log:
                    log_sync_end(log_id, "success", total, success, fail)
                logger.info(f"日线数据同步完成: mode={selection_mode}, 总数{total}, 成功{success}, 失败{fail}")
                return {"total": total, "success": success, "fail": fail, "selection_mode": selection_mode}
            except Exception as e:
                if bs_client is not None:
                    _baostock_logout(bs_client)
                if baostock_lock_acquired:
                    BAOSTOCK_SESSION_LOCK.release()
                reporter.update(
                    force=True,
                    stage="日线同步失败",
                    error_message=str(e),
                    watchdog_tripped=isinstance(e, TaskStalledError),
                )
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"日线数据同步失败: {e}")
                if manage_log:
                    return {"total": 0, "success": 0, "fail": 0}
                raise
            finally:
                watchdog.stop()
    except TaskAlreadyRunningError as e:
        message = _task_running_message("日线同步", e)
        logger.warning(message)
        if manage_log:
            return {"total": 0, "success": 0, "fail": 0, "skipped": True}
        raise


def sync_index_list(manage_log: bool = True):
    """同步独立指数池"""
    try:
        with task_lock("index_list") as task_handle:
            log_id = log_sync_start("index_list") if manage_log else None
            reporter = TaskProgressReporter("index_list", task_handle, log_id)
            reporter.update(force=True, stage="初始化指数池同步", processed=0, success=0, fail=0, total=None)
            logger.info("开始同步指数池...")

            try:
                reporter.update(force=True, stage="抓取指数池")
                index_records = _fetch_index_list_with_akshare()
                reporter.update(
                    force=True,
                    stage="写入指数池",
                    total=len(index_records),
                    processed=len(index_records),
                    success=len(index_records),
                    fail=0,
                )
                _upsert_index_records(index_records)
                _cleanup_index_code_formats()
                total = len(index_records)
                reporter.update(
                    force=True,
                    stage="指数池同步完成",
                    total=total,
                    processed=total,
                    success=total,
                    fail=0,
                )
                if manage_log:
                    log_sync_end(log_id, "success", total, total, 0)
                logger.info(f"指数池同步完成: 总数{total}, 成功{total}, 失败0")
                return {"total": total, "success": total, "fail": 0}
            except Exception as e:
                reporter.update(force=True, stage="指数池同步失败", error_message=str(e))
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"指数池同步失败: {e}")
                if manage_log:
                    return {"total": 0, "success": 0, "fail": 0}
                raise
    except TaskAlreadyRunningError as e:
        message = _task_running_message("指数池同步", e)
        logger.warning(message)
        if manage_log:
            return {"total": 0, "success": 0, "fail": 0, "skipped": True}
        raise


def sync_financial_data(limit: Optional[int] = None, offset: int = 0, only_missing: bool = False, manage_log: bool = True):
    """同步财务数据"""
    try:
        with task_lock("financial") as task_handle:
            log_id = log_sync_start("financial") if manage_log else None
            reporter = TaskProgressReporter("financial", task_handle, log_id)
            watchdog = TaskProgressWatchdog("financial", reporter).start()
            reporter.update(
                force=True,
                stage="初始化财务同步",
                processed=0,
                success=0,
                fail=0,
                total=None,
                offset=offset,
                limit=limit,
                only_missing=only_missing,
                written_rows=0,
            )
            logger.info(f"开始同步财务数据: limit={limit or 'all'}, offset={offset}, only_missing={only_missing}")

            try:
                if only_missing:
                    stocks = db.fetchall(
                        """
                        SELECT s.stock_code
                        FROM stocks s
                        WHERE s.status = 1
                          AND NOT EXISTS (
                              SELECT 1
                              FROM financial_reports f
                              WHERE f.stock_code = s.stock_code
                          )
                        ORDER BY s.stock_code
                        """
                    )
                else:
                    stocks = db.fetchall(
                        """
                        SELECT stock_code
                        FROM stocks
                        WHERE status = 1
                        ORDER BY stock_code
                        """
                    )

                stock_codes = [row["stock_code"] for row in stocks]
                if offset:
                    stock_codes = stock_codes[offset:]
                if limit is not None:
                    stock_codes = stock_codes[:limit]

                total = len(stock_codes)
                success = 0
                fail = 0
                written_rows = 0
                processed = 0
                watchdog.mark_progress(
                    context="抓取并写入财务数据",
                    force=True,
                    stage="抓取并写入财务数据",
                    total=total,
                    processed=0,
                    success=0,
                    fail=0,
                    written_rows=0,
                )

                for stock_code in stock_codes:
                    watchdog.raise_if_tripped()
                    try:
                        watchdog.set_context(f"{stock_code} 抓取财务数据")
                        reporter.update(
                            current_item=stock_code,
                            processed=processed,
                            success=success,
                            fail=fail,
                            written_rows=written_rows,
                            error_message=None,
                        )
                        report_rows = _build_financial_report_rows(stock_code)
                        if report_rows:
                            _upsert_financial_report_rows(report_rows)
                            written_rows += len(report_rows)
                        success += 1
                        watchdog.mark_progress(
                            context=f"{stock_code} 财务写入完成",
                            current_item=stock_code,
                            processed=processed + 1,
                            success=success,
                            fail=fail,
                            written_rows=written_rows,
                            rows_written=len(report_rows),
                            error_message=None,
                        )
                        logger.info(f"{stock_code} 财务同步完成，写入{len(report_rows)}条")
                        time.sleep(0.15)
                    except Exception as e:
                        if isinstance(e, TaskStalledError):
                            raise
                        logger.error(f"{stock_code} 财务同步失败: {e}")
                        fail += 1
                        watchdog.mark_progress(
                            context=f"{stock_code} 财务失败",
                            current_item=stock_code,
                            processed=processed + 1,
                            success=success,
                            fail=fail,
                            written_rows=written_rows,
                            rows_written=0,
                            error_message=str(e),
                        )
                    finally:
                        processed += 1

                watchdog.mark_progress(
                    context="财务同步完成",
                    force=True,
                    stage="财务同步完成",
                    total=total,
                    processed=processed,
                    success=success,
                    fail=fail,
                    written_rows=written_rows,
                )
                if manage_log:
                    log_sync_end(log_id, "success", total, success, fail)
                logger.info(f"财务数据同步完成: 总数{total}, 成功{success}, 失败{fail}, 写入{written_rows}条")
                return {"total": total, "success": success, "fail": fail, "written_rows": written_rows}
            except Exception as e:
                reporter.update(
                    force=True,
                    stage="财务同步失败",
                    error_message=str(e),
                    watchdog_tripped=isinstance(e, TaskStalledError),
                )
                if manage_log:
                    log_sync_end(log_id, "failed", error=str(e))
                logger.error(f"财务数据同步失败: {e}")
                if manage_log:
                    return {"total": 0, "success": 0, "fail": 0}
                raise
            finally:
                watchdog.stop()
    except TaskAlreadyRunningError as e:
        message = _task_running_message("财务同步", e)
        logger.warning(message)
        if manage_log:
            return {"total": 0, "success": 0, "fail": 0, "skipped": True}
        raise
