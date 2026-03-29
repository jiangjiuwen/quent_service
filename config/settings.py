import os
from datetime import date
from pathlib import Path
from typing import Optional


def _path_setting(env_name: str, default: Path) -> Path:
    raw_value = os.getenv(env_name)
    if raw_value:
        path = Path(raw_value).expanduser()
        return path if path.is_absolute() else (Path.cwd() / path)
    return default.expanduser().resolve()


BASE_DIR = _path_setting("QUANT_BASE_DIR", Path(__file__).resolve().parent.parent)
DATA_DIR = _path_setting("QUANT_DATA_DIR", BASE_DIR / "data")
LOG_DIR = _path_setting("QUANT_LOG_DIR", BASE_DIR / "logs")
WEB_DIR = _path_setting("QUANT_WEB_DIR", BASE_DIR / "web")
WEB_ASSETS_DIR = _path_setting("QUANT_WEB_ASSETS_DIR", WEB_DIR / "assets")

# 数据库配置
DB_PATH = os.getenv("QUANT_DB_PATH", str(DATA_DIR / "a_stock_quant.db"))

# API配置
API_HOST = os.getenv("QUANT_API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("QUANT_API_PORT", "8000"))
API_WORKERS = int(os.getenv("QUANT_API_WORKERS", "1"))

# 同步配置
SYNC_HOUR = int(os.getenv("QUANT_SYNC_HOUR", "17"))  # 每日同步时间（收盘后）
SYNC_MINUTE = int(os.getenv("QUANT_SYNC_MINUTE", "0"))
BATCH_SIZE = int(os.getenv("QUANT_BATCH_SIZE", "100"))  # 每批处理股票数

# 数据源配置
DATA_SOURCES = {
    "akshare": {
        "priority": 1,
        "enabled": os.getenv("QUANT_SOURCE_AKSHARE_ENABLED", "true").lower() == "true",
    },
    "baostock": {
        "priority": 2,
        "enabled": os.getenv("QUANT_SOURCE_BAOSTOCK_ENABLED", "true").lower() == "true",
    },
    "tushare": {
        "priority": 3,
        "enabled": os.getenv("QUANT_SOURCE_TUSHARE_ENABLED", "false").lower() == "true",
    },  # 需要token
}

# 日志配置
LOG_LEVEL = os.getenv("QUANT_LOG_LEVEL", "INFO")
LOG_FORMAT = os.getenv(
    "QUANT_LOG_FORMAT",
    "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}",
)

# 数据范围
HISTORY_YEARS = int(os.getenv("QUANT_HISTORY_YEARS", "15"))
START_YEAR = int(os.getenv("QUANT_START_YEAR", "2010"))
END_YEAR = int(os.getenv("QUANT_END_YEAR", "2025"))


def rolling_history_start_date(reference: Optional[date] = None, years_back: Optional[int] = None) -> date:
    reference = reference or date.today()
    years_back = HISTORY_YEARS if years_back is None else years_back
    try:
        return reference.replace(year=reference.year - years_back)
    except ValueError:
        # 处理闰年 2 月 29 日回退时的非法日期。
        return reference.replace(year=reference.year - years_back, month=2, day=28)


def rolling_history_end_date(reference: Optional[date] = None) -> date:
    return reference or date.today()


def rolling_history_start_text(reference: Optional[date] = None, years_back: Optional[int] = None) -> str:
    return rolling_history_start_date(reference=reference, years_back=years_back).isoformat()


def rolling_history_end_text(reference: Optional[date] = None) -> str:
    return rolling_history_end_date(reference=reference).isoformat()
