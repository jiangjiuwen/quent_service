from loguru import logger
import sys
from config.settings import LOG_LEVEL, LOG_FORMAT, LOG_DIR

# 配置日志
logger.remove()  # 移除默认配置
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 控制台输出
logger.add(
    sys.stdout,
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    colorize=True
)

# 文件输出 - 同步日志
logger.add(
    str(LOG_DIR / "sync.log"),
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    rotation="10 MB",  # 每10MB轮转
    retention="30 days",  # 保留30天
    compression="zip",
    filter=lambda record: record["name"].startswith("sync")
)

# 文件输出 - API日志
logger.add(
    str(LOG_DIR / "api.log"),
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    rotation="10 MB",
    retention="30 days",
    compression="zip",
    filter=lambda record: record["name"].startswith("api")
)

# 文件输出 - 错误日志
logger.add(
    str(LOG_DIR / "error.log"),
    level="ERROR",
    format=LOG_FORMAT,
    rotation="10 MB",
    retention="60 days",
    compression="zip"
)

__all__ = ["logger"]
