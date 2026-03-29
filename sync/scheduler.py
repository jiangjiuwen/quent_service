from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from config.settings import SYNC_HOUR, SYNC_MINUTE
from sync.task_dispatcher import (
    trigger_scheduled_adjust_factor_sync,
    trigger_scheduled_corporate_action_sync,
    trigger_scheduled_daily_sync,
    trigger_scheduled_financial_sync,
    trigger_scheduled_index_list_sync,
    trigger_scheduled_stock_list_sync,
    trigger_scheduled_stock_profile_sync,
    trigger_startup_daily_catchup_if_needed,
)
from utils.logger import logger
import pytz


class SyncScheduler:
    """同步任务调度器"""

    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Shanghai"))
        self.is_running = False

    def start(self):
        """启动定时任务"""
        if self.is_running:
            logger.warning("调度器已在运行")
            return

        self.scheduler.add_job(
            trigger_scheduled_daily_sync,
            trigger=CronTrigger(day_of_week="mon-fri", hour=SYNC_HOUR, minute=SYNC_MINUTE),
            id="sync_daily",
            name="同步最新日线数据",
            replace_existing=True,
        )
        self.scheduler.add_job(
            trigger_scheduled_stock_list_sync,
            trigger=CronTrigger(day_of_week="sat", hour=8, minute=30),
            id="sync_stock_list_weekly",
            name="同步股票池主数据",
            replace_existing=True,
        )
        self.scheduler.add_job(
            trigger_scheduled_index_list_sync,
            trigger=CronTrigger(day_of_week="sat", hour=8, minute=40),
            id="sync_index_list_weekly",
            name="同步指数池主数据",
            replace_existing=True,
        )
        self.scheduler.add_job(
            trigger_scheduled_stock_profile_sync,
            trigger=CronTrigger(day_of_week="sat", hour=9, minute=0),
            id="sync_stock_profiles_weekly",
            name="补齐股票详情快照",
            replace_existing=True,
        )
        self.scheduler.add_job(
            trigger_scheduled_corporate_action_sync,
            trigger=CronTrigger(day_of_week="sat", hour=22, minute=30),
            id="sync_corporate_actions_weekly",
            name="同步公司行为",
            replace_existing=True,
        )
        self.scheduler.add_job(
            trigger_scheduled_adjust_factor_sync,
            trigger=CronTrigger(day_of_week="sun", hour=1, minute=30),
            id="sync_adjust_factors_weekly",
            name="同步复权因子",
            replace_existing=True,
        )
        self.scheduler.add_job(
            trigger_scheduled_financial_sync,
            trigger=CronTrigger(day_of_week="sun", hour=4, minute=30),
            id="sync_financial_weekly",
            name="同步财务数据",
            replace_existing=True,
        )
        logger.info(f"已添加日线同步任务: 每个交易日 {SYNC_HOUR:02d}:{SYNC_MINUTE:02d}")
        logger.info("已添加周末维护任务: 股票池/指数池/详情快照/公司行为/复权因子/财务")

        self.scheduler.start()
        self.is_running = True
        logger.info("定时同步服务已启动")

        trigger_startup_daily_catchup_if_needed()

    def shutdown(self):
        """关闭定时任务"""
        if not self.is_running:
            return

        self.scheduler.shutdown()
        self.is_running = False
        logger.info("定时同步服务已停止")

    def get_jobs(self):
        """获取所有任务"""
        return self.scheduler.get_jobs()


# 全局调度器实例
scheduler = SyncScheduler()
