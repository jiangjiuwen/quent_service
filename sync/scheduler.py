from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from config.settings import SYNC_HOUR, SYNC_MINUTE
from sync.task_dispatcher import trigger_scheduled_daily_sync
from utils.logger import logger
import pytz

class SyncScheduler:
    """同步任务调度器"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Shanghai'))
        self.is_running = False
    
    def start(self):
        """启动定时任务"""
        if self.is_running:
            logger.warning("调度器已在运行")
            return

        # 仅保留交易日收盘后的最新日线增量同步。
        self.scheduler.add_job(
            trigger_scheduled_daily_sync,
            trigger=CronTrigger(day_of_week="mon-fri", hour=SYNC_HOUR, minute=SYNC_MINUTE),
            id="sync_daily",
            name="同步最新日线数据",
            replace_existing=True,
        )
        logger.info(f"已添加日线同步任务: 每日{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}")

        self.scheduler.start()
        self.is_running = True
        logger.info("定时同步服务已启动")
    
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
