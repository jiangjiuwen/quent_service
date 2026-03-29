#!/usr/bin/env python3
"""
A股量化数据服务主入口
启动Web API服务和定时同步任务
"""

import uvicorn
from api.main import app
from sync.scheduler import scheduler
from config.settings import API_HOST, API_PORT

def main():
    """主函数"""
    # 启动定时同步服务
    scheduler.start()
    
    # 启动Web API服务
    uvicorn.run(
        "api.main:app",
        host=API_HOST,
        port=API_PORT,
        reload=False,
        log_level="info",
        access_log=True
    )

if __name__ == "__main__":
    main()
