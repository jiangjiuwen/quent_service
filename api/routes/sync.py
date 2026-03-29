from fastapi import APIRouter
from api.models.schemas import APIResponse
from services.query_service import QueryService
from sync.task_dispatcher import spawn_latest_daily_sync
from sync.task_locks import get_task_lock_status

router = APIRouter()
query_service = QueryService()


def task_conflict_response(task_name: str, label: str):
    status = get_task_lock_status(task_name)
    if not status["is_running"]:
        return None

    detail_parts = []
    metadata = status.get("metadata", {})
    if metadata.get("started_at"):
        detail_parts.append(f"started_at={metadata['started_at']}")
    if metadata.get("pid"):
        detail_parts.append(f"pid={metadata['pid']}")
    detail_text = f" ({', '.join(detail_parts)})" if detail_parts else ""
    return APIResponse(code=409, message=f"{label}已在运行，暂不重复触发{detail_text}")

@router.get("/status")
async def get_sync_status():
    """获取同步任务状态"""
    try:
        status = query_service.get_sync_status()
        return APIResponse(data=status)
    except Exception as e:
        return APIResponse(code=500, message=str(e))

@router.post("/daily")
async def trigger_daily_sync():
    """手动触发最近交易日的日线增量同步"""
    try:
        conflict = task_conflict_response("daily_kline", "日线同步")
        if conflict:
            return conflict

        result = spawn_latest_daily_sync()
        if not result.get("spawned"):
            if result.get("reason") == "no_open_trade_day":
                return APIResponse(code=404, message="未找到可同步的最近交易日")
            return APIResponse(code=500, message="日线同步任务未能启动")

        return APIResponse(
            message=f"最新日线同步任务已启动 ({result['start_date']} -> {result['end_date']})",
            data=result,
        )
    except Exception as e:
        return APIResponse(code=500, message=str(e))
