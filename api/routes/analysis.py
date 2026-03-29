from fastapi import APIRouter, HTTPException, Query

from api.models.schemas import APIResponse
from services.factor_service import factor_service
from services.technical_analysis_service import technical_analysis_service


router = APIRouter()


@router.get("/scorecard")
async def get_watchlist_scorecard(
    limit: int = Query(12, ge=1, le=200, description="返回短线机会池数量"),
    min_score: int = Query(6, ge=0, le=10, description="进入短线机会池的最低分"),
):
    """获取短线机会观察池"""
    try:
        return APIResponse(data=factor_service.get_watchlist(limit=limit, min_score=min_score))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scorecard/{stock_code}")
async def get_stock_scorecard(stock_code: str):
    """获取单只股票短线机会评分卡"""
    try:
        data = factor_service.get_stock_score(stock_code)
        if not data:
            raise HTTPException(status_code=404, detail=f"股票 {stock_code} 暂无短线机会评分数据")
        return APIResponse(data=data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/technical/{stock_code}")
async def get_stock_technical_analysis(stock_code: str):
    """获取单只股票技术面深度分析"""
    try:
        data = technical_analysis_service.get_stock_analysis(stock_code)
        if not data:
            raise HTTPException(status_code=404, detail=f"股票 {stock_code} 暂无技术分析数据")
        return APIResponse(data=data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
