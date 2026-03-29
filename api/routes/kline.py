from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import date
from api.models.schemas import KlineResponse, KlineData, LatestPrice, APIResponse
from services.query_service import QueryService

router = APIRouter()
query_service = QueryService()

@router.get("/daily/{stock_code}", response_model=KlineResponse)
async def get_daily_kline(
    stock_code: str,
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    limit: int = Query(100, ge=1, le=5000, description="返回条数")
):
    """获取日K线数据
    
    - **stock_code**: 股票代码，如 600519
    - **start_date**: 开始日期，格式 YYYY-MM-DD
    - **end_date**: 结束日期，格式 YYYY-MM-DD
    - **limit**: 返回条数，默认100，最大5000
    """
    try:
        data = query_service.get_daily_kline(stock_code, start_date, end_date, limit)
        return KlineResponse(
            stock_code=stock_code,
            data=[KlineData(**row) for row in data]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/latest/{stock_code}", response_model=LatestPrice)
async def get_latest_price(stock_code: str):
    """获取最新价格（包含涨跌幅）"""
    try:
        result = query_service.get_latest_price(stock_code)
        if not result:
            raise HTTPException(status_code=404, detail=f"股票 {stock_code} 无数据")
        return LatestPrice(**result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history/{stock_code}")
async def get_kline_history(
    stock_code: str,
    days: int = Query(30, ge=1, le=365, description="最近N天")
):
    """获取最近N天的K线数据"""
    try:
        from datetime import timedelta
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        data = query_service.get_daily_kline(stock_code, start_date, end_date, days)
        return KlineResponse(
            stock_code=stock_code,
            data=[KlineData(**row) for row in data]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
