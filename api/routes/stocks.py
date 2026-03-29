from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from api.models.schemas import (
    StockInfo, StockListResponse, KlineResponse, 
    LatestPrice, FinancialResponse, APIResponse
)
from services.query_service import QueryService

router = APIRouter()
query_service = QueryService()


@router.get("/search")
async def search_stocks(
    q: str = Query(..., min_length=1, description="股票代码或名称"),
    limit: int = Query(12, ge=1, le=50, description="返回数量"),
):
    """搜索股票"""
    try:
        items = query_service.search_stocks(q, limit)
        return APIResponse(
            data={
                "query": q,
                "count": len(items),
                "items": items,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list", response_model=StockListResponse)
async def get_stock_list(
    market: Optional[str] = Query(None, description="市场: SH/SZ"),
    industry: Optional[str] = Query(None, description="行业代码"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(100, ge=1, le=1000, description="每页数量")
):
    """获取股票列表"""
    try:
        stocks, total = query_service.get_stocks(market, industry, page, page_size)
        return StockListResponse(
            data=[StockInfo(**stock) for stock in stocks],
            total=total,
            page=page,
            page_size=page_size
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{stock_code}", response_model=StockInfo)
async def get_stock_info(stock_code: str):
    """获取单只股票信息"""
    try:
        stock = query_service.get_stock_by_code(stock_code)
        if not stock:
            raise HTTPException(status_code=404, detail=f"股票 {stock_code} 不存在")
        return StockInfo(**stock)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{stock_code}/industry")
async def get_stock_industry(stock_code: str):
    """获取股票所属行业"""
    try:
        result = query_service.get_stock_industry(stock_code)
        if not result:
            raise HTTPException(status_code=404, detail=f"股票 {stock_code} 不存在")
        return APIResponse(data=result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
