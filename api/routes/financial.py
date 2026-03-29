from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from api.models.schemas import FinancialResponse, FinancialData, APIResponse
from services.query_service import QueryService

router = APIRouter()
query_service = QueryService()

@router.get("/{stock_code}", response_model=FinancialResponse)
async def get_financial_data(
    stock_code: str,
    report_period: Optional[str] = Query(None, description="报告期，如 2024Q3")
):
    """获取财务数据"""
    try:
        data = query_service.get_financial_data(stock_code, report_period)
        return FinancialResponse(
            stock_code=stock_code,
            data=[FinancialData(**row) for row in data]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{stock_code}/latest")
async def get_latest_financial(stock_code: str):
    """获取最新一期财务数据"""
    try:
        data = query_service.get_financial_data(stock_code)
        if not data:
            raise HTTPException(status_code=404, detail=f"股票 {stock_code} 无财务数据")
        return FinancialData(**data[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{stock_code}/indicators")
async def get_financial_indicators(stock_code: str):
    """获取关键财务指标"""
    try:
        data = query_service.get_financial_data(stock_code)
        if not data:
            raise HTTPException(status_code=404, detail=f"股票 {stock_code} 无财务数据")
        
        latest = data[0]
        indicators = {
            "stock_code": stock_code,
            "report_period": latest.get("report_period"),
            "roe": latest.get("roe"),  # 净资产收益率
            "eps": latest.get("eps"),  # 每股收益
            "gross_margin": latest.get("gross_margin"),  # 毛利率
            "debt_ratio": latest.get("debt_ratio"),  # 资产负债率
            "revenue": latest.get("revenue"),  # 营业收入
            "net_profit": latest.get("net_profit"),  # 净利润
        }
        return APIResponse(data=indicators)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
