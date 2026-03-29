from pydantic import BaseModel
from typing import Any, List, Optional
from datetime import date, datetime
from decimal import Decimal

# ========== 股票信息模型 ==========

class StockInfo(BaseModel):
    """股票基础信息"""
    stock_code: str
    stock_name: str
    market_type: str
    list_date: Optional[date] = None
    delist_date: Optional[date] = None
    status: int = 1
    total_shares: Optional[Decimal] = None
    float_shares: Optional[Decimal] = None
    industry_code: Optional[str] = None
    
    class Config:
        from_attributes = True

class StockListResponse(BaseModel):
    """股票列表响应"""
    data: List[StockInfo]
    total: int
    page: int
    page_size: int

# ========== K线数据模型 ==========

class KlineData(BaseModel):
    """K线数据"""
    trade_date: date
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int
    amount: Optional[Decimal] = None
    turnover_rate: Optional[Decimal] = None
    pe_ratio: Optional[Decimal] = None
    pb_ratio: Optional[Decimal] = None
    
    class Config:
        from_attributes = True

class KlineResponse(BaseModel):
    """K线数据响应"""
    stock_code: str
    data: List[KlineData]

class LatestPrice(BaseModel):
    """最新价格"""
    stock_code: str
    trade_date: date
    close_price: Decimal
    change: Optional[Decimal] = None
    pct_change: Optional[Decimal] = None

# ========== 财务数据模型 ==========

class FinancialData(BaseModel):
    """财务数据"""
    stock_code: str
    report_period: str
    report_type: int
    announce_date: Optional[date] = None
    report_period_end: Optional[date] = None
    statement_type: Optional[str] = None
    currency: Optional[str] = None
    total_assets: Optional[Decimal] = None
    total_liabilities: Optional[Decimal] = None
    net_assets: Optional[Decimal] = None
    revenue: Optional[Decimal] = None
    net_profit: Optional[Decimal] = None
    eps: Optional[Decimal] = None
    roe: Optional[Decimal] = None
    gross_margin: Optional[Decimal] = None
    debt_ratio: Optional[Decimal] = None
    
    class Config:
        from_attributes = True

class FinancialResponse(BaseModel):
    """财务数据响应"""
    stock_code: str
    data: List[FinancialData]

# ========== 同步任务模型 ==========

class SyncTask(BaseModel):
    """同步任务"""
    task_id: str
    task_name: str
    status: str  # running, success, failed
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    progress: Optional[float] = None  # 0-100

class SyncStatus(BaseModel):
    """同步状态"""
    last_sync_time: Optional[datetime] = None
    last_sync_status: Optional[str] = None
    next_sync_time: Optional[datetime] = None
    total_stocks: int
    total_kline_records: int

# ========== 通用响应模型 ==========

class APIResponse(BaseModel):
    """通用API响应"""
    code: int = 200
    message: str = "success"
    data: Optional[Any] = None
