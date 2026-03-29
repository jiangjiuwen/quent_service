from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from config.settings import WEB_DIR, WEB_ASSETS_DIR

app = FastAPI(
    title="A股量化数据服务",
    description="提供A股历史数据查询API，支持股票信息、K线数据、财务数据查询",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.mount("/assets", StaticFiles(directory=WEB_ASSETS_DIR), name="assets")

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 导入路由
from api.routes import analysis, stocks, kline, financial, sync

# 注册路由
app.include_router(stocks.router, prefix="/api/v1/stocks", tags=["股票信息"])
app.include_router(kline.router, prefix="/api/v1/kline", tags=["K线数据"])
app.include_router(financial.router, prefix="/api/v1/financial", tags=["财务数据"])
app.include_router(analysis.router, prefix="/api/v1/analysis", tags=["量化分析"])
app.include_router(sync.router, prefix="/api/v1/sync", tags=["同步任务"])


@app.middleware("http")
async def disable_cache_for_dashboard(request: Request, call_next):
    """控制台页面与实时接口统一禁用缓存，避免浏览器展示陈旧状态"""
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path == "/health" or path.startswith("/assets/") or path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

@app.get("/")
async def root():
    """根路径，返回 Web 首页"""
    index_path = WEB_DIR / "index.html"
    styles_path = WEB_ASSETS_DIR / "styles.css"
    app_js_path = WEB_ASSETS_DIR / "app.js"

    def versioned_asset(path) -> str:
        return f"{int(path.stat().st_mtime)}" if path.exists() else "0"

    html = index_path.read_text(encoding="utf-8")
    html = html.replace(
        'href="/assets/styles.css"',
        f'href="/assets/styles.css?v={versioned_asset(styles_path)}"',
    )
    html = html.replace(
        'src="/assets/app.js"',
        f'src="/assets/app.js?v={versioned_asset(app_js_path)}"',
    )
    return HTMLResponse(content=html)


@app.get("/api/info")
async def api_info():
    """服务信息"""
    return {
        "service": "A股量化数据服务",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "running"
    }

@app.get("/health")
async def health():
    """健康检查接口"""
    try:
        # 检查数据库连接
        from database.connection import db
        result = db.fetchone("SELECT 1 as health_check")
        if result:
            return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.on_event("startup")
async def startup_event():
    """服务启动时执行"""
    from config.settings import DATA_DIR, LOG_DIR

    # 确保数据目录存在
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # 初始化数据库表
    from database.connection import db
    db.init_tables()
    print("服务启动完成，数据库已初始化")

@app.on_event("shutdown")
async def shutdown_event():
    """服务关闭时执行"""
    print("服务正在关闭...")
