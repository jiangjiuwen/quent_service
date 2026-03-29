# A股量化数据服务

提供A股历史数据查询API，支持股票信息、K线数据、财务数据查询，并自动定时同步最新数据。

## 功能特性

- 📊 **股票信息查询** - 基础信息、行业分类
- 📈 **K线数据查询** - 日K线、历史数据、最新价格
- 💰 **财务数据查询** - 财务报表、关键指标
- 🔄 **自动定时同步** - 每日收盘后自动更新
- 🚀 **高性能API** - 基于FastAPI，支持异步
- 📱 **RESTful接口** - 标准REST API，易于集成

## 项目结构

```
quant_service/
├── api/                    # Web API
│   ├── main.py            # FastAPI入口
│   ├── models/            # 数据模型
│   └── routes/            # API路由
├── config/                # 配置
├── database/              # 数据库
├── services/              # 业务服务
├── sync/                  # 同步任务
├── utils/                 # 工具函数
├── data/                  # 数据库文件
├── logs/                  # 日志文件
├── main.py                # 服务入口
├── deploy.sh              # 部署脚本
└── requirements.txt       # 依赖
```

## 快速开始

### 1. 安装依赖

```bash
cd /root/quant_service
pip install -r requirements.txt
```

### 2. 部署服务

```bash
./deploy.sh
```

### 3. 管理服务

```bash
# 查看状态
systemctl status quant

# 启动服务
systemctl start quant

# 停止服务
systemctl stop quant

# 重启服务
systemctl restart quant

# 查看日志
journalctl -u quant -f
```

## macOS 常驻生产服务

本项目现在支持在 macOS 上以 `launchd` 常驻运行，并与本地开发环境隔离：

- 生产代码快照目录：`~/Library/Application Support/quant_service_prod/current`
- 生产数据库目录：`~/Library/Application Support/quant_service_prod/data`
- 生产日志目录：`~/Library/Application Support/quant_service_prod/logs`
- 生产端口：`18000`

首次部署并启动历史回填：

```bash
./scripts/prod_deploy.sh --start-backfill
```

常用管理命令：

```bash
# 查看生产状态
./scripts/prod_ctl.sh status

# 手动更新生产版本
./scripts/prod_ctl.sh deploy

# 手动启动历史回填
./scripts/prod_ctl.sh backfill-start

# 开启代码变更自动部署
./scripts/prod_ctl.sh watch-on

# 关闭代码变更自动部署
./scripts/prod_ctl.sh watch-off
```

## Git 管理

仓库地址：

```bash
https://github.com/jiangjiuwen/quent_service.git
```

推荐使用流程：

```bash
# 首次克隆
git clone https://github.com/jiangjiuwen/quent_service.git
cd quent_service

# 查看当前状态
git status

# 拉取远端最新代码
git pull --rebase origin main

# 新建功能分支
git checkout -b feat/your-change

# 提交改动
git add .
git commit -m "feat: describe your change"

# 推送分支
git push -u origin feat/your-change
```

当前仓库已经通过 `.gitignore` 排除了这些不应入库的运行态文件：

- 本地数据库文件 `data/*.db`
- 锁文件 `data/task_locks/*.lock`
- 日志目录 `logs/`
- 虚拟环境 `.venv/`
- Python 缓存与测试产物

如果需要保存部署配置，建议提交脱敏后的示例文件，例如 `.env.example`，不要直接提交真实密钥、生产数据库和日志文件。

## API接口

### 基础信息

- `GET /` - 服务信息
- `GET /health` - 健康检查
- `GET /docs` - API文档（Swagger UI）

### 股票接口

- `GET /api/v1/stocks/list` - 股票列表
- `GET /api/v1/stocks/{stock_code}` - 股票详情
- `GET /api/v1/stocks/{stock_code}/industry` - 所属行业

### K线接口

- `GET /api/v1/kline/daily/{stock_code}` - 日K线数据
- `GET /api/v1/kline/latest/{stock_code}` - 最新价格
- `GET /api/v1/kline/history/{stock_code}` - 历史数据

### 财务接口

- `GET /api/v1/financial/{stock_code}` - 财务数据
- `GET /api/v1/financial/{stock_code}/latest` - 最新财报
- `GET /api/v1/financial/{stock_code}/indicators` - 关键指标

### 同步任务

- `GET /api/v1/sync/status` - 同步状态
- `POST /api/v1/sync/daily` - 触发日线同步
- `POST /api/v1/sync/stocks` - 触发股票列表同步
- `GET /api/v1/sync/logs` - 同步日志

## 使用示例

```bash
# 查询股票列表
curl http://localhost:8000/api/v1/stocks/list

# 查询单只股票
curl http://localhost:8000/api/v1/stocks/600519

# 查询K线数据
curl "http://localhost:8000/api/v1/kline/daily/600519?limit=10"

# 查询最新价格
curl http://localhost:8000/api/v1/kline/latest/600519

# 查询财务数据
curl http://localhost:8000/api/v1/financial/600519
```

## 定时任务

- **每日15:30** - 同步日线数据（收盘后）
- **每日01:00** - 同步股票列表
- **每周日02:00** - 同步财务数据

## 数据库

SQLite数据库位置：`/root/quant_service/data/a_stock_quant.db`

主要表：
- `stocks` - 股票基础信息
- `daily_kline` - 日K线数据
- `financial_reports` - 财务报表
- `industries` - 行业分类
- `sync_logs` - 同步日志

## 配置

配置文件：`config/settings.py`

```python
DB_PATH = "/root/quant_service/data/a_stock_quant.db"
API_HOST = "0.0.0.0"
API_PORT = 8000
SYNC_HOUR = 15
SYNC_MINUTE = 30
```

## 日志

日志位置：`/root/quant_service/logs/`

- `sync.log` - 同步任务日志
- `api.log` - API访问日志
- `error.log` - 错误日志

## 技术栈

- **Web框架**: FastAPI
- **数据库**: SQLite
- **定时任务**: APScheduler
- **数据源**: akshare, baostock
- **部署**: Systemd

## 许可证

MIT
