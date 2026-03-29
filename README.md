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
cd /path/to/quent_service
pip install -r requirements.txt
```

### 2. 部署服务

```bash
./deploy.sh
```

### 3. 管理服务

```bash
# 查看状态
./scripts/prod_ctl.sh status

# 重启服务
./scripts/prod_ctl.sh restart

# 查看日志
./scripts/prod_ctl.sh logs
```

## 跨平台生产部署

本项目现在支持 macOS 和 Linux 两套生产部署方式，并且统一使用同一组命令：

- `./deploy.sh`
- `./scripts/prod_deploy.sh`
- `./scripts/prod_ctl.sh <command>`

脚本会自动识别当前系统：

- macOS 使用 `launchd`
- Linux 使用 `systemd`

首次部署：

```bash
./scripts/prod_deploy.sh
```

常用管理命令：

```bash
# 查看生产状态
./scripts/prod_ctl.sh status

# 启动生产服务
./scripts/prod_ctl.sh start

# 停止生产服务
./scripts/prod_ctl.sh stop

# 重启生产服务
./scripts/prod_ctl.sh restart

# 查看日志
./scripts/prod_ctl.sh logs

# 重新部署当前代码
./scripts/prod_ctl.sh deploy
```

macOS 额外支持代码变更自动部署监听：

```bash
./scripts/prod_ctl.sh watch-on
./scripts/prod_ctl.sh watch-off
./scripts/prod_ctl.sh watch-status
```

默认生产目录：

- 生产代码快照目录：`~/Library/Application Support/quant_service_prod/current`
- 生产数据库目录：`~/Library/Application Support/quant_service_prod/data`
- 生产日志目录：`~/Library/Application Support/quant_service_prod/logs`
- 生产端口：`18000`
- Linux 生产根目录默认是 `~/quant_service_prod`
- Linux 首次部署会自动写入 `systemd` 服务；如果当前用户不是 root，脚本会在需要时调用 `sudo`

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

仓库现在还包含基础 GitHub Actions 检查流程：

- 推送到 `main` 或向 `main` 发起 Pull Request 时自动执行
- 检查所有已跟踪 Python 文件的语法
- 检查前端脚本 `web/assets/app.js` 的语法

如果需要本地环境配置，可以复制一份示例文件再按实际环境修改：

```bash
cp .env.example .env
```

## GitHub 自动部署腾讯云

仓库已经补好了 GitHub Actions 部署工作流：

- 工作流文件：`.github/workflows/deploy-tencent-cloud.yml`
- 触发方式：推送到 `main`，或手动执行 `workflow_dispatch`
- 部署方式：GitHub Actions 通过 `SSH + rsync` 将当前代码同步到腾讯云 Linux 服务器，然后在服务器上执行 `./scripts/prod_deploy.sh`

这样做的好处是：

- 服务器不需要再单独配置 GitHub 拉取权限
- 部署的就是当前 GitHub 里的提交内容
- 仍然复用项目现有的 Linux `systemd` 部署脚本

### 服务器首次准备

以 Ubuntu 为例，先登录腾讯云服务器执行一次：

```bash
sudo apt update
sudo apt install -y python3 python3-venv rsync curl sqlite3
```

建议使用一个普通登录用户，例如 `ubuntu`，并给它最小够用的免密 `sudo` 权限，因为部署时需要写入 `systemd` 服务并重启服务：

```bash
echo 'ubuntu ALL=(ALL) NOPASSWD:/usr/bin/systemctl,/bin/systemctl,/usr/bin/install,/bin/install' | sudo tee /etc/sudoers.d/quant-service-deploy
sudo chmod 440 /etc/sudoers.d/quant-service-deploy
```

然后确认该用户可以免密执行：

```bash
sudo -n systemctl status
```

### GitHub 仓库配置

在 GitHub 仓库的 `Settings -> Secrets and variables -> Actions` 中新增：

Repository variables:

- `TENCENT_CVM_HOST`：腾讯云服务器公网 IP 或域名
- `TENCENT_CVM_PORT`：SSH 端口，默认可填 `22`
- `TENCENT_CVM_USER`：SSH 登录用户，例如 `ubuntu`
- `TENCENT_CVM_WORKSPACE`：服务器上的代码同步目录，例如 `/home/ubuntu/quent_service`
- `QUANT_PROD_ROOT`：生产目录，例如 `/home/ubuntu/quant_service_prod`
- `QUANT_PROD_API_PORT`：服务端口，例如 `18000`
- `QUANT_PROD_SERVICE_NAME`：systemd 服务名，例如 `quant-service`
- `QUANT_PROD_SERVICE_USER`：运行服务的 Linux 用户，例如 `ubuntu`

Repository secret:

- `TENCENT_CVM_SSH_KEY`：用于登录腾讯云服务器的私钥内容

### 推荐的 SSH key 配置方式

在本地生成一个单独用于部署的密钥对：

```bash
ssh-keygen -t ed25519 -f ~/.ssh/tencent_quant_deploy -C "github-actions-deploy"
```

把公钥追加到服务器目标用户的 `~/.ssh/authorized_keys`：

```bash
ssh-copy-id -i ~/.ssh/tencent_quant_deploy.pub ubuntu@your-server-ip
```

再把私钥内容填入 GitHub 的 `TENCENT_CVM_SSH_KEY`：

```bash
cat ~/.ssh/tencent_quant_deploy
```

### 部署触发

完成以上配置后，后续只要推送到 `main`：

```bash
git push origin main
```

GitHub Actions 就会自动：

- 连接腾讯云服务器
- 同步当前仓库代码
- 执行 `./scripts/prod_deploy.sh`
- 输出 `./scripts/prod_ctl.sh status` 的结果

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
