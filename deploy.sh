#!/bin/bash
# 部署脚本

echo "=== A股量化数据服务部署脚本 ==="

# 1. 安装依赖
echo "[1/5] 安装Python依赖..."
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 2. 创建必要的目录
echo "[2/5] 创建数据目录..."
mkdir -p /root/quant_service/data
mkdir -p /root/quant_service/logs

# 3. 初始化数据库
echo "[3/5] 初始化数据库..."
python3 -c "from database.connection import db; db.init_tables()"

# 4. 安装systemd服务
echo "[4/5] 安装系统服务..."
cp /root/quant_service/systemd/quant.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable quant

# 5. 启动服务
echo "[5/5] 启动服务..."
systemctl start quant

echo ""
echo "=== 部署完成 ==="
echo "服务状态: systemctl status quant"
echo "查看日志: journalctl -u quant -f"
echo "API地址: http://localhost:8000"
echo "API文档: http://localhost:8000/docs"
