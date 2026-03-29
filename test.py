#!/usr/bin/env python3
"""测试脚本 - 验证服务各项功能"""

import requests
import sys

BASE_URL = "http://localhost:8000"

def test_health():
    """测试健康检查"""
    print("[1] 测试健康检查...")
    try:
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        if resp.status_code == 200:
            print(f"    ✓ 服务运行正常: {resp.json()}")
            return True
        else:
            print(f"    ✗ 服务异常: {resp.status_code}")
            return False
    except Exception as e:
        print(f"    ✗ 连接失败: {e}")
        return False

def test_stock_list():
    """测试股票列表接口"""
    print("[2] 测试股票列表...")
    try:
        resp = requests.get(f"{BASE_URL}/api/v1/stocks/list?page_size=5", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            print(f"    ✓ 获取到 {data.get('total', 0)} 只股票")
            return True
        else:
            print(f"    ✗ 请求失败: {resp.status_code}")
            return False
    except Exception as e:
        print(f"    ✗ 请求异常: {e}")
        return False

def test_stock_info():
    """测试股票详情"""
    print("[3] 测试股票详情...")
    try:
        resp = requests.get(f"{BASE_URL}/api/v1/stocks/600519", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            print(f"    ✓ 股票信息: {data.get('stock_name')} ({data.get('stock_code')})")
            return True
        else:
            print(f"    ✗ 请求失败: {resp.status_code}")
            return False
    except Exception as e:
        print(f"    ✗ 请求异常: {e}")
        return False

def test_kline():
    """测试K线数据"""
    print("[4] 测试K线数据...")
    try:
        resp = requests.get(f"{BASE_URL}/api/v1/kline/latest/600519", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            print(f"    ✓ 最新价格: {data.get('close_price')} (日期: {data.get('trade_date')})")
            return True
        else:
            print(f"    ✗ 请求失败: {resp.status_code}")
            return False
    except Exception as e:
        print(f"    ✗ 请求异常: {e}")
        return False

def test_sync_status():
    """测试同步状态"""
    print("[5] 测试同步状态...")
    try:
        resp = requests.get(f"{BASE_URL}/api/v1/sync/status", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            print(f"    ✓ 同步状态: {data.get('data', {})}")
            return True
        else:
            print(f"    ✗ 请求失败: {resp.status_code}")
            return False
    except Exception as e:
        print(f"    ✗ 请求异常: {e}")
        return False

def main():
    print("=== 量化数据服务测试 ===\n")
    
    results = []
    results.append(("健康检查", test_health()))
    results.append(("股票列表", test_stock_list()))
    results.append(("股票详情", test_stock_info()))
    results.append(("K线数据", test_kline()))
    results.append(("同步状态", test_sync_status()))
    
    print("\n=== 测试结果 ===")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    print(f"通过: {passed}/{total}")
    
    for name, result in results:
        status = "✓" if result else "✗"
        print(f"  {status} {name}")
    
    if passed == total:
        print("\n✓ 所有测试通过！")
        return 0
    else:
        print(f"\n✗ {total - passed} 项测试失败")
        return 1

if __name__ == "__main__":
    sys.exit(main())
