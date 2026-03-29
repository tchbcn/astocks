#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
快速测试 AkShare 财务API
"""

import akshare as ak

print("🧪 测试 AkShare 财务API")
print("=" * 60)

# 测试股票
code = "000001"

print(f"\n📊 测试股票: {code}")

# 测试1：东方财富利润表（按报告期）
print("\n1️⃣ 测试 stock_profit_sheet_by_report_em")
try:
    df = ak.stock_profit_sheet_by_report_em(symbol=code)
    if df is None or df.empty:
        print("   ❌ 返回空数据")
    else:
        print(f"   ✅ 成功！获取 {len(df)} 条记录")
        print(f"   📋 列名: {list(df.columns[:8])}")
        print(f"   📅 数据示例:\n{df[['报告期', '净利润']].head(3) if '净利润' in df.columns else df.head(3)}")
except Exception as e:
    print(f"   ❌ 失败: {e}")

# 测试2：东方财富资产负债表
print("\n2️⃣ 测试 stock_balance_sheet_by_report_em")
try:
    df = ak.stock_balance_sheet_by_report_em(symbol=code)
    if df is None or df.empty:
        print("   ❌ 返回空数据")
    else:
        print(f"   ✅ 成功！获取 {len(df)} 条记录")
        print(f"   📋 列名: {list(df.columns[:8])}")
except Exception as e:
    print(f"   ❌ 失败: {e}")

# 测试3：新浪财务摘要
print("\n3️⃣ 测试 stock_financial_abstract")
try:
    df = ak.stock_financial_abstract(symbol=code)
    if df is None or df.empty:
        print("   ❌ 返回空数据")
    else:
        print(f"   ✅ 成功！获取 {len(df)} 条记录")
        print(f"   📋 列名: {list(df.columns)}")
except Exception as e:
    print(f"   ❌ 失败: {e}")

# 测试4：同花顺主要指标
print("\n4️⃣ 测试 stock_financial_abstract_ths")
try:
    df = ak.stock_financial_abstract_ths(symbol=code, indicator="按报告期")
    if df is None or df.empty:
        print("   ❌ 返回空数据")
    else:
        print(f"   ✅ 成功！获取 {len(df)} 条记录")
        print(f"   📋 列名: {list(df.columns[:8])}")
except Exception as e:
    print(f"   ❌ 失败: {e}")

print("\n" + "=" * 60)
print("✅ 测试完成")
print("=" * 60)