# ============================================================================
# 诊断脚本：找出股票下载失败的具体原因
# ============================================================================

import akshare as ak
import pandas as pd
import time

# 测试几个具体的股票
test_stocks = [
    ('301349', 'SZ'),  # 你遇到error的那个
    ('000001', 'SZ'),  # 深圳第一只
    ('600000', 'SS'),  # 上海第一只
    ('300001', 'SZ'),  # 创业板第一只
]

print("=" * 60)
print("🔍 诊断股票数据下载问题")
print("=" * 60)

for code, market in test_stocks:
    ticker = f"{code}.{market}"
    print(f"\n测试股票: {ticker}")
    print("-" * 40)

    try:
        # 测试下载
        print(f"  尝试下载K线数据...")
        hist = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date="20230501",
            end_date="20250224",
            adjust="qfq"
        )

        if hist is None:
            print(f"  ❌ 返回 None")
        elif hist.empty:
            print(f"  ⚠️  数据为空 DataFrame")
        else:
            print(f"  ✅ 成功! 获取 {len(hist)} 行数据")
            print(f"     列名: {list(hist.columns)}")
            print(f"     样本:\n{hist.head(2)}")

    except TypeError as e:
        print(f"  ❌ TypeError: {e}")
        print(f"     原因: 参数类型错误，检查 symbol/period/adjust")

    except ValueError as e:
        print(f"  ❌ ValueError: {e}")
        print(f"     原因: 参数值错误，可能是日期格式或股票代码")

    except KeyError as e:
        print(f"  ❌ KeyError: {e}")
        print(f"     原因: 返回数据的列不符合预期")

    except ConnectionError as e:
        print(f"  ❌ ConnectionError: {e}")
        print(f"     原因: 网络连接问题")

    except Exception as e:
        print(f"  ❌ {type(e).__name__}: {e}")

    time.sleep(1)

print("\n" + "=" * 60)
print("🔧 常见问题和解决方案")
print("=" * 60)

print("""
问题1：全部都是 error
原因: 很可能是某个参数错误
检查:
  - symbol 应该是数字代码，如 "301349"，不是 "301349.SZ"
  - period 应该是 "daily"
  - adjust 应该是 "qfq" 或 "hfq"
  - 日期格式必须是 "YYYYMMDD"

问题2：某些股票 error，某些成功
原因: 该股票可能已退市或刚上市
解决: 添加 try-except，跳过失败的股票

问题3：返回 None 或空 DataFrame
原因: 
  - 股票代码有效但无数据
  - 日期范围有问题
  - AkShare API 返回异常
解决: 检查返回值是否为 None/empty，跳过

问题4：列名不对（如预期 "日期"，实际 "Date"）
原因: AkShare 可能返回英文列名或中文列名
解决: 动态检测列名，映射到统一名称
""")