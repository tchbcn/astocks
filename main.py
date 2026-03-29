#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A股量化交易系统 - 主入口
支持策略选择、数据管理、回测运行
"""

import os
import sys
import argparse
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def print_banner():
    """打印启动横幅"""
    print("=" * 70)
    print("     A股量化交易系统 (astocks) - 专业版")
    print("=" * 70)
    print(f"     当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

def check_environment():
    """检查运行环境"""
    print("\n🔍 检查运行环境...")

    # Python版本
    python_version = sys.version.split()[0]
    print(f"  ✅ Python版本: {python_version}")

    # 检查关键依赖
    required_modules = {
        'akshare': 'akshare',
        'pandas': 'pandas',
        'numpy': 'numpy',
        'clickhouse_connect': 'clickhouse-connect',
        'tushare': 'tushare'
    }

    missing = []
    for module, package in required_modules.items():
        try:
            __import__(module)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} (未安装)")
            missing.append(package)

    if missing:
        print(f"\n⚠️  缺少依赖: {', '.join(missing)}")
        print("   请运行: pip install -r requirements.txt")
        return False

    return True

def main():
    """主函数"""
    print_banner()

    # 解析命令行参数
    parser = argparse.ArgumentParser(description='A股量化交易系统')
    parser.add_argument('--strategy', '-s', type=str,
                        choices=['a_mul', 'a_cap_sl', 'a_one', 'a_high', 'gs_aShare', 'a_h', 'a_h_c', 'a_h_c_t'],
                        help='选择策略运行')
    parser.add_argument('--mode', '-m', type=str,
                        choices=['backtest', 'live', 'download', 'diagnose'],
                        default='backtest',
                        help='运行模式')
    parser.add_argument('--config', '-c', type=str,
                        help='配置文件路径')
    parser.add_argument('--list', '-l', action='store_true',
                        help='列出所有可用策略')
    parser.add_argument('--test', '-t', action='store_true',
                        help='运行快速测试')
    args = parser.parse_args()

    # 检查环境
    if not check_environment():
        sys.exit(1)

    # 列出策略
    if args.list:
        print("\n📋 可用策略列表:")
        strategies = {
            'a_mul': '多因子微盘股策略 (A股)',
            'a_cap_sl': '市值流动性策略',
            'a_one': '单因子策略',
            'a_high': '高频策略',
            'gs_aShare': '两会特别策略 (A股)',
            'a_h': 'A股策略H',
            'a_h_c': 'A股策略H-C',
            'a_h_c_t': 'A股策略H-C-T'
        }
        for key, desc in strategies.items():
            print(f"  • {key:<15} - {desc}")
        return

    # 快速测试
    if args.test:
        print("\n🧪 运行快速测试...")
        try:
            import t_simple
            print("  ✅ 测试完成")
        except Exception as e:
            print(f"  ❌ 测试失败: {e}")
        return

    # 默认显示菜单
    if not args.strategy:
        print("\n📊 请选择运行模式:")
        print("  1. 运行策略回测")
        print("  2. 数据管理")
        print("  3. 诊断工具")
        print("  4. 列出所有策略")
        print("  0. 退出")

        try:
            choice = input("\n请输入选项 (0-4): ").strip()

            if choice == '1':
                print("\n可用策略:")
                strategies = ['a_mul', 'a_cap_sl', 'a_one', 'a_high', 'gs_aShare', 'a_h', 'a_h_c', 'a_h_c_t']
                for i, s in enumerate(strategies, 1):
                    print(f"  {i}. {s}")
                strategy_idx = int(input("选择策略编号: ")) - 1
                args.strategy = strategies[strategy_idx]
                args.mode = 'backtest'
            elif choice == '2':
                print("\n数据管理功能暂未集成到主入口")
                sys.exit(0)
            elif choice == '3':
                import u
                sys.exit(0)
            elif choice == '4':
                args.list = True
                main()  # 递归调用显示列表
                return
            elif choice == '0':
                print("再见！")
                sys.exit(0)
            else:
                print("无效选择")
                sys.exit(1)
        except (ValueError, IndexError):
            print("输入错误")
            sys.exit(1)

    # 运行策略
    print(f"\n🚀 运行策略: {args.strategy}")
    print(f"   模式: {args.mode}")

    try:
        strategy_module = __import__(args.strategy)

        # 检查是否有main函数
        if hasattr(strategy_module, 'main'):
            strategy_module.main()
        elif hasattr(strategy_module, 'Backtester'):
            # 回测类
            print(f"  ✅ 加载策略模块成功")
            print(f"  ℹ️  请直接运行 python {args.strategy}.py 以启动回测")
        else:
            print(f"  ⚠️  策略模块未找到入口，请直接运行该文件")
            print(f"     python {args.strategy}.py")

    except ImportError as e:
        print(f"  ❌ 导入失败: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"  ❌ 运行错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
