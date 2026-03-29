# -*- coding: utf-8 -*-
"""
gs_aShare.py - A 股两会特别策略实盘版
策略核心：紧跟两会政策主线 (AI+ 机器人 + 半导体)
选股池：A 股分析团队精选的 10 只核心标的
交易逻辑：结合技术面 (布林带 + EMA) 与 AI 仓位管理 (t.py 简化版)
"""

import time
import datetime
import logging
import json
# 假设使用 akshare 获取 A 股数据 (需安装: pip install akshare)
import akshare as ak
import pandas as pd
import numpy as np

# ================= 配置区 =================
# A 股分析团队精选股票池 (两会特别版)
# 代码格式：sz/sh + 6 位数字
A_SHARE_STOCKS = [
    {"code": "sz002747", "name": "埃斯顿", "sector": "机器人"},
    {"code": "sz300124", "name": "汇川技术", "sector": "工控/机器人"},
    {"code": "sh603986", "name": "兆易创新", "sector": "半导体"},
    {"code": "sz300308", "name": "中际旭创", "sector": "光模块"},
    {"code": "sh688017", "name": "绿的谐波", "sector": "机器人零部件"},
    {"code": "sz002371", "name": "北方华创", "sector": "半导体设备"},
    # 港股通标的需特殊处理，此处暂列
    # {"code": "hk09880", "name": "优必选", "sector": "人形机器人"}, 
    {"code": "sh600031", "name": "三一重工", "sector": "工程机械"},
    {"code": "sz002475", "name": "立讯精密", "sector": "消费电子"},
    {"code": "sz002415", "name": "海康威视", "sector": "安防/AI"},
]

# 交易参数
INITIAL_CAPITAL = 100000  # 初始资金 10 万
POSITION_RATIO = 0.6      # 目标仓位 6 成 (根据两会报告建议)
STOP_LOSS_PCT = 0.08      # 止损 8%
TAKE_PROFIT_PCT = 0.20    # 止盈 20%

# 技术指标参数
BB_PERIOD = 20
BB_STD = 2
EMA_PERIOD = 10

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= 工具函数 =================

def get_stock_data(symbol: str, period: str = "daily") -> pd.DataFrame:
    """获取 A 股历史行情数据 (使用 akshare)"""
    try:
        # 移除大小写前缀，统一格式
        code = symbol.lower().replace("sz", "").replace("sh", "")
        if symbol.lower().startswith("sz"):
            df = ak.stock_zh_a_hist(symbol=f"sz{code}", period="daily", adjust="qfq")
        else:
            df = ak.stock_zh_a_hist(symbol=f"sh{code}", period="daily", adjust="qfq")
        
        # 重命名列以匹配计算逻辑
        df = df.rename(columns={
            "收盘": "close", "开盘": "open", "最高": "high", "最低": "low", "成交量": "volume"
        })
        return df
    except Exception as e:
        logger.error(f"获取 {symbol} 数据失败：{e}")
        return pd.DataFrame()

def calculate_bb_ema(df: pd.DataFrame) -> pd.DataFrame:
    """计算布林带和 EMA"""
    if df.empty:
        return df
    
    # 布林带
    df['bb_mid'] = df['close'].rolling(window=BB_PERIOD).mean()
    df['bb_std'] = df['close'].rolling(window=BB_PERIOD).std()
    df['bb_upper'] = df['bb_mid'] + (BB_STD * df['bb_std'])
    df['bb_lower'] = df['bb_mid'] - (BB_STD * df['bb_std'])
    
    # EMA
    df['ema'] = df['close'].ewm(span=EMA_PERIOD, adjust=False).mean()
    
    return df

def generate_signal(df: pd.DataFrame) -> dict:
    """
    生成交易信号
    逻辑：
    1. 价格 < 布林带下轨 且 出现反弹 -> 买入
    2. 价格 > 布林带上轨 -> 卖出/减仓
    3. 价格 > EMA 且 趋势向上 -> 持有
    """
    if len(df) < 30:
        return {"signal": "HOLD", "reason": "数据不足"}
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 买入信号：价格触及下轨并反弹
    if prev['close'] < prev['bb_lower'] and last['close'] > last['bb_lower']:
        return {"signal": "BUY", "reason": "触及下轨反弹"}
    
    # 卖出信号：价格触及上轨
    if last['close'] > last['bb_upper']:
        return {"signal": "SELL", "reason": "触及上轨"}
    
    # 趋势持有：价格在 EMA 之上
    if last['close'] > last['ema']:
        return {"signal": "HOLD", "reason": "趋势向上"}
        
    return {"signal": "WAIT", "reason": "观望"}

# ================= 主循环 =================

def run_strategy():
    logger.info("🚀 启动 A 股两会特别策略...")
    logger.info(f"📊 股票池：{len(A_SHARE_STOCKS)} 只精选标的")
    logger.info(f"💰 初始资金：{INITIAL_CAPITAL}, 目标仓位：{POSITION_RATIO*100}%")
    
    while True:
        now = datetime.datetime.now()
        
        # 检查是否为 A 股交易时间 (周一至周五 9:30-11:30, 13:00-15:00)
        if now.weekday() >= 5: # 周末不运行
            time.sleep(60)
            continue
            
        current_time = now.time()
        if not (datetime.time(9, 30) <= current_time <= datetime.time(11, 30) or 
                datetime.time(13, 0) <= current_time <= datetime.time(15, 0)):
            time.sleep(60)
            continue

        logger.info(f"\n--- {now.strftime('%Y-%m-%d %H:%M')} 扫描开始 ---")
        
        for stock in A_SHARE_STOCKS:
            symbol = stock['code']
            name = stock['name']
            
            # 获取数据
            df = get_stock_data(symbol)
            if df.empty:
                continue
                
            # 计算指标
            df = calculate_bb_ema(df)
            
            # 生成信号
            signal_info = generate_signal(df)
            signal = signal_info['signal']
            
            if signal != "WAIT":
                price = df['close'].iloc[-1]
                logger.info(f"🎯 {name}({symbol}): {signal} - {signal_info['reason']} (现价：{price})")
                # TODO: 此处接入实盘下单逻辑 (需对接券商 API 或 模拟盘)
                # order(symbol, signal, price)
        
        # 每 60 秒扫描一次
        time.sleep(60)

if __name__ == "__main__":
    try:
        run_strategy()
    except KeyboardInterrupt:
        logger.info("策略停止")
    except Exception as e:
        logger.error(f"策略异常：{e}")
