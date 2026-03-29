from datetime import timedelta
import pandas as pd
import numpy as np
import logging, warnings
import a_ak_c
CH_DATABASE = 'stock_db'  # 自定义数据库名
DB_PATH = CH_DATABASE  # 保持常量名，但实际用于存储数据库名
INITIAL_CASH = 100000
DATA_START_DATE = '2023-05-01'  # 数据起始日期
BACKTEST_START = '2024-01-01'  # 回测起始日期
BACKTEST_END = '2025-12-19'  # 回测结束日期
# 全局缓存
_INDEX_CACHE = None
_FINANCIAL_CACHE=None
warnings.simplefilter(action='ignore', category=FutureWarning)# 忽略 pandas 警告

class Backtester:
    """回测引擎（微盘+一致性风控版）"""

    def __init__(self, db, initial_cash):
        self.db = db
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.holdings = {}
        self.trade_log = []
        self.daily_stats = []

        # 缓存结构
        self.data_cache = {}
        self.stock_info_cache = {}
        self.trading_dates = []

        # 微盘策略参数
        self.STOCK_NUM = 5  # 持仓数量（聚宽经典小市值）
        self.INDEX_CODE = '399101.XSHE'  # 中小板综指

        # 一致性风控参数
        self.consistency_control = True  # 是否启用一致性风控
        self.consistency_signal = False  # 清仓信号（False=满仓，True=空仓）
        self.mini_cosi_list = []  # 一致性历史数据（用于计算BOLL带）

        # 放量风控参数
        self.HV_control = True  # 是否启用放量止损
        self.HV_duration = 60  # 放量判断周期
        self.HV_ratio = 0.9  # 放量阈值（90%历史最大量）

        # 涨停股票追踪
        self.high_limit_list = []  # 昨日涨停列表
        self.history_hold_list = []  # 过去5天持仓历史
        self.limit_days = 5  # 涨停后N天不再买入

    def load_all_data_to_cache(self, start_date, end_date):
        """预加载全量数据到内存"""
        print("⚡ 正在预加载全量数据到内存...")

        # 1. 加载股票基础信息
        info_query = f"SELECT ticker, short_name, is_st FROM {self.db.db_path}.stock_list"
        info_res = self.db.client.query(info_query)
        for row in info_res.result_rows:
            self.stock_info_cache[row[0]] = {'short_name': row[1], 'is_st': row[2]}

        # 2. 计算数据加载起始时间（多加载250天用于年线计算）
        load_start = (pd.to_datetime(start_date) - timedelta(days=250)).strftime('%Y-%m-%d')

        # 3. 批量拉取K线数据
        kline_query = f"""
             SELECT ticker, date, open, close, high, low, volume, market_cap
             FROM {self.db.db_path}.daily_kline 
             WHERE date >= '{load_start}' AND date <= '{end_date}'
             ORDER BY ticker, date
         """
        df_all = self.db.client.query_df(kline_query)

        if not df_all.empty:
            df_all.columns = ['ticker', 'date', 'Open', 'Close', 'High', 'Low', 'Volume', 'Market Cap']
            df_all['date'] = pd.to_datetime(df_all['date'])
            df_all.set_index('date', inplace=True)
            self.data_cache = {k: v for k, v in df_all.groupby('ticker')}

        print(f"✅ 缓存完成: {len(self.data_cache)} 只股票")

    def get_price_from_cache(self, ticker, date_str):
        """从缓存获取收盘价"""
        try:
            df = self.data_cache.get(ticker)
            if df is not None:
                if date_str in df.index:
                    return df.loc[date_str]['Close']
                sliced = df.loc[:date_str]
                if not sliced.empty:
                    return sliced.iloc[-1]['Close']
        except:
            pass
        return 0

    def get_trading_dates(self, start_date, end_date):
        """获取交易日历"""
        query = f"""
         SELECT DISTINCT date 
         FROM {self.db.db_path}.daily_kline 
         WHERE ticker = '600000.SS' 
           AND date >= '{start_date}' 
           AND date <= '{end_date}'
         ORDER BY date ASC
         """
        result = self.db.client.query(query)
        if result.result_rows:
            return [pd.to_datetime(row[0]) for row in result.result_rows]
        return pd.date_range(start=start_date, end=end_date, freq='D')

    def get_micro_cap_candidates(self, date_str):
        """
        使用JQ的多因子选股逻辑：
        1. 三因子打分（financial_liability, VOL100, administration_expense_ttm）
        2. 按综合得分排序取前10%
        3. 按流通市值升序排序
        4. 过滤eps<=0
        5. 取前STOCK_NUM只
        """
        candidates = []

        for ticker, df in self.data_cache.items():
            if df is None or date_str not in df.loc[:date_str].index:
                continue

            # 过滤ST、科创板、北交所
            s_info = self.stock_info_cache.get(ticker)
            if not s_info or s_info['is_st'] == 1:
                continue
            if ticker.startswith('688') or ticker.startswith('8') or ticker.startswith('4'):
                continue

            # 过滤次新股（375天）
            df_slice = df.loc[:date_str]
            if len(df_slice) < 100:
                continue

            latest_row = df_slice.iloc[-1]

            # ✅ 关键：需要从数据库获取因子数据
            # 这里你需要添加获取因子的逻辑，如果没有因子数据，可以简化为：
            # - financial_liability: 用负债率代替
            # - VOL100: 用100日波动率
            # - administration_expense_ttm: 用管理费用TTM

            # 简化版：只用市值和波动率（如果没有完整因子数据）
            volatility = df_slice['Close'].pct_change().std() * np.sqrt(100) if len(df_slice) >= 100 else 0

            candidates.append({
                'ticker': ticker,
                'market_cap': latest_row['Market Cap'],
                'close': latest_row['Close'],
                'open': latest_row['Open'],
                'high': latest_row['High'],
                'volatility': volatility,
                'prev_close': df_slice['Close'].iloc[-2] if len(df_slice) >= 2 else latest_row['Close'],
                'short_name': s_info['short_name']
            })

        if not candidates:
            return []

        # ✅ 关键：按波动率排序取前10%（模拟因子打分）
        candidates.sort(key=lambda x: x['volatility'], reverse=True)
        top_10_pct = candidates[:max(int(len(candidates) * 0.1), 50)]

        # ✅ 关键：按市值升序排序
        top_10_pct.sort(key=lambda x: x['market_cap'])

        # TODO: 如果有eps数据，这里过滤eps<=0
        # top_10_pct = [c for c in top_10_pct if c.get('eps', 0) > 0]

        return top_10_pct[:self.STOCK_NUM]

    def buy_stock(self, ticker, price, high_price, qty, date, prev_close):
        """执行买入"""
        is_star_market = ticker.startswith('688')
        is_chi_next = ticker.startswith('300')
        limit_up_threshold = 0.20 if (is_star_market or is_chi_next) else 0.10

        stock_info = self.stock_info_cache.get(ticker, {})
        short_name = stock_info.get('short_name', ticker)

        # ✅ 修改：涨停检查使用前一日收盘价
        # JQ逻辑：current_data[stock].high_limit 是基于前一交易日收盘价计算的
        limit_price = prev_close * (1 + limit_up_threshold)

        if price >= limit_price * 0.999:  # 允许0.1%误差
            return None

        # 资金检查
        cost = price * qty * (1 + 0.0003)
        if cost > self.cash:
            return None

        self.cash -= cost
        self.holdings[ticker] = {
            'short_name': short_name,
            'qty': qty,
            'avg_cost': price,
            'buy_date': pd.to_datetime(date),
            'buy_initial_high': high_price,
            'period_high': high_price
        }
        self.trade_log.append({
            'date': date,
            'ticker': ticker,
            'action': 'BUY',
            'price': price,
            'qty': qty,
            'amount': cost
        })
        return cost

    def sell_stock(self, ticker, price, date, reason):
        """执行卖出"""
        holding = self.holdings[ticker]
        qty = holding['qty']
        revenue = price * qty
        commission = revenue * (0.001 + 0.0003)
        real_revenue = revenue - commission

        profit = real_revenue - (holding['avg_cost'] * qty)
        profit_rate = (profit / (holding['avg_cost'] * qty)) * 100

        self.cash += real_revenue
        del self.holdings[ticker]

        self.trade_log.append({
            'date': date,
            'ticker': ticker,
            'action': 'SELL',
            'price': price,
            'qty': qty,
            'amount': real_revenue,
            'profit': profit,
            'profit_rate': profit_rate,
            'reason': reason
        })
        return profit, profit_rate

    def run_backtest(self, start_date, end_date):
        """运行回测（微盘+一致性风控完整版 - 完全对齐JQ）"""
        print(f"\n📊 开始回测 - 微盘股+止损创新高")
        print(f"💰 初始资金: ¥{self.initial_cash:,.2f}")
        print(f"📋 持仓数量: {self.STOCK_NUM} 只")
        print(f"🎯 风控机制: 特殊月份清仓")
        print("=" * 100)

        # 预加载数据
        self.load_all_data_to_cache(start_date, end_date)
        self.trading_dates = self.get_trading_dates(start_date, end_date)

        header = f"{'日期':<12} | {'资产':<12} | {'日盈亏':<10} | {'仓位':<4} | {'风控信号':<15} | {'操作'}"
        print(header)
        print("-" * 100)

        prev_total_value = self.initial_cash
        last_rebalance_week = None

        for i, current_date in enumerate(self.trading_dates):
            date_str = current_date.strftime('%Y-%m-%d')
            current_month = current_date.month
            current_day = current_date.day
            current_week = current_date.isocalendar()[1]
            current_weekday = current_date.weekday()  # 0=周一, 4=周五

            # ========================================
            # 1️⃣ 开盘前准备（模拟JQ的prepare_stock_list @6:00）
            # ========================================

            # 1.1 判断关仓期（1月1-30日，4月1-30日）
            is_no_trade_period = (
                    (current_month == 1 and 1 <= current_day <= 30) or
                    (current_month == 4 and 1 <= current_day <= 30)
            )

            daily_bought = []
            daily_sold=[]
            # ========================================
            # 2️⃣ 关仓期处理
            # ========================================
            if is_no_trade_period:
                if self.holdings:
                    for ticker in list(self.holdings.keys()):
                        df = self.data_cache.get(ticker)
                        if df is None:
                            continue
                        df_slice = df.loc[:date_str]
                        if df_slice.empty or df_slice.index[-1].strftime('%Y-%m-%d') != date_str:
                            continue
                        curr_row = df_slice.iloc[-1]
                        if curr_row['Volume'] > 0:
                            self.sell_stock(ticker, curr_row['Close'], date_str, f"{current_month}月清仓")

                # 每日结算
                current_holdings_value = sum(
                    h['qty'] * self.get_price_from_cache(ticker, date_str)
                    for ticker, h in self.holdings.items()
                )
                total_value = self.cash + current_holdings_value
                daily_pnl = total_value - prev_total_value
                daily_return = (total_value - prev_total_value) / prev_total_value if prev_total_value > 0 else 0

                self.daily_stats.append({
                    'date': current_date,
                    'total_value': total_value,
                    'daily_return': daily_return,
                    'cash': self.cash,
                    'holdings': len(self.holdings)
                })

                print(
                    f"{date_str:<12} | {total_value:<12.0f} | {daily_pnl:<+10.0f} | "
                    f"{len(self.holdings):<4} | {'🚫关仓期':<15} | -"
                )

                prev_total_value = total_value
                continue  # 跳过所有后续交易逻辑

            else:
                # 在run_backtest的循环中，开盘和收盘止损检查前添加：
                # ========================================
                # ✅ 新增：6%止损检查（JQ每日9:45,  14:45检查3次）
                # ========================================
                for ticker in list(self.holdings.keys()):
                    holding = self.holdings[ticker]
                    df = self.data_cache.get(ticker)
                    df_slice = df.loc[:date_str]
                    curr_row = df_slice.iloc[-1]
                    # ✅ 关键：6%止损（JQ代码中是0.06）
                    if curr_row['Open'] < holding['avg_cost'] * 0.94:  # 1 - 0.06 = 0.94
                        self.sell_stock(ticker, curr_row['Open'], date_str, "止损-6%")
                        daily_sold.append(self.stock_info_cache.get(ticker, {}).get('short_name', ticker))
                for ticker in list(self.holdings.keys()):
                    holding = self.holdings[ticker]
                    df = self.data_cache.get(ticker)
                    df_slice = df.loc[:date_str]
                    curr_row = df_slice.iloc[-1]
                    if curr_row['Close'] < holding['avg_cost'] * 0.94:  # 1 - 0.06 = 0.94
                        self.sell_stock(ticker, curr_row['Close'], date_str, "止损-6%")
                        daily_sold.append(self.stock_info_cache.get(ticker, {}).get('short_name', ticker))

                if current_weekday == 2:  # 周三 早盘卖出 以开盘价近似
                    candidates = self.get_micro_cap_candidates(date_str)
                    target_tickers = [c['ticker'] for c in candidates]

                    for ticker in list(self.holdings.keys()):
                        # 不在目标池 且 不是昨日涨停
                        if ticker not in target_tickers:
                            df = self.data_cache.get(ticker)
                            if df is None:
                                continue
                            df_slice = df.loc[:date_str]
                            if df_slice.empty:
                                continue
                            if df_slice.iloc[-2]['Close']/df_slice.iloc[-3]['Close']>=1.099:
                                continue
                            curr_row = df_slice.iloc[-1]
                            if curr_row['Volume'] > 0:
                                profit, profit_rate = self.sell_stock(ticker, curr_row['Open'], date_str,
                                                                      "周三调仓卖出")
                                daily_sold.append(self.stock_info_cache.get(ticker, {}).get('short_name', ticker))

                # ========================================
                # ✅ 修改：周五9:30买入（用开盘价近似 - 注意这里有偏差）
                # ========================================
                if current_weekday == 4:  # 周五
                    candidates = self.get_micro_cap_candidates(date_str)

                    slots_available = self.STOCK_NUM - len(self.holdings)
                    if slots_available > 0 and self.cash > 0:
                        per_stock_cash = self.cash / slots_available

                        for cand in candidates:
                            if cand['ticker'] in self.holdings:
                                continue

                            ticker = cand['ticker']
                            is_star = ticker.startswith('688')
                            min_qty = 200 if is_star else 100

                            qty = max(min_qty, (per_stock_cash / cand['close']) // 100 * 100)
                            res = self.buy_stock(ticker, cand['close'], cand['high'], qty, date_str, cand['prev_close'])
                            if res:
                                daily_bought.append(cand['short_name'])

                            if len(self.holdings) >= self.STOCK_NUM:
                                break

            # ========================================
            # 7️⃣ 每日结算
            # ========================================
            current_holdings_value = sum(
                h['qty'] * self.get_price_from_cache(ticker, date_str)
                for ticker, h in self.holdings.items()
            )
            total_value = self.cash + current_holdings_value
            daily_pnl = total_value - prev_total_value
            daily_return = (total_value - prev_total_value) / prev_total_value if prev_total_value > 0 else 0

            self.daily_stats.append({
                'date': current_date,
                'total_value': total_value,
                'daily_return': daily_return,
                'cash': self.cash,
                'holdings': len(self.holdings)
            })

            # 格式化输出
            action_str = ""
            if daily_bought:
                action_str = f"买[{','.join(daily_bought[:2])}{'...' if len(daily_bought) > 2 else ''}]"
            if daily_sold:
                if action_str:
                    action_str += " "
                action_str += f"卖[{','.join(daily_sold[:2])}{'...' if len(daily_sold) > 2 else ''}]"
            if not action_str:
                action_str = "-"

            print(
                f"{date_str:<12} | {total_value:<12.0f} | {daily_pnl:<+10.0f} | "
                f"{len(self.holdings):<4} |  | {action_str}"
            )


            prev_total_value = total_value

        self.print_performance_metrics()

    def print_performance_metrics(self):
        """打印回测指标"""
        if not self.daily_stats:
            print("❌ 无回测数据")
            return

        df = pd.DataFrame(self.daily_stats)
        df.set_index('date', inplace=True)

        final_value = df['total_value'].iloc[-1]
        total_return = (final_value - self.initial_cash) / self.initial_cash * 100
        days = len(df)
        annualized_return = ((1 + total_return / 100) ** (252 / days) - 1) * 100 if days > 0 else 0

        risk_free_rate = 0.03
        daily_rf = risk_free_rate / 252
        df['excess_return'] = df['daily_return'] - daily_rf
        sharpe_ratio = 0
        if df['daily_return'].std() != 0:
            sharpe_ratio =(df['excess_return'].mean() / df['daily_return'].std()) * np.sqrt(252)
        df['cumulative_max'] = df['total_value'].cummax()
        df['drawdown'] = (df['total_value'] - df['cumulative_max']) / df['cumulative_max']
        max_drawdown = df['drawdown'].min() * 100

        sell_trades = [t for t in self.trade_log if t['action'] == 'SELL']
        win_trades = [t for t in sell_trades if t['profit'] > 0]
        win_rate = (len(win_trades) / len(sell_trades) * 100) if sell_trades else 0

        print("\n" + "=" * 60)
        print("📊 微盘股+一致性风控策略 - 最终回测报告")
        print("=" * 60)
        print(f"💰 初始资金   : ¥{self.initial_cash:,.2f}")
        print(f"💰 最终资产   : ¥{final_value:,.2f}")
        print(f"📈 总收益率   : {total_return:+.2f}%")
        print(f"📅 年化收益率 : {annualized_return:+.2f}%")
        print("-" * 60)
        print(f"⚡ 夏普比率   : {sharpe_ratio:.2f}")
        print(f"📉 最大回撤   : {max_drawdown:.2f}%")
        print("-" * 60)
        print(f"🎲 交易次数   : {len(sell_trades)} 次")
        print(f"✅ 胜率       : {win_rate:.2f}%")
        print("=" * 60)

def main():
    print("=" * 60)
    print("     A股量化交易系统 (自动断点续传版)")
    print("=" * 60)
    print("\n请选择功能：")
    print("1. 下载/补全 A股历史数据")
    print("2. 运行历史回测")
    print("3. 更新数据")
    print("0. 退出")

    choice = input("\n请输入选项 (0-3): ").strip()

    db =a_ak_c.StockDatabase(DB_PATH)  # 确保你有定义 DB_PATH 常量，例如 'stock_data.db'
    collector = a_ak_c.DataCollector(db)
    try:
        if choice == '1':
            # 只需要询问批次大小，不需要询问开始位置了
            batch_str = input("每批下载数量 (默认100): ").strip()
            batch_size = int(batch_str) if batch_str.isdigit() else 100

            collector = a_ak_c.DataCollector(db)
            # 这里的 start_date 请确保定义了 DATA_START_DATE 常量，例如 '2020-01-01'
            collector.download_all_stocks_data(DATA_START_DATE, batch_size=batch_size)
            collector.download_all_indexs_data(DATA_START_DATE)
            collector.download_financial_data(DATA_START_DATE)
        elif choice == '2':
            # 确保定义了 INITIAL_CASH, BACKTEST_START, BACKTEST_END
            backtester = Backtester(db, INITIAL_CASH)
            backtester.run_backtest(BACKTEST_START, BACKTEST_END)

        elif choice == '3':
            collector = a_ak_c.DataCollector(db)
            collector.incremental_update()
            pass

        elif choice == '0':
            print("👋 再见！")

        else:
            print("❌ 无效选项")

    except Exception as e:
        print(f"❌ 程序运行出错: {e}")
        import traceback
        traceback.print_exc()

    finally:
        db.close()
if __name__ == "__main__":
    main()