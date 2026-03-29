from datetime import timedelta
import akshare as ak
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
        self.STOCK_NUM = 10  # 持仓数量（聚宽经典小市值）
        self.INDEX_CODE = '399101.XSHE'  # 中小板综指

        # 一致性风控参数
        self.consistency_control = True  # 是否启用一致性风控
        self.consistency_signal = False  # 清仓信号（False=满仓，True=空仓）
        self.mini_cosi_list = []  # 一致性历史数据（用于计算BOLL带）

        # 放量风控参数
        self.HV_control = False  # 是否启用放量止损
        self.HV_duration = 60  # 放量判断周期
        self.HV_ratio = 0.9  # 放量阈值（90%历史最大量）

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

    def check_market_bull_or_bear(self, date_str):
        """
        判断市场牛熊（基于沪指年线）
        True=牛市（关闭一致性检查），False=熊市（开启一致性检查）
        """
        global _INDEX_CACHE

        # 如果缓存为空，加载数据并预计算MA200
        if _INDEX_CACHE is None:
            index_ticker = '000001.SS'
            df = self.db.get_index_daily_data(index_ticker, None, None)
            df = df.sort_index()
            df['MA200'] = df['Close'].rolling(window=200, min_periods=200).mean()
            _INDEX_CACHE = df[['Close', 'MA200']]

        # 从缓存查询指定日期
        if date_str in _INDEX_CACHE.index:
            row = _INDEX_CACHE.loc[date_str]
        else:
            valid_dates = _INDEX_CACHE.index[_INDEX_CACHE.index <= date_str]
            if len(valid_dates) == 0:
                return True  # 数据不足时默认牛市
            row = _INDEX_CACHE.loc[valid_dates[-1]]

        if pd.isna(row['MA200']):
            return True  # MA200数据不足时默认牛市

        return row['Close'] > row['MA200']

    def calculate_consistency_signal(self, date_str):
        """计算一致性信号（完全对齐JQ）"""
        # 1. 牛熊判断
        is_bull = self.check_market_bull_or_bear(date_str)
        if is_bull:
            return False

        # 2. 获取Mini5%股票池
        all_tickers = []
        for ticker, df in self.data_cache.items():
            if df is None or df.empty or date_str not in df.loc[:date_str].index:
                continue
            s_info = self.stock_info_cache.get(ticker)
            if not s_info or s_info['is_st'] == 1 or ticker.startswith('688'):
                continue

            df_slice = df.loc[:date_str]
            if len(df_slice) < 20:
                continue

            latest_row = df_slice.iloc[-1]
            all_tickers.append({
                'ticker': ticker,
                'market_cap': latest_row['Market Cap'],
                'close': latest_row['Close'],
                'prev_close': df_slice['Close'].iloc[-2] if len(df_slice) >= 2 else latest_row['Close']
            })

        if len(all_tickers) < 100:
            return self.consistency_signal

        all_tickers.sort(key=lambda x: x['market_cap'])
        mini_pool = all_tickers[:int(len(all_tickers) * 0.05)]

        # ✅ 修改：一致性区间计算
        change_pcts = [(s['close'] / s['prev_close'] - 1) * 100 for s in mini_pool]
        chg_median = np.median(change_pcts)  # 用于信号判断
        chg_mean = np.mean(change_pcts)  # ✅ 关键：用于计算一致性区间
        chg_std = np.std(change_pcts)

        # ✅ 关键修改：一致性区间用 均值±1标准差
        within_1std = [c for c in change_pcts if (chg_mean - chg_std) < c < (chg_mean + chg_std)]
        consistency = len(within_1std) / len(change_pcts)

        self.mini_cosi_list.append(consistency)

        # BOLL带计算
        if len(self.mini_cosi_list) >= 120:
            cosi_mean = np.mean(self.mini_cosi_list[-120:])
            cosi_std = np.std(self.mini_cosi_list[-120:])
        else:
            cosi_mean = 0.80
            cosi_std = 0.05

        cosi_upper = cosi_mean + cosi_std

        # ✅ 修改：信号判断逻辑
        if chg_median < -2 and consistency >= cosi_upper:
            return True  # 清仓
        elif chg_median > 2 and consistency >= cosi_mean:  # ✅ 注意：这里用cosi_mean，不是cosi_upper
            return False  # 满仓
        else:
            return self.consistency_signal  # 保持原状态

    def get_micro_cap_candidates(self, date_str):
        """
        获取微盘股候选池（严格模拟JQ的中小板综指最小市值选股）
        """
        candidates = []
        df = ak.index_stock_cons(symbol="399101")
        # AkShare返回的列名可能是 '品种代码' 或 'code'
        if '品种代码' in df.columns:
            codes = df['品种代码'].tolist()
        elif 'code' in df.columns:
            codes = df['code'].tolist()
        # 转换为标准格式（000001 -> 000001.SZ）
        standard_codes = []
        for code in codes:
            if code.startswith('6'):
                standard_codes.append(f"{code}.SS")
            elif code.startswith(('0', '3')):
                standard_codes.append(f"{code}.SZ")

        print(f"✅ 获取中小板综指成分股: {len(standard_codes)} 只")
        for ticker, df in self.data_cache.items():
            if ticker not in standard_codes:
                continue
            if date_str not in df.loc[:date_str].index:
                continue

            # 过滤ST
            s_info = self.stock_info_cache.get(ticker)
            if not s_info or s_info['is_st'] == 1:
                continue

            df_slice = df.loc[:date_str]
            latest_row = df_slice.iloc[-1]

            candidates.append({
                'ticker': ticker,
                'market_cap': latest_row['Market Cap'],
                'close': latest_row['Close'],
                'open': latest_row['Open'],
                'high': latest_row['High'],
                'prev_close': df_slice['Close'].iloc[-2] if len(df_slice) >= 2 else latest_row['Close'],
                'short_name': s_info['short_name']
            })

        # 按流通市值升序排序，取前STOCK_NUM只
        candidates.sort(key=lambda x: x['market_cap'])
        return candidates[:self.STOCK_NUM]
    def check_high_volume_sell(self, ticker, date_str):
        """
        放量止损检查（JQ的check_high_volume逻辑）
        """
        if not self.HV_control:
            return False

        df = self.data_cache.get(ticker)
        if df is None:
            return False

        df_slice = df.loc[:date_str]
        if len(df_slice) < self.HV_duration:
            return False

        recent_volumes = df_slice['Volume'].iloc[-self.HV_duration:]
        current_volume = recent_volumes.iloc[-1]
        max_volume = recent_volumes.max()

        # 当日成交量 > 90%历史最大量
        if current_volume > self.HV_ratio * max_volume:
            return True
        return False

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

        if price >= limit_price * 0.9995:  #  涨停买不入，允许0.1%误差
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
        print(f"\n📊 开始回测 - 微盘股+一致性风控策略")
        print(f"💰 初始资金: ¥{self.initial_cash:,.2f}")
        print(f"📋 持仓数量: {self.STOCK_NUM} 只")
        print(f"🎯 风控机制: 一致性BOLL带 + 放量止损 + 特殊月份清仓")
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

            # ========================================
            # 1️⃣ 开盘前准备（模拟JQ的prepare_stock_list @6:00）
            # ========================================

            # 1.1 判断关仓期（1月1-30日，4月1-30日）
            is_no_trade_period = (
                    (current_month == 1 and 1 <= current_day <= 30) or
                    (current_month == 4 and 1 <= current_day <= 30)
            )

            # 1.2 更新昨日涨停列表（基于前一交易日数据）
            if i > 0:
                prev_date_str = self.trading_dates[i - 1].strftime('%Y-%m-%d')


            # 1.3 一致性风控检查（每日）
            if self.consistency_control:
                self.consistency_signal = self.calculate_consistency_signal(date_str)

            signal_str = "🔴清仓" if self.consistency_signal else "🟢满仓"

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

            # ========================================
            # 3️⃣ 一致性清仓信号处理
            # ========================================
            if self.consistency_signal and self.holdings:
                for ticker in list(self.holdings.keys()):
                    df = self.data_cache.get(ticker)
                    if df is None:
                        continue
                    df_slice = df.loc[:date_str]
                    if df_slice.empty or df_slice.index[-1].strftime('%Y-%m-%d') != date_str:
                        continue
                    curr_row = df_slice.iloc[-1]
                    if curr_row['Volume'] > 0:
                        self.sell_stock(ticker, curr_row['Close'], date_str, "一致性风控清仓")

            # ========================================
            # 6️⃣ 每周一 开盘调仓（模拟JQ的weekly_adjustment @9:30）
            # ========================================
            is_monday = current_date.weekday() == 0
            daily_bought = []  # 存名称，用于打印 (e.g., "同洲电子")
            daily_bought_tickers = []  # 存代码，用于 T+1 检查 (e.g., "002052.SZ")
            daily_sold = []  # 存名称，用于打印
            daily_sold_tickers = []  # 存代码，用于防止当日回补

            if is_monday and current_week != last_rebalance_week and not self.consistency_signal:
                last_rebalance_week = current_week

                # 获取微盘股候选池
                candidates = self.get_micro_cap_candidates(prev_date_str)
                target_tickers = [c['ticker'] for c in candidates]

                # 卖出不在目标池且不在涨停列表的股票
                for ticker in list(self.holdings.keys()):
                    if ticker not in target_tickers:
                        df = self.data_cache.get(ticker)
                        if df is None:
                            continue
                        df_slice = df.loc[:date_str]
                        if df_slice.empty or df_slice.index[-1].strftime('%Y-%m-%d') != date_str:
                            continue
                        pre_row = df_slice.iloc[-2]
                        curr_row = df_slice.iloc[-1]
                        # 计算涨停阈值
                        is_star_or_chi = ticker.startswith(('688', '300'))
                        limit_ratio = 1.20 if is_star_or_chi else 1.10
                        # 今日开盘没有涨停才允许卖
                        if curr_row['Open'] < pre_row['Close'] * limit_ratio * 0.9995:
                            profit, profit_rate = self.sell_stock(ticker, curr_row['Open'], date_str, "调仓卖出")
                            stock_info = self.stock_info_cache.get(ticker, {})
                            short_name = stock_info.get('short_name', ticker)
                            daily_sold.append(short_name)
                            daily_sold_tickers.append(ticker)
                # 买入新股票
                slots_available = self.STOCK_NUM - len(self.holdings)
                if slots_available > 0 and self.cash > 0:
                    per_stock_cash = self.cash / slots_available

                    for cand in candidates:
                        if cand['ticker'] in self.holdings:
                            continue

                        ticker = cand['ticker']
                        is_star = ticker.startswith('688')
                        min_qty = 200 if is_star else 100

                        qty = max(min_qty, (per_stock_cash / cand['open']) // 100 * 100)
                        res = self.buy_stock(ticker, cand['open'], cand['high'], qty, date_str, cand['prev_close'])
                        if res:
                            daily_bought.append(cand['short_name'])
                            daily_bought_tickers.append(ticker)
                        if len(self.holdings) >= self.STOCK_NUM:
                            break

            # 14：55-收盘检查昨天涨停，但是今天没有涨停就卖出
            for ticker in list(self.holdings.keys()):
                if ticker in daily_bought_tickers:
                    continue
                df = self.data_cache.get(ticker)
                if df is None:
                    continue
                df_slice = df.loc[:date_str]
                if df_slice.empty:
                    continue
                pre_2row=df_slice.iloc[-3]
                pre_row=df_slice.iloc[-2]
                curr_row = df_slice.iloc[-1]
                # 计算涨停阈值
                is_star_or_chi = ticker.startswith(('688', '300'))
                limit_ratio = 1.20 if is_star_or_chi else 1.10

                # 昨日涨停判断
                if pre_row['Close'] >= pre_2row['Close'] * limit_ratio * 0.9995:
                    if curr_row['Close']<pre_row['Close']* limit_ratio * 0.9995:
                        stock_info = self.stock_info_cache.get(ticker, {})
                        short_name = stock_info.get('short_name', ticker)
                        self.sell_stock(ticker, curr_row['Close'], date_str, "涨停打开")
                        daily_sold.append(short_name)
                        daily_sold_tickers.append(ticker)
                        self.reason_to_sell = 'limitup'  # 触发次日补仓

            # ========================================
            # 5️⃣ 14:55 - 放量止损检查（模拟JQ的check_high_volume）
            # ========================================
            for ticker in list(self.holdings.keys()):
                 if ticker in daily_bought_tickers:
                     continue
                 if self.check_high_volume_sell(ticker, date_str):
                    df = self.data_cache.get(ticker)
                    if df is None:
                        continue

                    df_slice = df.loc[:date_str]
                    if df_slice.empty:
                        continue

                    pre_2row = df_slice.iloc[-3]
                    pre_row = df_slice.iloc[-2]
                    curr_row = df_slice.iloc[-1]
                    limit_ratio = 1.20 if is_star_or_chi else 1.10

                    # 只有没有涨停才允许卖
                    if curr_row['Close'] < pre_row['Close'] * limit_ratio * 0.9995:
                        stock_info = self.stock_info_cache.get(ticker, {})
                        short_name = stock_info.get('short_name', ticker)
                        self.sell_stock(ticker, curr_row['Close'], date_str, "天量")
                        daily_sold.append(short_name)
                        daily_sold_tickers.append(ticker)
                        self.reason_to_sell = 'limitup'  # 触发次日补仓
                    # 计算涨停阈值
                    is_star_or_chi = ticker.startswith(('688', '300'))
                    limit_ratio = 1.20 if is_star_or_chi else 1.10

                    # 只有没有涨停才允许卖
                    if curr_row['Close'] < pre_row['Close'] * limit_ratio * 0.9995:
                        stock_info = self.stock_info_cache.get(ticker, {})
                        short_name = stock_info.get('short_name', ticker)
                        self.sell_stock(ticker, curr_row['Close'], date_str, "天量")
                        daily_sold.append(short_name)
                        daily_sold_tickers.append(ticker)
                        self.reason_to_sell = 'limitup'  # 触发次日补仓

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
                f"{len(self.holdings):<4} | {signal_str:<15} | {action_str}"
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

    db = a_ak_c.StockDatabase(DB_PATH)  # 确保你有定义 DB_PATH 常量，例如 'stock_data.db'
    collector = a_ak_c.DataCollector(db)
    # collector.download_financial_data(DATA_START_DATE)
    # collector.download_all_indexs_data(DATA_START_DATE)
    # hist = ak.stock_zh_a_hist(
    #     symbol="688593",
    #     period="daily",
    #     start_date='20240101'.replace('-', ''),
    #     end_date=datetime.now().strftime('%Y%m%d'),
    #     adjust="qfq"  # 前复权
    # )
    # db.update_short_names()
    try:
        if choice == '1':
            # 只需要询问批次大小，不需要询问开始位置了
            batch_str = input("每批下载数量 (默认100): ").strip()
            batch_size = int(batch_str) if batch_str.isdigit() else 100

            collector = DataCollector(db)
            # 这里的 start_date 请确保定义了 DATA_START_DATE 常量，例如 '2020-01-01'
            collector.download_all_stocks_data(DATA_START_DATE, batch_size=batch_size)
            collector.download_all_indexs_data(DATA_START_DATE)
            collector.download_financial_data(DATA_START_DATE)
        elif choice == '2':
            # 确保定义了 INITIAL_CASH, BACKTEST_START, BACKTEST_END
            backtester = Backtester(db, INITIAL_CASH)
            backtester.run_backtest(BACKTEST_START, BACKTEST_END)

        elif choice == '3':
            collector = DataCollector(db)
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