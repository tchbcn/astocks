import numpy as np
import pandas as pd
from datetime import timedelta
import akshare as ak
import logging, warnings
import a_ak_c

# clickhouse server
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
    """多因子微盘股回测引擎（完全对齐JQ策略）"""

    def __init__(self, db, initial_cash):
        self.db = db
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.holdings = {}
        self.trade_log = []
        self.daily_stats = []
        self.reason_to_sell=''
        # 缓存结构
        self.data_cache = {}
        self.stock_info_cache = {}
        self.trading_dates = []

        # ========== 核心策略参数 ==========
        self.STOCK_NUM = 15  # 持仓数量（JQ默认3只）
        self.INDEX_CODE = '399101.XSHE'  # 中小板综指
        self.up_price = 100
        # ========== 多因子参数 ==========
        self.momentum_enabled = False  # 是否启用动量因子
        self.momentum_period = 63  # 动量周期（3个月）
        self.momentum_weight = 0.3  # 动量因子权重
        self.market_cap_weight = 0.7  # 市值因子权重

        # ========== 风控参数 ==========
        self.run_stoploss = True  # 是否启用止损
        self.stoploss_strategy = 1  # 1=止损线 2=市场趋势 3=联合
        self.stoploss_limit = 0.88  # 止损线（-12%）
        self.stoploss_market = 0.94  # 市场趋势止损（-6%）

        # 放量风控
        self.HV_control = False  # 启用放量止损
        self.HV_duration = 120  # 放量周期
        self.HV_ratio = 0.9  # 放量阈值       F

        self.limit_days = 5  # 涨停后N天不再买入
        # 关仓期
        self.pass_april = True  # 1月、4月清仓

        # 涨停股票追踪

        self.not_buy_again = []  # 本周已买入列表（避免重复买入）

    def load_all_data_to_cache(self, start_date, end_date):
        """预加载全量数据到内存"""
        print("⚡ 正在预加载全量数据到内存...")

        # 1. 加载股票基础信息
        info_query = f"SELECT ticker, short_name, is_st FROM {self.db.db_path}.stock_list"
        info_res = self.db.client.query(info_query)
        for row in info_res.result_rows:
            self.stock_info_cache[row[0]] = {
                'short_name': row[1],
                'is_st': row[2]
            }

        # 2. 计算数据加载起始时间（多加载250天用于计算动量因子）
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

    def calculate_momentum_factor(self, candidates, date_str):
        """
        计算动量因子（对齐JQ的calculate_momentum_factor）
        返回：添加momentum_score列的DataFrame
        """
        if not self.momentum_enabled:
            return candidates

        momentum_scores = {}

        for cand in candidates:
            ticker = cand['ticker']
            df = self.data_cache.get(ticker)
            if df is None:
                momentum_scores[ticker] = 0
                continue

            df_slice = df.loc[:date_str]
            if len(df_slice) < self.momentum_period + 1:
                momentum_scores[ticker] = 0
                continue

            # 获取过去N日的价格数据
            recent_prices = df_slice['Close'].iloc[-(self.momentum_period + 1):]
            start_price = recent_prices.iloc[0]
            end_price = recent_prices.iloc[-1]

            # 计算动量（收益率）
            momentum = (end_price - start_price) / start_price

            # 计算波动率
            returns = recent_prices.pct_change().dropna()
            volatility = returns.std()

            # 动量得分：夏普比率思想（收益/波动）
            if volatility > 0:
                momentum_score = momentum / volatility
            else:
                momentum_score = momentum

            momentum_scores[ticker] = momentum_score

        # 转换为DataFrame并添加momentum_score列
        df = pd.DataFrame(candidates)
        df['momentum_score'] = df['ticker'].map(momentum_scores)

        return df

    def composite_ranking(self, df):
        """
        综合排序：市值因子 + 动量因子（对齐JQ的composite_ranking）
        """
        # 市值排序（小市值更好）
        df['market_cap_rank'] = df['market_cap'].rank(ascending=True)

        # 动量排序（高动量更好）
        df['momentum_rank'] = df['momentum_score'].rank(ascending=False)

        # 综合得分
        df['composite_score'] = (
            self.market_cap_weight * df['market_cap_rank'] +
            self.momentum_weight * df['momentum_rank']
        )

        # 按综合得分排序
        df = df.sort_values('composite_score', ascending=True)

        return df

    def get_stock_list(self, date_str):
        """
        选股模块（完全对齐JQ的get_stock_list）
        过滤链：中小板综指 → ST → 新股 → 停牌 → 涨跌停 → 多因子排序
        """
        # 1. 获取中小板综指成分股
        try:
            df_index = ak.index_stock_cons(symbol="399101")
            if '品种代码' in df_index.columns:
                codes = df_index['品种代码'].tolist()
            elif 'code' in df_index.columns:
                codes = df_index['code'].tolist()
            else:
                codes = []

            # 转换为标准格式
            standard_codes = []
            for code in codes:
                if code.startswith('6'):
                    standard_codes.append(f"{code}.SS")
                elif code.startswith(('0', '3')):
                    standard_codes.append(f"{code}.SZ")
        except:
            standard_codes = []

        if not standard_codes:
            return []

        # 2. 基础过滤
        candidates = []
        for ticker in standard_codes:
            if ticker not in self.data_cache:
                continue

            df = self.data_cache[ticker]
            df_slice = df.loc[:date_str]

            if df_slice.empty:
                continue

            # 过滤新股（上市不足375天）
            if len(df_slice) < 105:
                continue

            # 过滤ST
            s_info = self.stock_info_cache.get(ticker, {})
            if s_info.get('is_st', 1) == 1:
                continue
            if 'ST' in s_info.get('short_name', '') or '*' in s_info.get('short_name', ''):
                continue

            latest_row = df_slice.iloc[-1]



            # 过滤涨跌停
            prev_close = df_slice['Close'].iloc[-2] if len(df_slice) >= 2 else latest_row['Open']
            is_star = ticker.startswith('688')
            is_chi = ticker.startswith('300')
            limit_threshold = 1.20 if (is_star or is_chi) else 1.10

            high_limit = prev_close * limit_threshold
            low_limit = prev_close / limit_threshold

            # 涨停或跌停不选
            if latest_row['Close'] >= high_limit * 0.9995:
                continue
            if latest_row['Close'] <= low_limit * 1.0005:
                continue

            candidates.append({
                'ticker': ticker,
                'market_cap': latest_row['Market Cap'],
                'close': latest_row['Close'],
                'high': latest_row['High'],
                'prev_close': prev_close,
                'short_name': s_info.get('short_name', ticker)
            })

        if not candidates:
            return []

        # 3. 多因子排序
        if self.momentum_enabled:
            df_candidates = self.calculate_momentum_factor(candidates, date_str)
            df_candidates = self.composite_ranking(df_candidates)
        else:
            df_candidates = pd.DataFrame(candidates)
            df_candidates = df_candidates.sort_values('market_cap', ascending=True)

        # 4. 取前100名，再取前2*STOCK_NUM用于轮动
        stock_list = df_candidates['ticker'].tolist()[:100]
        final_list = stock_list[:2 * self.STOCK_NUM]

        return final_list

    def check_high_volume_sell(self, ticker, date_str):
        """放量止损检查（对齐JQ的check_high_volume）"""
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
        return current_volume > self.HV_ratio * max_volume

    def check_stoploss(self, ticker, date_str):
        """
        止损检查（对齐JQ的sell_stocks）
        策略1：个股止损线（-12%止损，+100%止盈）
        策略2：市场趋势止损（大盘跌幅>6%）
        策略3：联合策略
        """
        if not self.run_stoploss:
            return False, ""

        holding = self.holdings.get(ticker)
        if not holding:
            return False, ""

        df = self.data_cache.get(ticker)
        if df is None:
            return False, ""

        df_slice = df.loc[:date_str]
        if df_slice.empty:
            return False, ""

        curr_price = df_slice.iloc[-1]['Open']
        avg_cost = holding['avg_cost']

        # 策略1：个股止损
        if self.stoploss_strategy in [1, 3]:
            # 止盈：收益达100%
            if curr_price >= avg_cost * 2:
                return True, "收益100%止盈"

            # 止损：亏损超12%
            if curr_price < avg_cost * self.stoploss_limit:
                return True, f"止损({self.stoploss_limit})"

        # 策略2/3：市场趋势止损
        if self.stoploss_strategy in [2, 3]:
            # 获取大盘平均跌幅（使用前一交易日数据）
            index_stocks = self.get_stock_list(date_str)[:50]  # 取样本股票
            down_ratios = []

            for s in index_stocks:
                s_df = self.data_cache.get(s)
                if s_df is None:
                    continue
                s_slice = s_df.loc[:date_str]
                if len(s_slice) < 2:
                    continue

                prev_row = s_slice.iloc[-2]
                if prev_row['Open'] > 0:
                    down_ratio = prev_row['Close'] / prev_row['Open']
                    down_ratios.append(down_ratio)

            if down_ratios:
                avg_down = np.mean(down_ratios)
                if avg_down <= self.stoploss_market:
                    return True, f"大盘止损({avg_down:.2%})"

        return False, ""

    def buy_stock(self, ticker, price, high_price, qty, date, prev_close):
        """执行买入"""
        is_star = ticker.startswith('688')
        is_chi = ticker.startswith('300')
        limit_threshold = 1.20 if (is_star or is_chi) else 1.10

        stock_info = self.stock_info_cache.get(ticker, {})
        short_name = stock_info.get('short_name', ticker)

        # 涨停买不入
        limit_price = prev_close * limit_threshold
        if price >= limit_price * 0.9995:
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
            'buy_date': pd.to_datetime(date)
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
        holding = self.holdings.get(ticker)
        if not holding:
            return 0, 0

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

    def check_remain_amount(self, date_str):
        """余额补仓（对齐JQ的check_remain_amount）"""

        # 只有涨停打开卖出后才补仓
        if self.reason_to_sell != 'limitup':
            return

        # 检查是否有空位
        if len(self.holdings) >= self.STOCK_NUM:
            self.reason_to_sell = ''
            return


        # 过滤已持仓和本周已买入的股票
        candidates = [
            t for t in self.weekly_target_list
            if t not in self.holdings and t not in self.not_buy_again
        ]

        if not candidates or self.cash <= 0:
            self.reason_to_sell = ''
            return

        # 计算每只股票的买入金额
        slots = self.STOCK_NUM - len(self.holdings)
        per_stock_cash = self.cash / slots

        # 尝试买入
        for ticker in candidates[:slots]:
            df = self.data_cache.get(ticker)
            if df is None:
                continue

            df_slice = df.loc[:date_str]
            if df_slice.empty:
                continue

            curr_row = df_slice.iloc[-1]
            prev_close = df_slice['Close'].iloc[-2] if len(df_slice) >= 2 else curr_row['Open']

            is_star = ticker.startswith('688')
            min_qty = 200 if is_star else 100
            qty = max(min_qty, (per_stock_cash / curr_row['Close']) // 100 * 100)

            res = self.buy_stock(ticker, curr_row['Close'], curr_row['High'],
                                 qty, date_str, prev_close)

            if res:
                self.not_buy_again.append(ticker)
                print(f"✅ 补仓买入: {ticker}")

            if len(self.holdings) >= self.STOCK_NUM:
                break

        self.reason_to_sell = ''
    def run_backtest(self, start_date, end_date):
        """运行回测（完全对齐JQ策略）"""
        print(f"\n📊 开始回测 - 多因子微盘股策略")
        print(f"💰 初始资金: ¥{self.initial_cash:,.2f}")
        print(f"📋 持仓数量: {self.STOCK_NUM} 只")
        print(f"🎯 因子配置: 市值{self.market_cap_weight} + 动量{self.momentum_weight}")
        print(f"🛡️ 风控机制: 止损策略{self.stoploss_strategy} + 放量检查 + 关仓期")
        print("=" * 100)

        # 预加载数据
        self.load_all_data_to_cache(start_date, end_date)
        self.trading_dates = self.get_trading_dates(start_date, end_date)

        header = f"{'日期':<12} | {'资产':<12} | {'日盈亏':<10} | {'仓位':<4} | {'操作'}"
        print(header)
        print("-" * 100)

        prev_total_value = self.initial_cash
        last_rebalance_week = None
        pre_date_str=""
        for i, current_date in enumerate(self.trading_dates):
            date_str = current_date.strftime('%Y-%m-%d')
            current_month = current_date.month
            current_day = current_date.day
            current_week = current_date.isocalendar()[1]

            # ========================================
            # 1️⃣ 开盘前准备（9:05）
            # ========================================

            # 判断关仓期（1月1-30日，4月1-30日）
            is_no_trade_period = self.pass_april and (
                (current_month == 1 and 1 <= current_day <= 30) or
                (current_month == 4 and 1 <= current_day <= 30)
            )
            daily_bought = []  # 存名称，用于打印 (e.g., "同洲电子")
            daily_bought_tickers = []  # 存代码，用于 T+1 检查 (e.g., "002052.SZ")
            daily_sold = []  # 存名称，用于打印
            daily_sold_tickers = []  # 存代码，用于防止当日回补

            # ========================================
            # 2️⃣ 关仓期处理（14:50）
            # ========================================
            if is_no_trade_period:
                if self.holdings:
                    for ticker in list(self.holdings.keys()):
                        df = self.data_cache.get(ticker)
                        if df is None:
                            continue
                        df_slice = df.loc[:date_str]
                        if df_slice.empty:
                            continue
                        curr_row = df_slice.iloc[-1]
                        if curr_row['Volume'] > 0:
                            self.sell_stock(ticker, curr_row['Close'], date_str, f"{current_month}月清仓")
                            print(f'{current_month}月清仓')

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
                    f"{len(self.holdings):<4} | 🚫关仓期"
                )

                prev_total_value = total_value
                continue

            # ========================================
            # 6️⃣ 每周二开盘9:30,用昨天的收盘价模拟选股 - 调仓（weekly_adjustment）
            # ========================================
            is_tuesday = current_date.weekday() == 1
            if is_tuesday and current_week != last_rebalance_week:
                last_rebalance_week = current_week
                self.not_buy_again = []  # 重置本周买入列表

                # 获取选股列表
                target_list = self.get_stock_list(pre_date_str)
                print(target_list)
                if not target_list:
                    target_list = []
                self.weekly_target_list=target_list
                target_tickers = target_list[:self.STOCK_NUM]

                # 卖出不在目标池且不在涨停列表的股票
                for ticker in list(self.holdings.keys()):
                    if ticker not in target_tickers :
                        if ticker in daily_sold_tickers:
                            continue
                        df = self.data_cache.get(ticker)
                        if df is None:
                            continue
                        df_slice = df.loc[:date_str]
                        if df_slice.empty:
                            continue
                        pre_row = df_slice.iloc[-2]
                        curr_row = df_slice.iloc[-1]

                        # 计算涨停阈值
                        is_star_or_chi = ticker.startswith(('688', '300'))
                        limit_ratio = 1.20 if is_star_or_chi else 1.10

                        # 今日开盘没有涨停才允许卖
                        if curr_row['Open']< pre_row['Close'] * limit_ratio * 0.9995:
                            stock_info = self.stock_info_cache.get(ticker, {})
                            short_name = stock_info.get('short_name', ticker)
                            self.sell_stock(ticker, curr_row['Open'], date_str, "调仓卖出")
                            print(f'卖出{short_name}-{reason}')
                            daily_sold.append(short_name)
                            daily_sold_tickers.append(ticker)
                            self.not_buy_again.append(ticker)

                # 买入新股票
                slots_available = self.STOCK_NUM - len(self.holdings)
                if slots_available > 0 and self.cash > 0:
                    per_stock_cash = self.cash / slots_available

                    for ticker in target_tickers:
                        if ticker in self.holdings:
                            continue

                        df = self.data_cache.get(ticker)
                        if df is None:
                            continue
                        df_slice = df.loc[:date_str]
                        if df_slice.empty:
                            continue
                        # 条件3：近期N天无涨停 (不含今日)
                        # recent_pct = df['Close'].iloc[-self.limit_days:].pct_change()
                        # if (recent_pct > 0.098).any():
                        #     continue
                        curr_row = df_slice.iloc[-1]
                        prev_close = df_slice['Close'].iloc[-2]
                        is_star = ticker.startswith('688')
                        min_qty = 200 if is_star else 100

                        qty = max(min_qty, (per_stock_cash / curr_row['Open']) // 100 * 100)
                        res = self.buy_stock(ticker, curr_row['Open'], curr_row['High'], qty, date_str, prev_close)

                        if res:
                            stock_info = self.stock_info_cache.get(ticker, {})
                            short_name = stock_info.get('short_name', ticker)
                            daily_bought.append(short_name)
                            daily_bought_tickers.append(ticker)
                            self.not_buy_again.append(ticker)

                        if len(self.holdings) >= self.STOCK_NUM:
                            break

            # ========================================
            # 3️⃣ 9:30 - 开盘卖出检查-止损检查（sell_stocks），以开盘价模拟
            # ========================================
            for ticker in list(self.holdings.keys()):
                if ticker in daily_bought_tickers:
                    continue
                should_sell, reason = self.check_stoploss(ticker, date_str)
                if should_sell:
                    df = self.data_cache.get(ticker)
                    if df is None:
                        continue
                    df_slice = df.loc[:date_str]
                    if df_slice.empty:
                        continue
                    curr_row = df_slice.iloc[-1]
                    stock_info = self.stock_info_cache.get(ticker, {})
                    short_name = stock_info.get('short_name', ticker)
                    self.sell_stock(ticker, curr_row['Open'], date_str, reason)
                    print(f'卖出{short_name}-{reason}')
                    daily_sold.append(short_name)
                    daily_sold_tickers.append(ticker)
                    self.not_buy_again.append(ticker)


            # 14：55 收盘卖出检查——检查昨天涨停，但是今天没有涨停就卖出
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
                        print(f'卖出{short_name}-{reason}')
                        daily_sold.append(short_name)
                        daily_sold_tickers.append(ticker)
                        self.not_buy_again.append(ticker)
                        self.reason_to_sell = 'limitup'  # 触发次日补仓

            # ========================================
            # 5️⃣ 14:55 - 卖出检查-放量检查（trade_afternoon）
            # ========================================
            if self.HV_control:
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
                            print(f'卖出{short_name}-{reason}')
                            daily_sold.append(short_name)
                            daily_sold_tickers.append(ticker)
                            self.not_buy_again.append(ticker)
                            self.reason_to_sell = 'limitup'  # 触发次日补仓
                        # 计算涨停阈值
                        is_star_or_chi = ticker.startswith(('688', '300'))
                        limit_ratio = 1.20 if is_star_or_chi else 1.10

                        # 只有没有涨停才允许卖
                        if curr_row['Close'] < pre_row['Close'] * limit_ratio * 0.9995:
                            stock_info = self.stock_info_cache.get(ticker, {})
                            short_name = stock_info.get('short_name', ticker)
                            self.sell_stock(ticker, curr_row['Close'], date_str, "天量")
                            print(f'卖出{short_name}-{reason}')
                            daily_sold.append(short_name)
                            daily_sold_tickers.append(ticker)
                            self.not_buy_again.append(ticker)
                            self.reason_to_sell = 'limitup'  # 触发收盘补仓
            # ⚠️ 补仓逻辑
            # self.check_remain_amount(date_str)

            pre_date_str=date_str
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
                action_str = f"买[{','.join(daily_bought[:])}]"
            if daily_sold:
                if action_str:
                    action_str += " "
                action_str += f"卖[{','.join(daily_sold[:])}]"
            if not action_str:
                action_str = "-"

            print(
                f"{date_str:<12} | {total_value:<12.0f} | {daily_pnl:<+10.0f} | "
                f"{len(self.holdings):<4} | | {action_str}"
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
            sharpe_ratio = (df['excess_return'].mean() / df['daily_return'].std()) * np.sqrt(252)
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