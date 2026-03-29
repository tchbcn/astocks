import yfinance as yf
import pandas as pd
import numpy as np
import time
import sqlite3,logging,warnings
from datetime import datetime
DB_PATH = 'stock_data.db'
INITIAL_CASH = 100000
DATA_START_DATE = '2024-05-01'  # 数据起始日期
BACKTEST_START = '2025-01-01'  # 回测起始日期
BACKTEST_END = '2025-12-11'  # 回测结束日期
class StockDatabase:
    """股票数据库管理类"""

    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.init_database()

    def init_database(self):
        """初始化数据库表结构"""
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()

        # 创建股票列表表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_list (
                ticker TEXT PRIMARY KEY,
                name TEXT,
                market TEXT,
                market_cap REAL,
                last_update TEXT
            )
        ''')

        # 创建日K线数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_kline (
                ticker TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                market_cap REAL,
                PRIMARY KEY (ticker, date)
            )
        ''')

        # 创建分钟K线数据表（用于实盘）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS minute_kline (
                ticker TEXT,
                datetime TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (ticker, datetime)
            )
        ''')

        self.conn.commit()
        print("✅ 数据库初始化完成")

    def get_all_a_stocks(self):
        """获取所有A股代码（沪深主板、创业板、科创板）"""
        print("🔍 正在获取A股列表...")
        stocks = []

        # 沪市主板 600000-605000
        stocks += [f'{i:06d}.SS' for i in range(600000, 605000)]
        # 深市主板 000001-002000
        stocks += [f'{i:06d}.SZ' for i in range(1, 2000)]
        # 创业板 300000-301000
        stocks += [f'{i:06d}.SZ' for i in range(300000, 301000)]
        # 科创板 688000-689000
        stocks += [f'{i:06d}.SS' for i in range(688000, 689000)]

        print(f"📊 共生成 {len(stocks)} 个股票代码")
        return stocks

    def save_stock_list(self, stocks_info):
        """保存股票列表到数据库"""
        cursor = self.conn.cursor()
        for stock in stocks_info:
            cursor.execute('''
                            INSERT OR REPLACE INTO stock_list (ticker, name, market, market_cap, last_update)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                stock['ticker'],
                stock.get('name', ''),
                stock.get('market', ''),
                stock.get('market_cap', 0),  # 2. 从字典获取市值，默认为 0
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
        self.conn.commit()

    def save_daily_data(self, ticker, df):
        """保存日K线数据"""
        if df.empty:
            return

        cursor = self.conn.cursor()
        for idx, row in df.iterrows():
            market_cap = row.get('Market Cap', 0)  # 假设 df 中有 'Market Cap' 字段
            cursor.execute('''
                            INSERT OR REPLACE INTO daily_kline 
                            (ticker, date, open, high, low, close, volume, market_cap)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (ticker,
                              idx.strftime('%Y-%m-%d'),
                              row['Open'],
                              row['High'],
                              row['Low'],
                              row['Close'],
                              row['Volume'],
                              market_cap))  # 增加 market_cap
        self.conn.commit()

    def get_daily_data(self, ticker, start_date=None, end_date=None):
        """从数据库读取日K线数据"""
        query = f"SELECT * FROM daily_kline WHERE ticker = ?"
        params = [ticker]

        # ... (参数和排序逻辑不变)

        df = pd.read_sql_query(query, self.conn, params=params)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            # 【核心修改点】: 增加 Market Cap 列名
            df.columns = ['ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap']
        return df

    def get_all_tickers_with_data(self):
        """获取有数据的所有股票代码"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM daily_kline")
        return [row[0] for row in cursor.fetchall()]

    def close(self):
        if self.conn:
            self.conn.close()
class DataCollector:
    """数据采集类"""

    def __init__(self, db):
        self.db = db
        # 错误分类统计
        self.error_stats = {
            'delisted': [],  # 退市股票
            'not_found': [],  # 未找到
            'no_data': [],  # 无数据
            'network': [],  # 网络错误
            'other': []  # 其他错误
        }

    def classify_error(self, ticker, error_msg):
        """分类错误信息"""
        error_str = str(error_msg).lower()

        if 'delisted' in error_str or 'no price data found' in error_str:
            self.error_stats['delisted'].append(ticker)
            return 'delisted'
        elif '404' in error_str or 'not found' in error_str:
            self.error_stats['not_found'].append(ticker)
            return 'not_found'
        elif 'no timezone' in error_str or 'no data' in error_str:
            self.error_stats['no_data'].append(ticker)
            return 'no_data'
        elif 'timeout' in error_str or 'connection' in error_str:
            self.error_stats['network'].append(ticker)
            return 'network'
        else:
            self.error_stats['other'].append(ticker)
            return 'other'

    def download_all_stocks_data(self, start_date, batch_size=100, retry_network_errors=True):
        """
        自动断点续传下载所有A股历史数据
        """
        print(f"\n🚀 开始全量下载任务（智能断点续传）")
        print("=" * 60)

        # 1. 获取目标股票全表
        all_stocks_list = self.db.get_all_a_stocks()

        # 2. 获取数据库中已存在的股票
        print("🔍 正在检查数据库已存数据...")
        exist_tickers = set(self.db.get_all_tickers_with_data())

        # 3. 过滤出真正需要下载的股票 (全集 - 已存集)
        pending_stocks = [t for t in all_stocks_list if t not in exist_tickers]

        total_stocks = len(all_stocks_list)
        completed_count = len(exist_tickers)
        pending_count = len(pending_stocks)

        print(f"📊 统计概览:")
        print(f"   总股票数: {total_stocks}")
        print(f"   ✅ 已下载: {completed_count}")
        print(f"   ⏳ 待下载: {pending_count}")

        if pending_count == 0:
            print("\n🎉 所有股票数据已存在，无需下载！")
            return

        # 计算批次（基于剩余的 pending_stocks）
        total_batches = (pending_count + batch_size - 1) // batch_size
        print(f"   👉 本次将分 {total_batches} 批次下载剩余股票")
        print("=" * 60)

        # 禁用 yfinance 噪音
        logging.getLogger('yfinance').setLevel(logging.CRITICAL)
        warnings.filterwarnings('ignore')

        # 4. 遍历待下载列表（不再使用 start_batch，而是直接处理 pending_stocks）
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, pending_count)
            batch_stocks = pending_stocks[start_idx:end_idx]

            print(f"\n{'=' * 60}")
            print(f"📦 处理批次 {batch_num + 1}/{total_batches} (进度: {start_idx + completed_count}/{total_stocks})")
            print(f"{'=' * 60}")

            success_count = 0
            stocks_info = []

            # 进度条显示
            for i, ticker in enumerate(batch_stocks, 1):
                try:
                    # 下载数据
                    stock = yf.Ticker(ticker)
                    hist = stock.history(start=start_date, end=datetime.now().strftime('%Y-%m-%d'))

                    # 检查数据有效性
                    if not hist.empty and len(hist) > 20:
                        self.db.save_daily_data(ticker, hist)

                        # 获取股票名称（用于存入 stock_list 表）
                        name = ''
                        market_cap = 0  # 默认为 0
                        try:
                            info = stock.info
                            name = info.get('longName', info.get('shortName', ''))
                            market_cap = info.get('marketCap', 0)
                        except:
                            pass

                        stocks_info.append({
                            'ticker': ticker,
                            'name': name,
                            'market': 'SH' if ticker.endswith('.SS') else 'SZ',
                            'market_cap': market_cap,  # 新增字段
                        })
                        success_count += 1
                    else:
                        self.error_stats['no_data'].append(ticker)

                    # 实时进度打印
                    print(f"\rProcess: [{i}/{len(batch_stocks)}] | {ticker} | 成功: {success_count}", end='',
                          flush=True)

                    time.sleep(0.1)  # 稍微防封

                except KeyboardInterrupt:
                    print(f"\n\n⚠️  用户中断！已保存当前批次数据。")
                    if stocks_info:
                        self.db.save_stock_list(stocks_info)
                    return

                except Exception as e:
                    self.classify_error(ticker, str(e))

            # 批次结束保存元数据
            if stocks_info:
                self.db.save_stock_list(stocks_info)

            print(f"\n✅ 本批完成: 成功入库 {success_count} 只")

            # 批次间休息
            if batch_num < total_batches - 1:
                time.sleep(1)

        print(f"\n\n🎉 所有任务处理完成！")
        self.print_error_summary()

        # 处理网络错误重试
        if retry_network_errors and self.error_stats['network']:
            retry = input(f"\n发现 {len(self.error_stats['network'])} 个网络错误，是否重试? (y/n): ")
            if retry.lower() == 'y':
                self.retry_failed_stocks(start_date, self.error_stats['network'])

    def retry_failed_stocks(self, start_date, failed_list):
        """重试失败的股票"""
        print(f"\n🔄 重试 {len(failed_list)} 只股票...")
        success_count = 0

        for i, ticker in enumerate(failed_list, 1):
            try:
                print(f"\r进度: [{i}/{len(failed_list)}] 成功: {success_count}", end='', flush=True)

                stock = yf.Ticker(ticker)
                hist = stock.history(start=start_date, end=datetime.now().strftime('%Y-%m-%d'))  # 移除progress

                if not hist.empty and len(hist) > 20:
                    self.db.save_daily_data(ticker, hist)
                    success_count += 1

                time.sleep(0.5)
            except:
                pass

        print(f"\n✅ 重试完成: 成功 {success_count}/{len(failed_list)}")

    def retry_failed_stocks(self, start_date, failed_list):
        """重试失败的股票"""
        print(f"\n🔄 重试 {len(failed_list)} 只股票...")
        success_count = 0

        for i, ticker in enumerate(failed_list, 1):
            try:
                print(f"\r进度: [{i}/{len(failed_list)}] 成功: {success_count}", end='', flush=True)

                stock = yf.Ticker(ticker)
                hist = stock.history(start=start_date,
                                     end=datetime.now().strftime('%Y-%m-%d'),
                                     progress=False)

                if not hist.empty and len(hist) > 20:
                    self.db.save_daily_data(ticker, hist)
                    success_count += 1

                time.sleep(0.5)
            except:
                pass

        print(f"\n✅ 重试完成: 成功 {success_count}/{len(failed_list)}")

    def print_error_summary(self):
        """打印错误汇总"""
        print(f"\n{'=' * 60}")
        print("📋 错误统计汇总")
        print(f"{'=' * 60}")

        total_errors = sum(len(v) for v in self.error_stats.values())
        if total_errors == 0:
            print("✅ 无错误")
            return

        if self.error_stats['delisted']:
            print(f"❌ 退市/无数据: {len(self.error_stats['delisted'])} 只")

        if self.error_stats['not_found']:
            print(f"❌ 股票不存在: {len(self.error_stats['not_found'])} 只")

        if self.error_stats['no_data']:
            print(f"⚠️  数据不足: {len(self.error_stats['no_data'])} 只")

        if self.error_stats['network']:
            print(f"🌐 网络错误: {len(self.error_stats['network'])} 只")

        if self.error_stats['other']:
            print(f"❓ 其他错误: {len(self.error_stats['other'])} 只")

        print(f"\n总计错误: {total_errors} 只")

        # 保存错误日志到文件
        log_file = f"download_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("=" * 60 + "\n")
            f.write("下载错误日志\n")
            f.write("=" * 60 + "\n\n")

            for error_type, tickers in self.error_stats.items():
                if tickers:
                    f.write(f"\n{error_type.upper()} ({len(tickers)} 只):\n")
                    f.write(", ".join(tickers) + "\n")

        print(f"📝 详细错误日志已保存到: {log_file}")
class StrategyEngine:
    """策略引擎 (对齐 JoinQuant 逻辑)"""

    @staticmethod
    def calc_td_sequence(closes):
        """计算TD九转 (保持不变)"""
        n = len(closes)
        td = np.zeros(n)
        if n < 5: return td
        buy_cnt, sell_cnt = 0, 0
        for i in range(4, n):
            if closes[i] < closes[i - 4]:
                buy_cnt, sell_cnt = buy_cnt + 1, 0
                td[i] = -buy_cnt
            elif closes[i] > closes[i - 4]:
                sell_cnt, buy_cnt = sell_cnt + 1, 0
                td[i] = sell_cnt
            else:
                buy_cnt, sell_cnt = 0, 0
        return td

    @staticmethod
    def check_shadow_line(row):
        """阴线长上影线检查 (保持不变)"""
        if row['Close'] < row['Open']:
            upper_shadow = row['High'] - row['Open']
            entity = row['Open'] - row['Close']
            # JQ逻辑: 上影线 > 实体1.5倍 OR 上影线幅度 > 2%
            if (upper_shadow > entity * 1.5) or ((row['High'] - row['Open']) / row['Open'] > 0.02):
                return True
        return False

    @staticmethod
    def check_buy_signal(data_daily):
        """买入信号检查"""
        # JQ: 排除ST, 上市>120天 (假设外部已处理或数据长度已隐含)
        if len(data_daily) < 105:
            return False, "数据不足"

        prev_data = data_daily.iloc[:-1]
        current_row = data_daily.iloc[-1]

        # 1. 100天创新高 (JQ: past_high = close_df.max())
        # 注意：JQ是用过去100天(不含今日)的Close最大值
        past_100_high = prev_data['Close'].iloc[-100:].max()
        if current_row['Close'] <= past_100_high:
            return False, "未创新高"

        # 2. 检查阴线长上影 (JQ: 条件6)
        if StrategyEngine.check_shadow_line(current_row):
            return False, "长上影线"

        # 3. TD九转检查 (JQ: 条件4 & 5)
        td_seq = StrategyEngine.calc_td_sequence(data_daily['Close'].values)
        curr_td = td_seq[-1]
        # JQ: abs(curr_td) == 8 or abs(curr_td) == 9 continue
        if abs(curr_td) >= 8:
            return False, "TD接近9转"

        # JQ: sum(abs(recent_td) == 9) >= 2 continue
        if np.sum(np.abs(td_seq[-30:-1]) == 9) >= 2:
            return False, "近期多次9转"

        return True, "买入信号"

    @staticmethod
    def check_sell_signal(holding_info, current_row, prev_close):
        """
        卖出信号检查 (增强版)
        :param holding_info: 持仓字典
        :param current_row: 当日K线数据 (Series)
        :param prev_close: 昨日收盘价 (用于计算涨停)
        """
        current_price = current_row['Close']
        current_date = pd.to_datetime(current_row.name)  # 假设索引是日期

        # 1. 止损：亏损6% (JQ: current_price < cost * 0.94)
        if current_price < holding_info['avg_cost'] * 0.94:
            return True, "止损(-6%)"

        # 2. 涨停止盈 (JQ: current_price >= limit_up_price * 0.9)
        # 估算涨停价 (主板10%, 科创/创业20%)
        # 简单处理：统按10%估算，或根据代码判断。这里为了通用性按10%处理
        limit_up = round(prev_close * 1.10, 2)
        if current_price >= limit_up * 0.9:
            # print(f"prev_close:{prev_close},")
            return True, "第二天跌幅没有大于1%止盈:"+str(current_price)

        # 3. 时间离场 (JQ: len(days) >= 6)
        # 逻辑：持仓超过6天，且期间最高价(period_high) 没超过 买入日最高价(buy_high)
        # 注意：JQ逻辑是持有期内如果表现不好(没突破买入当天高点)就走
        hold_days = (current_date - holding_info['buy_date']).days

        # 更新持仓期间的最高价 (T+1 到 Now)
        if current_row['High'] > holding_info['period_high']:
            holding_info['period_high'] = current_row['High']

        if hold_days >= 6:
            # JQ: if period_high <= info['buy_high']: safe_sell
            # 意味着后续几天的最高价甚至没打过买入那天的最高价
            if holding_info['period_high'] <= holding_info['buy_initial_high']:
                return True, "时间止损(滞涨)"

        return False, ""
class Backtester:
    """回测引擎（增强版：包含详细日志与量化指标）"""

    def __init__(self, db, initial_cash):
        self.db = db
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.holdings = {}
        self.trade_log = []
        self.daily_stats = []  # 记录每日详细资产数据
        self.benchmark_data = []  # 可选：用于记录基准收益（这里暂只记录自身）

    def run_backtest(self, start_date, end_date):
        """运行回测"""
        print(f"\n📊 开始回测 ({start_date} 至 {end_date})")
        print(f"💰 初始资金: ¥{self.initial_cash:,.2f}")
        print("=" * 100)
        # 打印表头
        header = f"{'日期':<12} | {'资产总值':<12} | {'日盈亏':<10} | {'持仓':<4} | {'选股/买入':<20} | {'卖出/止盈损'}"
        print(header)
        print("-" * 100)

        tickers = self.db.get_all_tickers_with_data()
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        # 预加载所有数据到内存 (优化速度，可选)或保持循环读取
        # 为了演示逻辑，这里保持结构，但修改内部循环逻辑
        prev_total_value = self.initial_cash
        for current_date in date_range:
            date_str = current_date.strftime('%Y-%m-%d')
            # 当日临时数据
            daily_selected = []  # 触发买入信号的股票


            # --- 阶段1：卖出检查 (优先回笼资金) ---
            daily_sold = []
            # 使用 list(keys) 避免迭代时修改字典报错
            for ticker in list(self.holdings.keys()):
                try:
                    # 获取当日和昨日数据
                    df_latest = self.db.get_daily_data(ticker, end_date=date_str)
                    if df_latest.empty or df_latest.index[-1].strftime('%Y-%m-%d') != date_str:
                        continue  # 停牌或无数据

                    curr_row = df_latest.iloc[-1]
                    # 获取昨日收盘价 (用于计算涨停)
                    prev_close = df_latest['Close'].iloc[-2] if len(df_latest) >= 2 else curr_row['Open']
                    should_sell, reason = StrategyEngine.check_sell_signal(
                        self.holdings[ticker], curr_row, prev_close
                    )

                    if should_sell:
                        self.sell_stock(ticker, curr_row['Close'], date_str, reason)
                        daily_sold.append(f"{ticker}|{reason}")
                except Exception:
                    pass

            # --- 阶段2：全市场选股 (生成候选池) ---
            candidates = []

            for ticker in tickers:
                # 如果已经持仓，跳过选股
                if ticker in self.holdings: continue

                try:
                    data = self.db.get_daily_data(ticker, end_date=date_str)
                    # 确保最后一天是回测当天
                    if data.empty or data.index[-1].strftime('%Y-%m-%d') != date_str: continue

                    should_buy, _ = StrategyEngine.check_buy_signal(data)
                    if should_buy:
                        daily_selected.append(ticker)
                        candidates.append({
                            'ticker': ticker,
                            'close': data['Close'].iloc[-1],
                            'high': data['High'].iloc[-1],  # 记录当日最高，用于初始buy_high
                            'market_cap': data['Market Cap'].iloc[-1]
                        })
                except:
                    pass

            # --- 阶段3：排序与资金分配 (核心修改) ---
            # JQ逻辑：按市值从小到大排序，取前10。
            # 本地无市值数据，暂时模拟：随机打乱或不做处理（即按代码顺序），但限制最大持仓数
            # 如果你有 volumn 和 close，可以用成交额粗略模拟活跃度，或者直接取前10

            # 假设策略最大持仓 10 只
            TARGET_NUM = 10
            current_positions = len(self.holdings)
            slots_available = TARGET_NUM - current_positions

            daily_bought = []

            if slots_available > 0 and candidates:
                # 1. 【核心修改】对候选股按市值从小到大排序
                # 假设 candidates 列表中的每个字典都包含 'market_cap' 字段
                try:
                    candidates.sort(key=lambda x: x.get('market_cap', float('inf')))
                except AttributeError as e:
                    # 如果 candidates 不是字典列表，或者 'market_cap' 字段缺失，则跳过排序
                    print(f"⚠️ 排序失败，请检查 candidates 结构是否包含 'market_cap' 键: {e}")
                    # 如果无法排序，仍按原有顺序切片
                    pass
                buy_targets = candidates[:slots_available]

                # 资金分配：剩余资金 / 还要买几只 (或者固定仓位)
                # JQ逻辑：value = cash / len(buylist)
                if len(buy_targets) > 0:
                    per_stock_cash = self.cash / len(buy_targets)

                    for target in buy_targets:
                        # 资金门槛 (JQ: cash < 2000 return)
                        if per_stock_cash < 2000: break

                        qty = int(per_stock_cash / target['close'] / 100) * 100

                        # 最小手数检查 (JQ: 科创200，其他100)
                        min_qty = 200 if target['ticker'].startswith('688') else 100

                        if qty >= min_qty:
                            self.buy_stock(target['ticker'], target['close'], target['high'], qty, date_str)
                            daily_bought.append(target['ticker'])

            # 2. 每日结算
            current_holdings_value = sum(
                h['qty'] * self.get_latest_price(ticker, date_str)
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

            # 3. 打印每日日志 (仅在有交易或大幅波动时，或者每天都打)
            # 格式化输出
            selected_str = f"{len(daily_selected)}只"
            if daily_bought:
                bought_str = ",".join(daily_bought)
                action_str = f"选{len(daily_selected)}/买[{bought_str}]"
            else:
                action_str = f"选{len(daily_selected)}/无买入"

            sold_str = ",".join(daily_sold) if daily_sold else "-"

            # 仅在交易日显示（排除无数据变动的日子，或者如果是A股只显示周一到周五）
            if current_date.weekday() < 5:
                print(
                    f"{date_str:<12} | {total_value:<12.0f} | {daily_pnl:<+10.0f} | {len(self.holdings):<4} | {action_str:<20} | {sold_str}")

            prev_total_value = total_value

        self.print_performance_metrics()

    def get_latest_price(self, ticker, date_str):
        """获取指定日期的最新价格 (辅助计算市值)"""
        try:
            # 这里为了性能，实际应该缓存当天的所有价格
            data = self.db.get_daily_data(ticker, end_date=date_str)
            if not data.empty:
                return data['Close'].iloc[-1]
        except:
            pass
        # 如果获取不到当天的（比如停牌），取持仓成本价或上一次价格估算
        if ticker in self.holdings:
            return self.holdings[ticker]['avg_cost']
        return 0

    def buy_stock(self, ticker, price, high_price, qty, date):
        """执行买入 (更新记录字段)"""
        cost = price * qty * (1 + 0.0003)  # 简易滑点
        self.cash -= cost

        self.holdings[ticker] = {
            'qty': qty,
            'avg_cost': price,
            'buy_date': pd.to_datetime(date),
            'buy_initial_high': high_price,  # 记录买入当天的最高价 (JQ: info['buy_high'])
            'period_high': high_price  # 记录持仓期间的最高价 (初始为买入日最高)
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
        # 卖出印花税(0.1%) + 佣金(0.03%)
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
        return profit, profit_rate, real_revenue

    def print_performance_metrics(self):
        """计算并打印详细量化指标"""
        if not self.daily_stats:
            print("❌ 无回测数据")
            return

        df = pd.DataFrame(self.daily_stats)
        df.set_index('date', inplace=True)

        # 1. 基础收益
        final_value = df['total_value'].iloc[-1]
        total_return = (final_value - self.initial_cash) / self.initial_cash * 100

        # 2. 年化收益 (假设一年252个交易日)
        days = len(df)
        annualized_return = ((1 + total_return / 100) ** (252 / days) - 1) * 100 if days > 0 else 0

        # 3. 夏普比率 (Sharpe Ratio)
        # 假设无风险利率为 3%
        risk_free_rate = 0.03
        daily_rf = risk_free_rate / 252
        df['excess_return'] = df['daily_return'] - daily_rf
        sharpe_ratio = 0
        if df['daily_return'].std() != 0:
            sharpe_ratio = (df['excess_return'].mean() / df['daily_return'].std()) * np.sqrt(252)

        # 4. 最大回撤 (Max Drawdown)
        df['cumulative_max'] = df['total_value'].cummax()
        df['drawdown'] = (df['total_value'] - df['cumulative_max']) / df['cumulative_max']
        max_drawdown = df['drawdown'].min() * 100

        # 5. 胜率分析
        sell_trades = [t for t in self.trade_log if t['action'] == 'SELL']
        win_trades = [t for t in sell_trades if t['profit'] > 0]
        win_rate = (len(win_trades) / len(sell_trades) * 100) if sell_trades else 0

        print("\n" + "=" * 60)
        print("📊 最终回测报告")
        print("=" * 60)
        print(f"💰 初始资金   : ¥{self.initial_cash:,.2f}")
        print(f"💰 最终资产   : ¥{final_value:,.2f}")
        print(f"📈 总收益率   : {total_return:+.2f}%")
        print(f"📅 年化收益率 : {annualized_return:+.2f}%")
        print("-" * 60)
        print(f"⚡ 夏普比率   : {sharpe_ratio:.2f} (承受单位风险获得的超额回报)")
        print(f"📉 最大回撤   : {max_drawdown:.2f}% (历史最大亏损幅度)")
        print("-" * 60)
        print(f"🎲 交易次数   : {len(sell_trades)} 次")
        print(f"✅ 胜率       : {win_rate:.2f}%")

        if sell_trades:
            avg_profit = np.mean([t['profit'] for t in sell_trades])
            avg_loss = np.mean([t['profit'] for t in sell_trades if t['profit'] < 0] or [0])
            avg_win = np.mean([t['profit'] for t in win_trades] or [0])
            # 盈亏比
            p_l_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
            print(f"⚖️ 盈亏比     : {p_l_ratio:.2f} (平均盈利/平均亏损)")

        print("=" * 60)

        # 可选：绘制简单的ASCII图表
        self.plot_ascii_chart(df['total_value'])

    def plot_ascii_chart(self, series):
        """在控制台打印简单的资产曲线"""
        print("\n📈 资产曲线概览:")
        try:
            # 简化版采样，防止太长
            sample = series.iloc[::max(1, len(series) // 40)]
            min_val = sample.min()
            max_val = sample.max()
            range_val = max_val - min_val

            if range_val == 0: return

            for date, val in sample.items():
                # 归一化到 0-50 的宽度
                width = int((val - min_val) / range_val * 50)
                date_str = date.strftime('%y-%m')
                print(f"{date_str} |{'#' * width} ¥{val / 10000:.1f}万")
        except:
            pass
class LiveTrader:
    """实盘交易（信号提示）"""

    def __init__(self, db, initial_cash):
        self.db = db
        self.cash = initial_cash
        self.holdings = {}

    def run_live(self):
        """实时监控（每分钟扫描）"""
        print("\n🚀 实盘监控启动（仅提供信号，不实际交易）")
        print("=" * 60)

        tickers = self.db.get_all_tickers_with_data()
        print(f"📈 监控股票池: {len(tickers)} 只")

        while True:
            now = datetime.now()

            # 仅在交易时间运行（周一至周五 9:30-15:00）
            if now.weekday() < 5 and 9 <= now.hour < 15:
                print(f"\n⏰ [{now.strftime('%Y-%m-%d %H:%M:%S')}] 扫描中...")

                for ticker in tickers[:50]:  # 限制扫描数量
                    try:
                        # 获取最新数据
                        stock = yf.Ticker(ticker)
                        hist = stock.history(period="150d")

                        if hist.empty:
                            continue

                        current_price = hist['Close'].iloc[-1]

                        # 持仓检查
                        if ticker in self.holdings:
                            should_sell, reason = StrategyEngine.check_sell_signal(
                                self.holdings[ticker], current_price, now
                            )
                            if should_sell:
                                print(f"🔔 卖出信号: {ticker} @ ¥{current_price:.2f} ({reason})")

                        # 买入检查
                        else:
                            should_buy, reason = StrategyEngine.check_buy_signal(hist)
                            if should_buy:
                                print(f"💰 买入信号: {ticker} @ ¥{current_price:.2f}")

                    except Exception as e:
                        pass

            time.sleep(60)  # 每分钟扫描

def main():
    print("=" * 60)
    print("     A股量化交易系统 (自动断点续传版)")
    print("=" * 60)
    print("\n请选择功能：")
    print("1. 下载/补全 A股历史数据")
    print("2. 运行历史回测")
    print("3. 实盘监控（交易信号提示）")
    print("0. 退出")

    choice = input("\n请输入选项 (0-3): ").strip()

    db = StockDatabase(DB_PATH)  # 确保你有定义 DB_PATH 常量，例如 'stock_data.db'

    try:
        if choice == '1':
            # 只需要询问批次大小，不需要询问开始位置了
            batch_str = input("每批下载数量 (默认100): ").strip()
            batch_size = int(batch_str) if batch_str.isdigit() else 100

            collector = DataCollector(db)
            # 这里的 start_date 请确保定义了 DATA_START_DATE 常量，例如 '2020-01-01'
            collector.download_all_stocks_data(DATA_START_DATE, batch_size=batch_size)

        elif choice == '2':
            # 确保定义了 INITIAL_CASH, BACKTEST_START, BACKTEST_END
            backtester = Backtester(db, INITIAL_CASH)
            backtester.run_backtest(BACKTEST_START, BACKTEST_END)

        elif choice == '3':
            trader = LiveTrader(db, INITIAL_CASH)
            trader.run_live()

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