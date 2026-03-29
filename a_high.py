
import akshare as ak
import pandas as pd
import numpy as np
import time,requests
import logging, warnings
from datetime import datetime
import clickhouse_connect
import tushare as ts
from clickhouse_connect.driver.client import Client

# ClickHouse 连接配置
CH_HOST = 'localhost'
CH_PORT = 8123  # 默认 HTTP 端clickhouse server口
CH_USER = 'default'  # 根据你的配置修改
CH_PASSWORD = ''  # 根据你的配置修改
CH_DATABASE = 'stock_db'  # 自定义数据库名

DB_PATH = CH_DATABASE  # 保持常量名，但实际用于存储数据库名
INITIAL_CASH = 100000
DATA_START_DATE = '2023-05-01'  # 数据起始日期
BACKTEST_START = '2024-01-01'  # 回测起始日期
BACKTEST_END = '2025-12-11'  # 回测结束日期
warnings.simplefilter(action='ignore', category=FutureWarning)# 忽略 pandas 警告

class StockDatabase:
    """股票数据库管理类 (ClickHouse版本)"""

    def __init__(self, db_path):
        self.db_path = db_path
        self.client: Client = None  # 使用 Type Hint
        self.init_database()

    def init_database(self):
        """初始化数据库连接和表结构"""
        try:
            # 1. 尝试连接到 ClickHouse 的默认数据库 'default'
            self.client = clickhouse_connect.get_client(
                host=CH_HOST,
                port=CH_PORT,
                username=CH_USER,
                password=CH_PASSWORD,
                # 关键修改：连接时不指定目标数据库，先连到默认数据库
                database='default'
            )

            # 2. 确保目标数据库存在
            # 这一步必须在连接成功后执行
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.db_path}")
            print(f"✅ ClickHouse 数据库 '{self.db_path}' 连接成功")

            # 3. 切换到目标数据库，并创建表结构
            # 所有的 CREATE TABLE 语句都需要加上目标数据库名

            # 股票列表表
            self.client.command(f'''
                CREATE TABLE IF NOT EXISTS {self.db_path}.stock_list (
                    ticker String,
                    name String,
                    short_name String,
                    market String,
                    market_cap Float64,
                    shares_outstanding Float64,  -- 流通股本
                    total_shares Float64,        -- 总股本
                    is_st UInt8,
                    last_update DateTime
                ) ENGINE = ReplacingMergeTree
                ORDER BY ticker
            ''')

            # 日K线数据表 - 添加停牌标记
            self.client.command(f'''
                CREATE TABLE IF NOT EXISTS {self.db_path}.daily_kline (
                    ticker String,
                    date Date,
                    open Float64,
                    high Float64,
                    low Float64,
                    close Float64,
                    volume Int64,
                    market_cap Float64) ENGINE = ReplacingMergeTree
                ORDER BY (ticker, date)
                PRIMARY KEY (ticker, date)
            ''')

            # 分钟K线数据表
            self.client.command(f'''
                CREATE TABLE IF NOT EXISTS {self.db_path}.minute_kline (
                    ticker String,
                    datetime DateTime,
                    open Float64,
                    high Float64,
                    low Float64,
                    close Float64,
                    volume Int64
                ) ENGINE = ReplacingMergeTree
                ORDER BY (ticker, datetime)
                PRIMARY KEY (ticker, datetime)
            ''')

            print("✅ 数据库表初始化完成")
        except Exception as e:
            print(f"❌ ClickHouse 连接或初始化失败: {e}")
            raise

    def get_all_a_stocks(self):
        """获取所有A股代码及核心指数"""
        print("🔍 正在从 AkShare 获取股票列表...")

        try:
            # 获取沪深A股实时行情
            stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()

            # 提取股票代码并转换为标准格式
            stocks = []
            for code in stock_zh_a_spot_em_df['代码']:
                if code.startswith('6'):  # 沪市
                    stocks.append(f"{code}.SS")
                elif code.startswith(('0', '3')):  # 深市和创业板
                    stocks.append(f"{code}.SZ")
                elif code.startswith('8'):  # 北交所
                    stocks.append(f"{code}.BJ")

            # 添加核心指数
            indices = [
                '000001.SS',  # 上证指数
                '399001.SZ',  # 深证成指
                '399006.SZ',  # 创业板指
                '000300.SS',  # 沪深300
                '000688.SS',  # 科创50
                '899050.BJ'  # 北证50
            ]
            stocks = indices + stocks

            print(f"📊 获取成功: {len(indices)}个指数 + {len(stocks) - len(indices)}只个股")
            return stocks

        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []

    def save_stock_list(self, stocks_info):
        """保存股票列表到数据库 (ClickHouse版本)"""
        if not self.client or not stocks_info:
            return

        data = []
        now_dt = datetime.now()

        for stock in stocks_info:

            data.append([
                stock['ticker'],
                stock.get('name', ''),
                stock.get('short_name', ''),
                stock.get('market', ''),
                float(stock.get('market_cap', 0.0)),
                float(stock.get('shares_outstanding', 0.0)),
                float(stock.get('total_shares', 0.0)),
                1 if stock.get('is_st', False) else 0,
                now_dt
            ])

        try:
            self.client.insert(f'{self.db_path}.stock_list', data, column_names=[
                'ticker', 'name', 'short_name', 'market', 'market_cap', 'shares_outstanding',
                'total_shares', 'is_st', 'last_update'
            ])
        except Exception as e:
            print(f"❌ 批量插入 stock_list 失败: {e}")

    def save_daily_data(self, ticker, df):
        """保存日K线数据 (ClickHouse版本) - 增强容错"""
        if df.empty:
            print(f"⚠️ {ticker} 数据为空，跳过插入。")
            return 0

        if not self.client:
            return 0

        try:
            df_copy = df.copy()

            # 处理索引
            if df_copy.index.name == 'Date' or pd.api.types.is_datetime64_any_dtype(df_copy.index):
                df_copy.reset_index(inplace=True)
                if 'Date' in df_copy.columns:
                    df_copy.rename(columns={'Date': 'date'}, inplace=True)

            # 处理时区
            if 'date' in df_copy.columns:
                if pd.api.types.is_datetime64_any_dtype(df_copy['date']):
                    if df_copy['date'].dt.tz is not None:
                        df_copy['date'] = df_copy['date'].dt.tz_localize(None)

            # 统一列名为小写
            df_copy.columns = df_copy.columns.str.lower()

            # 确保必要字段存在
            required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_columns if col not in df_copy.columns]
            if missing_cols:
                print(f"❌ {ticker} 缺少必要列: {missing_cols}")
                return 0

            # 处理market_cap列 - 关键修复
            if 'market cap' in df_copy.columns:
                df_copy['market_cap'] = df_copy['market cap'].fillna(0.0).astype(float)
                df_copy.drop(columns=['market cap'], inplace=True)
            elif 'market_cap' not in df_copy.columns:
                df_copy['market_cap'] = 0.0

            # 确保market_cap是float类型且无NaN
            df_copy['market_cap'] = df_copy['market_cap'].fillna(0.0).astype(float)

            # 准备插入数据
            data_to_insert = df_copy[['date', 'open', 'high', 'low', 'close', 'volume',
                                      'market_cap']].values.tolist()

            # 插入ticker字段
            for row in data_to_insert:
                row.insert(0, ticker)

            columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume',
                       'market_cap']

            # 批量插入数据
            self.client.insert(f'{self.db_path}.daily_kline', data_to_insert, column_names=columns)
            return len(data_to_insert)

        except Exception as e:
            print(f"\n❌ {ticker} 保存失败: {e}")
            import traceback
            traceback.print_exc()  # 打印详细错误
            return 0

    def get_daily_data(self, ticker, start_date=None, end_date=None):
        """从数据库读取日K线数据 (ClickHouse版本)"""
        # 注意 ClickHouse 的 Date 类型查询
        query = f"SELECT ticker, date, open, high, low, close, volume, market_cap FROM {self.db_path}.daily_kline WHERE ticker = %(ticker)s"
        params = {'ticker': ticker}

        if start_date:
            query += " AND date >= %(start_date)s"
            params['start_date'] = start_date

        if end_date:
            query += " AND date <= %(end_date)s"
            params['end_date'] = end_date

        # ClickHouse 默认按 ORDER BY 排序，这里可以加上明确排序
        query += " ORDER BY date ASC"

        try:
            # 使用 client.query_df() 直接返回 Pandas DataFrame
            df = self.client.query_df(query, parameters=params)
        except Exception as e:
            print(f"❌ 查询 daily_kline 失败: {e}")
            return pd.DataFrame()

        if not df.empty:
            # 重命名列以匹配原始代码的后续处理逻辑
            df.columns = ['ticker', 'date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap']
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            # 删除不再需要的 ticker 列
            df.drop(columns=['ticker'], inplace=True, errors='ignore')

        return df

    def get_latest_price(self, ticker, date_str):
        """获取指定日期的收盘价 - 优化版"""
        query = f"""
            SELECT close 
            FROM {self.db_path}.daily_kline 
            WHERE ticker = %(ticker)s 
              AND date = %(date)s
            LIMIT 1
        """

        result = self.client.query_df(query, parameters={
            'ticker': ticker,
            'date': date_str
        })
        if not result.empty:
            return result['close'].iloc[0]
        else:
            print(f"❌ 查询价格失败 {ticker} @ {date_str}")
            return 0

    def get_all_tickers_with_data(self):
        """获取有数据的所有股票代码 (ClickHouse版本)"""
        # 修复：加上 {self.db_path}. 前缀
        query = f"SELECT DISTINCT ticker FROM {self.db_path}.daily_kline"

        try:
            result = self.client.query(query)
            return [row[0] for row in result.result_rows]
        except Exception as e:
            # 这里的失败可能是因为表确实不存在 (第一次运行)，需要优雅处理
            # 第一次运行时，如果表不存在，ClickHouse会抛出异常，这里捕获后返回空列表
            if 'UNKNOWN_TABLE' in str(e):
                return []
            print(f"❌ 查询 DISTINCT ticker 失败: {e}")
            return []

    def close(self):
        # ClickHouse Connect 的客户端通常不需要显式关闭
        # 但如果使用了连接池，可能需要管理连接。这里保持简单，不作操作
        pass

    def get_stock_info(self, ticker):
        """获取股票基本信息"""
        query = f"""
            SELECT ticker, name, short_name, market, shares_outstanding, total_shares,
       is_st
            FROM {self.db_path}.stock_list 
            WHERE ticker = %(ticker)s
        """
        try:
            df = self.client.query_df(query, parameters={'ticker': ticker})
            if not df.empty:
                return df.iloc[0].to_dict()
        except Exception as e:
            print(f"❌ 查询股票信息失败: {e}")
        return None

    def update_short_names(self):
        """从 TuShare API 更新所有股票的中文简称到 short_name 字段"""
        try:
            # 需要用户提供 TuShare token (从 https://tushare.pro/register 获取)
            TOKEN = 'f84e4be3d2b660c9277360e4e56f96416af1c40b6c7088f1e5b5ad93'  # 请替换为你的实际 token

            pro = ts.pro_api(token=TOKEN)

            # 获取所有 A 股基本信息 (list_status='L' 为上市股票)
            df = pro.stock_basic(list_status='L', fields='ts_code,symbol,name')

            if df.empty:
                print("❌ TuShare 返回空数据")
                return

            updated_count = 0
            # 修改: 在 update_short_names 方法的 for 循环中，修正 TuShare 的 ts_code 为数据库格式 (.SH -> .SS)
            for _, row in df.iterrows():
                ticker = row['ts_code'].upper()  # 如 '000001.SZ' 或 '600000.SH'

                # 修正上海代码: .SH -> .SS
                if '.SH' in ticker:
                    ticker = ticker.replace('.SH', '.SS')

                short_name = row['name']  # 中文简称

                # 更新数据库
                update_query = f"""
                ALTER TABLE {self.db_path}.stock_list
                UPDATE short_name = '{short_name}'
                WHERE ticker = '{ticker}'
                """
                try:
                    self.client.command(update_query)
                    updated_count += 1
                    print(f"✅ 更新 {ticker}: {short_name}")
                except Exception as e:
                    print(f"⚠️ 更新 {ticker} 失败: {e}")
            print(f"🎉 完成更新: {updated_count}/{len(df)} 只股票")
        except Exception as e:
            print(f"❌ 更新过程出错: {e}. 请检查 TuShare token 是否正确，并确保已注册。")
class DataCollector:

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

    def download_all_stocks_data(self, start_date, batch_size=100):
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
                    # 提取股票代码（去掉后缀）
                    code = ticker.split('.')[0]
                    market = ticker.split('.')[1]

                    # 根据市场选择对应的AkShare接口
                    if market in ['SS', 'SZ']:
                        # 获取A股历史数据
                        hist = ak.stock_zh_a_hist(
                            symbol=code,
                            period="daily",
                            start_date=start_date.replace('-', ''),
                            end_date=datetime.now().strftime('%Y%m%d'),
                            adjust="qfq"  # 前复权
                        )
                    elif code.startswith('000') or code.startswith('399'):
                        # 指数数据
                        hist = ak.stock_zh_index_daily(symbol=f"sh{code}" if market == 'SS' else f"sz{code}")
                    else:
                        continue

                    # 检查数据有效性
                    if hist is None or hist.empty or len(hist) < 20:
                        self.error_stats['no_data'].append(ticker)
                        continue

                    # 统一列名（AkShare返回中文列名）
                    column_mapping = {
                        '日期': 'Date',
                        '开盘': 'Open',
                        '最高': 'High',
                        '最低': 'Low',
                        '收盘': 'Close',
                        '成交量': 'Volume',
                        '成交额': 'Amount'
                    }
                    hist.rename(columns=column_mapping, inplace=True)

                    # 设置日期为索引
                    if 'Date' in hist.columns:
                        hist['Date'] = pd.to_datetime(hist['Date'])
                        hist.set_index('Date', inplace=True)

                    # 获取股票基本信息
                    name = ''
                    short_name = ''
                    is_st = 0
                    shares_outstanding = 0.0
                    total_shares = 0.0


                    # 获取个股信息
                    stock_info = ak.stock_individual_info_em(symbol=code)
                    if not stock_info.empty:
                        for _, row in stock_info.iterrows():
                            if row['item'] == '股票简称':
                                short_name = row['value']
                                name = short_name
                            elif row['item'] == '总股本':
                                total_shares = float(row['value']) * 10000  # 转换为股
                            elif row['item'] == '流通股':
                                shares_outstanding = float(row['value']) * 10000

                    # 判断是否ST股
                    if 'ST' in short_name.upper() or '*ST' in short_name.upper():
                        is_st = 1



                    # 计算市值
                    if shares_outstanding > 0:
                        hist['Market Cap'] = hist['Close'].astype(float) * float(shares_outstanding)
                    else:
                        hist['Market Cap'] = 0.0


                    # 保存数据（后续代码保持不变）
                    saved_rows = self.db.save_daily_data(ticker, hist)
                    if saved_rows > 0:
                        # 获取最新市值和停牌状态（用于stock_list）
                        latest_market_cap = hist['Market Cap'].iloc[-1]


                        stocks_info.append({
                            'ticker': ticker,
                            'name': name if name else ticker,
                            'short_name': short_name,  # 新增: 简称
                            'market': 'SH' if ticker.endswith('.SS') else 'SZ',
                            'market_cap': float(latest_market_cap),
                            'shares_outstanding': float(shares_outstanding),
                            'total_shares': float(total_shares),
                            'is_st': is_st# 新增: ST标记（判断历史的ST股基于当前名称是否含ST，假设当前含ST即视为历史ST股）
                        })
                        success_count += 1
                    else:
                        self.error_stats['no_data'].append(ticker)


                    # 实时进度打印
                    print(f"\rProcess: [{i}/{len(batch_stocks)}] | {ticker} | 成功: {success_count}", end='',
                          flush=True)

                    time.sleep(0.1)  # 防封

                except KeyboardInterrupt:
                    print(f"\n\n⚠️  用户中断！已保存当前批次数据。")
                    if stocks_info:
                        self.db.save_stock_list(stocks_info)
                    return

                except Exception as e:
                    # 外层异常捕获
                    error_msg = str(e)
                    print(e)
                    self.classify_error(ticker, error_msg)
                    # 不打印每个错误，只在classify_error中记录
                    continue
            # 批次结束保存元数据
            if stocks_info:
                self.db.save_stock_list(stocks_info)

            print(f"\n✅ 本批完成: 成功入库 {success_count} 只")

            # 批次间休息
            if batch_num < total_batches - 1:
                time.sleep(1)
        self.db.update_short_names()
        print(f"\n\n🎉 所有任务处理完成！")
        self.print_error_summary()

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
        pre_row = data_daily.iloc[-2]

        if current_row['Close']/pre_row['Close'] -1>0.03 :
            return False, "当日涨幅大于3个点"
        # 1. 100天创新高 (JQ: past_high = close_df.max())
        # 注意：JQ是用过去100天(不含今日)的Close最大值
        past_100_high = prev_data['Close'].iloc[-100:].max()
        if current_row['Close'] <= past_100_high:
            return False, "未创新高"

        # 2. 检查阴线长上影 (JQ: 条件6)
        if StrategyEngine.check_shadow_line(current_row):
            return False, "长上影线"

        # 条件3：近期10天无涨停 (不含今日)
        recent_15_pct = prev_data['Close'].iloc[-10:].pct_change()
        if (recent_15_pct > 0.098).any():
            return False, "近期有涨停"

        # 4. TD九转检查 (JQ: 条件4 & 5)
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
        卖出信号检查 早盘交易
        :param holding_info: 持仓字典
        :param current_row: 当日K线数据 (Series)
        :param prev_close: 昨日收盘价 (用于计算涨停)
        """
        current_price = current_row['Close']   #早盘交易价格作为当前价格，开盘就交易
        current_date = pd.to_datetime(current_row.name)  # 假设索引是日期

        # 1. 接近涨停止盈
        sell_price = round(prev_close * 1.08, 2)
        if current_row['High'] >= sell_price:
            # print(f"prev_close:{prev_close},")
            return True, holding_info['short_name']+"大涨8个点止盈:" + str(sell_price),sell_price
        # 2. 止损：亏损6% (JQ: current_price < cost * 0.94)
        if current_price < holding_info['avg_cost'] * 0.94:
            return True, holding_info['short_name']+"止损(-6%)",current_price

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
                return True, holding_info['short_name']+"时间止损(滞涨)",current_price

        return False, "",0
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
        self.trading_dates = []  # 新增：缓存交易日历
        # --- 新增缓存结构 ---
        self.data_cache = {}  # { '600000.SS': DataFrame, ... }
        self.stock_info_cache = {}  # { '600000.SS': {'short_name': '浦发银行', 'is_st': 0}, ... }

    def load_all_data_to_cache(self, start_date, end_date):
        """🚀 核心优化：一次性加载所有数据到内存"""
        print("⚡ 正在预加载全量数据到内存 (约为回测期 + 前推150天)...")

        # 1. 加载股票基础信息 (用于快速获取名称和ST判断)
        info_query = f"SELECT ticker, short_name, is_st FROM {self.db.db_path}.stock_list"
        info_res = self.db.client.query(info_query)
        for row in info_res.result_rows:
            self.stock_info_cache[row[0]] = {'short_name': row[1], 'is_st': row[2]}

        # 2. 计算数据加载的起始时间 (多加载150天用于计算MA/TD9)
        load_start = (pd.to_datetime(start_date) - pd.Timedelta(days=150)).strftime('%Y-%m-%d')

        # 3. 批量拉取K线数据 (仅拉取必要列，float32省内存)
        # 注意：这里直接查所有股票，内存占用约 200-300MB，完全可控
        kline_query = f"""
            SELECT ticker, date, open, close, high, low, volume, market_cap
            FROM {self.db.db_path}.daily_kline 
            WHERE date >= '{load_start}' AND date <= '{end_date}'
            ORDER BY ticker, date
            """

        # 使用 query_df 直接获取 DataFrame
        df_all = self.db.client.query_df(kline_query)

        if not df_all.empty:
            # 统一列名
            df_all.columns = ['ticker', 'date', 'Open', 'Close', 'High', 'Low', 'Volume', 'Market Cap']
            df_all['date'] = pd.to_datetime(df_all['date'])
            df_all.set_index('date', inplace=True)

            # 转换为字典结构加速索引: {ticker: DataFrame}
            # 这一步是Python层面的分组，速度很快
            self.data_cache = {k: v for k, v in df_all.groupby('ticker')}

        print(f"✅ 缓存完成: {len(self.data_cache)} 只股票数据, {len(self.stock_info_cache)} 条基础信息")

    def get_price_from_cache(self, ticker, date_str):
        """从缓存获取收盘价 (用于市值计算) - 停牌时返回最近交易日价格"""
        try:
            df = self.data_cache.get(ticker)
            if df is not None:
                # 尝试获取当日数据
                if date_str in df.index:
                    return df.loc[date_str]['Close']

                # 停牌或无数据时，找最近的一个收盘价（截取到当日之前）
                sliced = df.loc[:date_str]
                if not sliced.empty:
                    return sliced.iloc[-1]['Close']
        except Exception as e:
            print(f"⚠️ 获取 {ticker} 价格失败: {e}")
        return 0
    def get_trading_dates(self, start_date, end_date):
        """
        获取指定日期区间的所有交易日（基于600000.SS的实际数据）
        只查询一次，大幅提升性能
        """
        query = f"""
        SELECT DISTINCT date 
        FROM {self.db.db_path}.daily_kline 
        WHERE ticker = '600000.SS' 
          AND date >= '{start_date}' 
          AND date <= '{end_date}'
        ORDER BY date ASC
        """

        try:
            result = self.db.client.query(query)
            if result.result_rows:
                # 转换为日期列表
                trading_dates = [pd.to_datetime(row[0]) for row in result.result_rows]
                print(f"✅ 获取到 {len(trading_dates)} 个交易日")
                return trading_dates
            else:
                print("⚠️ 未找到600000.SS的交易数据，使用全日期范围")
                return pd.date_range(start=start_date, end=end_date, freq='D')
        except Exception as e:
            print(f"❌ 获取交易日历失败: {e}，使用全日期范围")
            return pd.date_range(start=start_date, end=end_date, freq='D')

    def buy_stock(self, ticker, price, high_price, qty, date, prev_close):
        """执行买入 (增加涨停检查和资金不足检查，更新记录字段)"""

        # 判断板块类型，确定涨停阈值
        is_star_market = ticker.startswith('688')  # 科创板
        is_chi_next = ticker.startswith('300')  # 创业板
        limit_up_threshold = 0.20 if (is_star_market or is_chi_next) else 0.10


        # 获取股票简称（用于日志）
        stock_info_query = f"SELECT short_name FROM {self.db.db_path}.stock_list WHERE ticker = '{ticker}'"
        try:
            stock_result = self.db.client.query(stock_info_query)
            short_name = stock_result.result_rows[0][0] if stock_result.result_rows else ticker
        except:
            short_name = ticker
        # 涨停检查（需要前一日收盘价）
        change_pct = (price - prev_close) / prev_close
        if change_pct >= limit_up_threshold - 0.001:  # 预留0.1%误差容限


            board_name = "科创板" if is_star_market else ("创业板" if is_chi_next else "主板")
            print(f"⚠️ {short_name}({board_name})涨停({change_pct * 100:.2f}%)，无法买入")
            return None  # 返回None表示买入失败

        # 资金不足检查
        cost = price * qty * (1 + 0.0003)  # 计算所需成本（含滑点）
        if cost > self.cash:
            min_qty = 200 if is_star_market else 100
            print(f"⚠️ 跳过 {short_name}({ticker}): 需{qty}股(约{cost:.0f}元), 仅有现金{self.cash:.0f}元")
            return None

        # 执行买入
        self.cash -= cost

        self.holdings[ticker] = {
            'short_name':short_name,
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

    def run_backtest(self, start_date, end_date):
        """运行回测 (高性能内存版)"""
        print(f"\n📊 开始回测 ({start_date} 至 {end_date})")
        print(f"💰 初始资金: ¥{self.initial_cash:,.2f}")
        print("=" * 100)

        # --- 步骤1：预加载数据 ---
        self.load_all_data_to_cache(start_date, end_date)

        # 获取交易日历 (可以直接用缓存里的数据生成，或者查一次库)
        if '600000.SS' in self.data_cache:
            # 利用缓存生成交易日历，完全不查库
            full_dates = self.data_cache['600000.SS'].index
            self.trading_dates = [d for d in full_dates if start_date <= d.strftime('%Y-%m-%d') <= end_date]
        else:
            self.trading_dates = self.get_trading_dates(start_date, end_date)

        header = f"{'日期':<12} | {'资产总值':<12} | {'日盈亏':<10} | {'持仓':<4} | {'选股/买入':<20} | {'卖出/止盈损'}"
        print(header)
        print("-" * 100)
        prev_total_value = self.initial_cash

        for current_date in self.trading_dates:
            date_str = current_date.strftime('%Y-%m-%d')
            current_month = current_date.month
            is_no_trade_month = current_month in [1, 4]

            # --- 特殊月份清仓逻辑 ---
            if is_no_trade_month and self.holdings:
                print(f"🚫 {current_month}月不交易期，执行清仓...")
                for ticker in list(self.holdings.keys()):
                    # 从缓存获取数据
                    df = self.data_cache.get(ticker)
                    if df is None: continue

                    # 截取到当前日期
                    df_latest = df.loc[:date_str]
                    if df_latest.empty or df_latest.index[-1].strftime('%Y-%m-%d') != date_str:
                        continue

                    curr_row = df_latest.iloc[-1]
                    if curr_row['Volume'] > 0:  # 仅非停牌可卖
                        self.sell_stock(ticker, curr_row['Close'], date_str, f"{current_month}月清仓")

            if is_no_trade_month:
                continue

            # --- 阶段1：卖出检查 (纯内存操作) ---
            daily_sold = []
            daily_suspended = []  # 新增：记录停牌股票

            for ticker in list(self.holdings.keys()):
                df = self.data_cache.get(ticker)
                if df is None:
                    continue

                # 获取截止到当日的数据窗口
                df_slice = df.loc[:date_str]

                # 检查当日是否有数据（是否停牌）
                if df_slice.empty:
                    continue

                last_trade_date = df_slice.index[-1].strftime('%Y-%m-%d')

                # 判断是否停牌：最后交易日不是当日（数据库无当日数据即停牌）
                if last_trade_date != date_str:
                    short_name = self.holdings[ticker].get('short_name', ticker)
                    daily_suspended.append(f"{short_name}(停牌)")
                    continue  # 停牌时跳过卖出检查

                curr_row = df_slice.iloc[-1]
                prev_close = df_slice['Close'].iloc[-2] if len(df_slice) >= 2 else curr_row['Open']

                # 检查卖出信号
                should_sell, reason, sell_price = StrategyEngine.check_sell_signal(
                    self.holdings[ticker], curr_row, prev_close
                )

                if should_sell:
                    self.sell_stock(ticker, sell_price, date_str, reason)
                    daily_sold.append(f"{ticker}|{reason}")
            # --- 阶段2：选股 (纯内存操作，替换原有的 SQL Query) ---
            candidates = []

            # 遍历内存缓存，代替 SQL 查询
            # 这里的 items() 遍历在 Python 3 中也是比较高效的
            for ticker, df in self.data_cache.items():
                if ticker in self.holdings: continue

                # 1. 快速过滤 ST (查缓存字典)
                s_info = self.stock_info_cache.get(ticker)
                # 如果库里标记了ST，或者名字里带ST
                if s_info and (s_info['is_st'] == 1 or 'ST' in s_info['short_name'].upper()):
                    continue

                # 2. 截取数据窗口 (截至当前日期)
                # 优化：先判断日期是否存在，避免切片开销
                if date_str not in df.index:
                    continue

                # 只需要切片到 date_str
                data_window = df.loc[:date_str]

                # 确保数据长度足够策略计算 (105天)
                if len(data_window) < 105: continue

                curr_row = data_window.iloc[-1]
                pre_row = data_window.iloc[-2]

                # 3. 运行策略
                should_buy, _ = StrategyEngine.check_buy_signal(data_window)

                if should_buy:
                    short_name = s_info['short_name'] if s_info else ticker
                    candidates.append({
                        'ticker': ticker,
                        'close': curr_row['Close'],
                        'prev_close': pre_row['Close'],
                        'high': curr_row['High'],
                        'market_cap': curr_row['Market Cap'],
                        'short_name': short_name,
                        'change_pct': curr_row['Close'] / pre_row['Close'] - 1
                    })

            # --- 阶段3：排序与资金分配 (逻辑保持不变) ---
            # ... (这部分代码无需修改，保持原样即可) ...
            TARGET_NUM = 10
            current_positions = len(self.holdings)
            slots_available = TARGET_NUM - current_positions
            daily_bought = []

            if slots_available > 0 and candidates:
                candidates.sort(key=lambda x: x.get('market_cap', float('inf')))
                buy_targets = candidates[:slots_available]

                if buy_targets:
                    per_stock_cash = self.cash / len(buy_targets)
                    for target in buy_targets:
                        ticker = target['ticker']
                        is_star_market = ticker.startswith('688')
                        min_qty = 200 if is_star_market else 100

                        # 计算手数
                        can_buy_shares = (per_stock_cash / target['close']) // 100 * 100
                        qty = max(min_qty, can_buy_shares)

                        # 买入
                        res = self.buy_stock(ticker, target['close'], target['high'], qty, date_str,
                                             target['prev_close'])
                        if res: daily_bought.append(target['short_name'])

            # --- 每日结算 (使用缓存价格) ---
            # 优化：get_price_from_cache 也是纯内存操作
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
            if daily_bought:
                action_str = f"选{len(candidates)}/买[{','.join(daily_bought)}]"
            else:
                action_str = f"选{len(candidates)}/无买入"

            # 修改：合并卖出和停牌信息
            sell_info = []
            if daily_sold:
                sell_info.extend(daily_sold)
            if daily_suspended:
                sell_info.extend(daily_suspended)
            sold_str = ",".join(sell_info) if sell_info else "-"

            print(
                f"{date_str:<12} | {total_value:<12.0f} | {daily_pnl:<+10.0f} | "
                f"{len(self.holdings):<4} | {action_str:<20} | {sold_str}"
            )

        self.print_performance_metrics()


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

        elif choice == '2':
            # 确保定义了 INITIAL_CASH, BACKTEST_START, BACKTEST_END
            backtester = Backtester(db, INITIAL_CASH)
            backtester.run_backtest(BACKTEST_START, BACKTEST_END)

        elif choice == '3':
            # trader = LiveTrader(db, INITIAL_CASH)
            # trader.run_live()
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