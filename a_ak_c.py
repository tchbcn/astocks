
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
# clickhouse server
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
INDEX_LIST=[
            '000001.SS',  # 上证指数
            '399001.SZ',  # 深证成指
            '399101.SZ',  # 中小综指
            '399006.SZ',  # 创业板指
            '000300.SS',  # 沪深300
            '000688.SS',  # 科创50
            '899050.BJ'  # 北证50
        ]
_INDEX_CACHE = None
_FINANCIAL_CACHE=None
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
            # ✅ 财务数据表（季度数据）
            self.client.command(f'''
                 CREATE TABLE IF NOT EXISTS {self.db_path}.financial_data (
                     ticker String,
                     report_date Date,              -- 财报日期（如2024-03-31）
                     ann_date Date,                 -- 公告日期（报告期+45天估算）
                     total_shares Float64,          -- 总股本（股）
                     eps Float64,                   -- 每股收益（元）
                     update_time DateTime
                 ) ENGINE = ReplacingMergeTree(update_time)
                 ORDER BY (ticker, report_date)
                 PRIMARY KEY (ticker, report_date)
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
            self.client.command(f'''
                CREATE TABLE IF NOT EXISTS {self.db_path}.index_daily_kline (
                    ticker String,
                    date Date,
                    open Float64,
                    high Float64,
                    low Float64,
                    close Float64,
                    volume Int64,
                    amount Float64
                ) ENGINE = ReplacingMergeTree
                ORDER BY (ticker, date)
                PRIMARY KEY (ticker, date)
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
            stock_zh_a_spot_em_df = ak.stock_info_a_code_name()

            # 提取股票代码并转换为标准格式
            stocks = []
            for code in stock_zh_a_spot_em_df['code']:
                if code.startswith('6'):  # 沪市
                    stocks.append(f"{code}.SS")
                elif code.startswith(('0', '3')):  # 深市和创业板
                    stocks.append(f"{code}.SZ")
                elif code.startswith('8'):  # 北交所
                    stocks.append(f"{code}.BJ")


            print(f"📊 获取成功:  {len(stocks)}只个股")
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

    def save_index_daily_data(self, ticker, df):
        """保存指数日K线数据 (ClickHouse版本)"""
        if df.empty:
            print(f"⚠️ {ticker} 指数数据为空，跳过插入。")
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
                print(f"❌ {ticker} 指数缺少必要列: {missing_cols}")
                return 0

            # 处理成交额字段
            if 'amount' not in df_copy.columns:
                df_copy['amount'] = 0.0
            df_copy['amount'] = df_copy['amount'].fillna(0.0).astype(float)

            # 准备插入数据
            data_to_insert = df_copy[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']].values.tolist()

            # 插入ticker字段
            for row in data_to_insert:
                row.insert(0, ticker)

            columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']

            # 批量插入数据
            self.client.insert(f'{self.db_path}.index_daily_kline', data_to_insert, column_names=columns)
            return len(data_to_insert)

        except Exception as e:
            print(f"\n❌ {ticker} 指数保存失败: {e}")
            import traceback
            traceback.print_exc()
            return 0

    def get_index_daily_data(self, ticker, start_date=None, end_date=None):
        """从数据库读取指数日K线数据 (ClickHouse版本)"""
        query = f"SELECT ticker, date, open, high, low, close, volume, amount FROM {self.db_path}.index_daily_kline WHERE ticker = %(ticker)s"
        params = {'ticker': ticker}

        if start_date:
            query += " AND date >= %(start_date)s"
            params['start_date'] = start_date

        if end_date:
            query += " AND date <= %(end_date)s"
            params['end_date'] = end_date

        query += " ORDER BY date ASC"

        try:
            df = self.client.query_df(query, parameters=params)
        except Exception as e:
            print(f"❌ 查询 index_daily_kline 失败: {e}")
            return pd.DataFrame()

        if not df.empty:
            df.columns = ['ticker', 'date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount']
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df.drop(columns=['ticker'], inplace=True, errors='ignore')

        return df

    def get_all_index_tickers(self):
        """获取有数据的所有指数代码"""
        query = f"SELECT DISTINCT ticker FROM {self.db_path}.index_daily_kline"

        try:
            result = self.client.query(query)
            return [row[0] for row in result.result_rows]
        except Exception as e:
            if 'UNKNOWN_TABLE' in str(e):
                return []
            print(f"❌ 查询指数 ticker 失败: {e}")
            return []
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
    def download_all_indexs_data(self,start_date):
        # 添加核心指数
        index_list = INDEX_LIST

        exist_indices = set(self.db.get_all_index_tickers())
        pending_indices = [t for t in index_list if t not in exist_indices]

        print(f"   指数总数: {len(index_list)}")
        print(f"   ✅ 已下载: {len(exist_indices)}")
        print(f"   ⏳ 待下载: {len(pending_indices)}")

        if not pending_indices:
            print("✅ 所有指数已存在！")
            return

        success_count = 0
        for i, ticker in enumerate(pending_indices, 1):
            try:
                code = ticker.split('.')[0]
                market = ticker.split('.')[1]

                # 使用AkShare指数接口
                if market == 'SS':
                    hist = ak.stock_zh_index_daily(symbol=f"sh{code}")
                elif market == 'SZ':
                    hist = ak.stock_zh_index_daily(symbol=f"sz{code}")
                else:
                    continue

                if hist is None or hist.empty:
                    self.error_stats['no_data'].append(ticker)
                    continue

                # 统一列名
                column_mapping = {
                    'date': 'Date',
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume',
                    'amount': 'Amount'
                }
                hist.rename(columns=column_mapping, inplace=True)

                if 'Date' in hist.columns:
                    hist['Date'] = pd.to_datetime(hist['Date'])
                    hist.set_index('Date', inplace=True)

                # 保存到指数表
                saved_rows = self.db.save_index_daily_data(ticker, hist)
                if saved_rows > 0:
                    success_count += 1
                    print(f"\r📊 [{i}/{len(pending_indices)}] {ticker} ✅ 保存 {saved_rows} 条", end='', flush=True)

                time.sleep(0.2)

            except Exception as e:
                self.classify_error(ticker, str(e))
                continue

        print(f"\n✅ 指数下载完成: {success_count}/{len(pending_indices)}")
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
    def download_financial_data(self, start_date='2018-01-01', batch_size=50):
        """
        增强版财务数据下载器 (基于同花顺接口修改版)
        - 使用 stock_financial_abstract_ths 替代失效的东财接口
        - 自动补全缺失的资产负债数据为 0
        """
        print(f"\n💰 开始下载财务数据（同花顺源修复版）")
        print("=" * 60)

        # 1. 从数据库获取股票列表
        print("🔍 正在从数据库获取股票列表...")
        all_stocks_query = f"SELECT DISTINCT ticker FROM {self.db.db_path}.stock_list"
        try:
            result = self.db.client.query(all_stocks_query)
            all_stocks = [row[0] for row in result.result_rows]
            print(f"✅ 从数据库获取到 {len(all_stocks)} 只股票")
        except Exception as e:
            print(f"❌ 数据库查询失败: {e}")
            print("💡 请先运行 download_all_stocks_data() 以填充 stock_list 表")
            return

        if not all_stocks:
            print("❌ 股票列表为空，请先下载股票数据")
            return

        # 2. 检查已有财务数据
        exist_query = f"SELECT DISTINCT ticker FROM {self.db.db_path}.financial_data"
        try:
            result = self.db.client.query(exist_query)
            exist_tickers = set([row[0] for row in result.result_rows])
        except:
            exist_tickers = set()

        pending_stocks = [s for s in all_stocks if s not in exist_tickers]

        print(f"📊 股票总数: {len(all_stocks)}")
        print(f"✅ 已下载: {len(exist_tickers)}")
        print(f"⏳ 待下载: {len(pending_stocks)}")

        if not pending_stocks:
            print("✅ 所有财务数据已存在！")
            return

        # 3. 分批处理
        total_batches = (len(pending_stocks) + batch_size - 1) // batch_size
        print(f"📦 将分 {total_batches} 批次处理")
        print("=" * 60)

        success_count = 0
        failed_stocks = []

        # 定义处理单个股票的内部逻辑 (避免代码重复)
        def process_stock_data(ticker_symbol):
            code_symbol = ticker_symbol.split('.')[0]

            # A. 获取股本信息 (保留原逻辑，通常此接口较稳定)
            t_shares = 0.0
            try:
                stock_info = ak.stock_individual_info_em(symbol=code_symbol)
                if not stock_info.empty:
                    # 查找总股本
                    row = stock_info[stock_info['item'] == '总股本']
                    if not row.empty:
                        t_shares = float(row.iloc[0]['value']) * 10000
            except Exception:
                pass  # 股本获取失败不影响主要流程，默认为0

            # B. 获取财务指标 (修改点：使用同花顺接口)
            # indicator="按报告期" 是测试中成功的参数
            df_fin = ak.stock_financial_abstract_ths(symbol=code_symbol, indicator="按报告期")

            if df_fin is None or df_fin.empty:
                return None

            # C. 列名映射 (适配同花顺返回的列名)
            # THS 列名: '报告期', '净利润', '营业总收入', '基本每股收益'
            rename_map = {
                '报告期': 'report_date',
                '净利润': 'net_profit',
                '营业总收入': 'total_revenue',
                '基本每股收益': 'eps'
            }
            df_fin.rename(columns=rename_map, inplace=True)

            # 检查关键列
            if 'report_date' not in df_fin.columns:
                return None

            # D. 数据清洗与补全
            # 1. 转换日期
            df_fin['report_date'] = pd.to_datetime(df_fin['report_date'], errors='coerce')
            df_fin.dropna(subset=['report_date'], inplace=True)
            df_fin = df_fin.sort_values('report_date')

            # 2. 转换数值类型 (同花顺可能返回字符串，需强制转float)
            numeric_cols = ['eps']
            for col in numeric_cols:
                if col in df_fin.columns:
                    df_fin[col] = pd.to_numeric(df_fin[col], errors='coerce').fillna(0.0)

            df_fin['ann_date'] = df_fin['report_date'] + pd.Timedelta(days=45)

            # F. 组装数据
            insert_rows = []
            now_time = datetime.now()

            for _, r in df_fin.iterrows():
                insert_rows.append([
                    ticker_symbol,
                    r['report_date'],
                    r['ann_date'],
                    float(t_shares),
                    float(r.get('eps', 0.0)),
                    now_time
                ])

            return insert_rows

        # --- 主循环 ---
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(pending_stocks))
            batch_stocks = pending_stocks[start_idx:end_idx]

            print(f"\n📦 批次 {batch_num + 1}/{total_batches} ({start_idx + 1}-{end_idx}/{len(pending_stocks)})")

            for i, ticker in enumerate(batch_stocks, 1):
                try:
                    data_to_insert = process_stock_data(ticker)

                    if not data_to_insert:
                        self.error_stats['no_data'].append(ticker)
                        print(f"\r⚠️ [{i}/{len(batch_stocks)}] {ticker} 无财务数据", end='')
                        continue

                    # 执行插入
                    self.db.client.insert(
                        f'{self.db.db_path}.financial_data',
                        data_to_insert,
                        column_names=[
                            'ticker', 'report_date', 'ann_date', 'total_shares',
                            'eps', 'update_time'
                        ]
                    )
                    success_count += 1
                    print(
                        f"\r✅ [{i}/{len(batch_stocks)}] {ticker} 保存 {len(data_to_insert)} 条 | 总成功: {success_count}",
                        end='', flush=True)

                    time.sleep(0.8)  # 同花顺接口由于更稳定，建议稍慢一点点防止封禁

                except KeyboardInterrupt:
                    print(f"\n\n⚠️ 用户中断！已保存 {success_count} 只股票")
                    self.print_error_summary()
                    return

                except Exception as e:
                    error_msg = str(e)
                    if 'timeout' in error_msg.lower() or 'connection' in error_msg.lower():
                        failed_stocks.append(ticker)
                        print(f"\r⚠️ [{i}/{len(batch_stocks)}] {ticker} 网络错误", end='')
                    else:
                        # 记录非网络错误
                        self.classify_error(ticker, error_msg)
                        print(f"\r❌ [{i}/{len(batch_stocks)}] {ticker} 异常: {error_msg[:30]}...", end='')
                    continue

            # 批次间休息
            if batch_num < total_batches - 1:
                print(f"\n⏸️  批次间休息 3 秒...")
                time.sleep(3)

        # 4. 重试失败的股票 (复用逻辑)
        if failed_stocks:
            print(f"\n\n🔄 开始重试 {len(failed_stocks)} 只网络失败的股票...")
            retry_success = 0

            for ticker in failed_stocks:
                try:
                    data_to_insert = process_stock_data(ticker)
                    if data_to_insert:
                        self.db.client.insert(
                            f'{self.db.db_path}.financial_data',
                            data_to_insert,
                            column_names=[
                                'ticker', 'report_date', 'ann_date', 'total_shares', 'eps', 'update_time'
                            ]
                        )
                        retry_success += 1
                        success_count += 1
                        print(f"\r✅ 重试成功: {ticker} ({retry_success}/{len(failed_stocks)})", end='', flush=True)
                    time.sleep(1)
                except:
                    continue

            print(f"\n✅ 重试完成: {retry_success}/{len(failed_stocks)} 成功")

        print(f"\n\n{'=' * 60}")
        print(f"🎉 财务数据下载完成！")
        print(f"   总成功: {success_count}/{len(pending_stocks)}")
        print(f"   💡 注意: 使用了同花顺摘要接口，资产/权益/ROE字段目前已置为0")
        print("=" * 60)
    def load_all_financial_data(self):
        """
        一次性加载所有季度财务数据到内存（全局缓存）
        返回格式: {ticker: DataFrame}，索引为report_date
        """
        global _FINANCIAL_CACHE

        if _FINANCIAL_CACHE is not None:
            print("✅ 使用已缓存的财务数据")
            return _FINANCIAL_CACHE

        print("📊 正在加载全量季度财务数据到内存...")

        query = f"""
            SELECT ticker, report_date, ann_date, total_shares, eps
            FROM {self.db_path}.financial_data
            ORDER BY ticker, report_date
        """

        try:
            df_all = self.client.query_df(query)
            # 转换日期
            df_all['report_date'] = pd.to_datetime(df_all['report_date'])
            df_all['ann_date'] = pd.to_datetime(df_all['ann_date'])

            # 按股票代码分组，存入字典
            _FINANCIAL_CACHE = {}
            for ticker, group in df_all.groupby('ticker'):
                # 以公告日期为索引（重要：基于公告日，不是报告日）
                group_sorted = group.sort_values('ann_date').set_index('ann_date')
                _FINANCIAL_CACHE[ticker] = group_sorted.drop(columns=['ticker'])

            print(f"✅ 财务数据加载完成: {len(_FINANCIAL_CACHE)} 只股票")
            return _FINANCIAL_CACHE

        except Exception as e:
            print(f"❌ 加载财务数据失败: {e}")
            _FINANCIAL_CACHE = {}
            return _FINANCIAL_CACHE
    def get_financial_metrics_by_date(self, ticker, date_str):
        """
        获取指定股票在指定日期可用的最新财务指标

        逻辑：查找公告日期 <= date_str 的最新一期财报
        """
        global _FINANCIAL_CACHE

        # 如果缓存为空，自动加载
        if _FINANCIAL_CACHE is None:
            self.load_all_financial_data()

        # 股票不存在
        if ticker not in _FINANCIAL_CACHE:
            return None

        df = _FINANCIAL_CACHE[ticker]
        query_date = pd.to_datetime(date_str)

        # 找到公告日期 <= 查询日期的所有财报
        available_reports = df[df.index <= query_date]

        if available_reports.empty:
            return None

        # 返回最新一期财报
        latest = available_reports.iloc[-1]

        return {
            'report_date': latest['report_date'],
            'total_shares': latest['total_shares'],
            'eps': latest['eps']
        }
    def download_indices(self, index_list, start_date):
        """下载指数数据（独立方法）"""
        exist_indices = set(self.db.get_all_index_tickers())
        pending_indices = [t for t in index_list if t not in exist_indices]

        print(f"   指数总数: {len(index_list)}")
        print(f"   ✅ 已下载: {len(exist_indices)}")
        print(f"   ⏳ 待下载: {len(pending_indices)}")

        if not pending_indices:
            print("✅ 所有指数已存在！")
            return

        success_count = 0
        for i, ticker in enumerate(pending_indices, 1):
            try:
                code = ticker.split('.')[0]
                market = ticker.split('.')[1]

                # 使用AkShare指数接口
                if market == 'SS':
                    hist = ak.stock_zh_index_daily(symbol=f"sh{code}")
                elif market == 'SZ':
                    hist = ak.stock_zh_index_daily(symbol=f"sz{code}")
                else:
                    continue

                if hist is None or hist.empty:
                    self.error_stats['no_data'].append(ticker)
                    continue

                # 统一列名
                column_mapping = {
                    'date': 'Date',
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume',
                    'amount': 'Amount'
                }
                hist.rename(columns=column_mapping, inplace=True)

                if 'Date' in hist.columns:
                    hist['Date'] = pd.to_datetime(hist['Date'])
                    hist.set_index('Date', inplace=True)

                # 保存到指数表
                saved_rows = self.db.save_index_daily_data(ticker, hist)
                if saved_rows > 0:
                    success_count += 1
                    print(f"\r📊 [{i}/{len(pending_indices)}] {ticker} ✅ 保存 {saved_rows} 条", end='', flush=True)

                time.sleep(0.2)

            except Exception as e:
                self.classify_error(ticker, str(e))
                continue

        print(f"\n✅ 指数下载完成: {success_count}/{len(pending_indices)}")
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

    def incremental_update(self, max_stocks_per_run=5500, max_workers=20):
        """
        智能增量更新：并发下载，速度提升10-20倍

        参数:
            max_stocks_per_run: 单次最多更新的股票数量
            max_workers: 并发线程数（建议10-20）

        返回:
            dict: 更新统计信息
        """
        print("\n🔄 开始增量更新检查...")
        print("=" * 60)

        # 1. 获取最新交易日
        today = datetime.now()
        latest_trading_day = self._get_latest_trading_day(today)
        print(f"📅 最新交易日: {latest_trading_day}")

        # 2. 检查所有股票的最新数据日期
        print("🔍 检查数据库中各股票最新数据...")

        query = f"""
            SELECT ticker, MAX(date) as last_date
            FROM {self.db.db_path}.daily_kline
            GROUP BY ticker
        """

        try:
            df_status = self.db.client.query_df(query)
            df_status['last_date'] = pd.to_datetime(df_status['last_date'])

            # 筛选出需要更新的股票
            needs_update = df_status[df_status['last_date'] < pd.to_datetime(latest_trading_day)]

            total_stocks = len(df_status)
            outdated_count = len(needs_update)

            print(f"📊 数据库状态:")
            print(f"   总股票数: {total_stocks}")
            print(f"   ✅ 已最新: {total_stocks - outdated_count}")
            print(f"   ⏳ 需更新: {outdated_count}")

            if outdated_count == 0:
                print("✅ 所有数据已是最新！")
                return {'status': 'up_to_date', 'updated': 0, 'failed': 0}

            # 3. 限制单次更新数量
            if outdated_count > max_stocks_per_run:
                print(f"⚠️  需更新股票过多，本次只更新前 {max_stocks_per_run} 只")
                needs_update = needs_update.head(max_stocks_per_run)

            # 4. 并发执行增量更新
            print(f"\n🚀 开始并发更新 {len(needs_update)} 只股票 (线程数: {max_workers})...")
            print("=" * 60)

            from concurrent.futures import ThreadPoolExecutor, as_completed
            from threading import Lock

            success_count = 0
            failed_list = []
            progress_lock = Lock()

            def update_single_stock(row):
                """更新单只股票的内部函数"""
                ticker = row['ticker']
                last_date = row['last_date']

                # ⚡ 关键修改：每个线程创建独立的 ClickHouse 客户端
                thread_client = clickhouse_connect.get_client(
                    host=CH_HOST,
                    port=CH_PORT,
                    username=CH_USER,
                    password=CH_PASSWORD,
                    database=self.db.db_path
                )

                try:
                    # 计算日期范围
                    start_date = (last_date + pd.Timedelta(days=1)).strftime('%Y%m%d')
                    end_date = datetime.now().strftime('%Y%m%d')

                    code = ticker.split('.')[0]
                    market = ticker.split('.')[1]

                    # 下载数据
                    if market not in ['SS', 'SZ']:
                        return {'status': 'skip', 'ticker': ticker}

                    hist = ak.stock_zh_a_hist(
                        symbol=code,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq"
                    )

                    if hist is None or hist.empty:
                        return {'status': 'no_data', 'ticker': ticker}

                    # 处理数据
                    column_mapping = {
                        '日期': 'Date',
                        '开盘': 'Open',
                        '最高': 'High',
                        '最低': 'Low',
                        '收盘': 'Close',
                        '成交量': 'Volume'
                    }
                    hist.rename(columns=column_mapping, inplace=True)

                    if 'Date' in hist.columns:
                        hist['Date'] = pd.to_datetime(hist['Date'])
                        hist.set_index('Date', inplace=True)

                    # ⚡ 使用线程独立客户端查询股本信息
                    query = f"""
                        SELECT shares_outstanding 
                        FROM {self.db.db_path}.stock_list 
                        WHERE ticker = %(ticker)s
                    """
                    result = thread_client.query_df(query, parameters={'ticker': ticker})
                    shares_outstanding = float(result['shares_outstanding'].iloc[0]) if not result.empty else 0.0

                    # 计算市值
                    if shares_outstanding > 0:
                        hist['Market Cap'] = hist['Close'].astype(float) * float(shares_outstanding)
                    else:
                        hist['Market Cap'] = 0.0

                    # ⚡ 使用线程独立客户端保存数据
                    df_copy = hist.copy()
                    df_copy.reset_index(inplace=True)
                    df_copy.columns = df_copy.columns.str.lower()

                    if 'market cap' in df_copy.columns:
                        df_copy['market_cap'] = df_copy['market cap'].fillna(0.0).astype(float)
                        df_copy.drop(columns=['market cap'], inplace=True)
                    elif 'market_cap' not in df_copy.columns:
                        df_copy['market_cap'] = 0.0

                    df_copy['market_cap'] = df_copy['market_cap'].fillna(0.0).astype(float)

                    data_to_insert = df_copy[
                        ['date', 'open', 'high', 'low', 'close', 'volume', 'market_cap']].values.tolist()

                    for row_data in data_to_insert:
                        row_data.insert(0, ticker)

                    columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'market_cap']

                    thread_client.insert(f'{self.db.db_path}.daily_kline', data_to_insert, column_names=columns)
                    saved_rows = len(data_to_insert)

                    if saved_rows > 0:
                        days_added = (pd.to_datetime(latest_trading_day) - last_date).days
                        return {
                            'status': 'success',
                            'ticker': ticker,
                            'rows': saved_rows,
                            'days': days_added
                        }
                    else:
                        return {'status': 'failed', 'ticker': ticker, 'error': '保存失败'}

                except Exception as e:
                    return {'status': 'error', 'ticker': ticker, 'error': str(e)[:100]}
                finally:
                    # 关闭线程客户端连接
                    try:
                        thread_client.close()
                    except:
                        pass

            # 使用线程池并发处理
            start_time = time.time()

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_row = {
                    executor.submit(update_single_stock, row): row
                    for _, row in needs_update.iterrows()
                }

                # 实时显示进度
                completed = 0
                for future in as_completed(future_to_row):
                    completed += 1
                    result = future.result()

                    with progress_lock:
                        if result['status'] == 'success':
                            success_count += 1
                            print(f"\r✅ [{completed}/{len(needs_update)}] {result['ticker']} "
                                  f"补充{result['rows']}条 (落后{result['days']}天) | 成功:{success_count}",
                                  end='', flush=True)
                        elif result['status'] == 'error':
                            failed_list.append({
                                'ticker': result['ticker'],
                                'error': result.get('error', '未知错误')
                            })
                            print(f"\r❌ [{completed}/{len(needs_update)}] {result['ticker']} 失败",
                                  end='', flush=True)
                        elif result['status'] == 'no_data':
                            print(f"\r⚠️  [{completed}/{len(needs_update)}] {result['ticker']} 无新数据",
                                  end='', flush=True)

            elapsed_time = time.time() - start_time

            # 5. 更新指数数据
            print("\n\n📊 检查指数数据...")
            self._update_indices(latest_trading_day)

            # 6. 输出统计结果
            print(f"\n\n{'=' * 60}")
            print("🎉 增量更新完成！")
            print(f"{'=' * 60}")
            print(f"✅ 成功更新: {success_count}/{len(needs_update)}")
            print(f"❌ 更新失败: {len(failed_list)}")
            print(f"⏱️  耗时: {elapsed_time:.1f} 秒")
            print(f"⚡ 平均速度: {len(needs_update) / elapsed_time:.1f} 只/秒")

            if failed_list:
                print(f"\n失败列表 (前10个):")
                for item in failed_list[:10]:
                    print(f"   {item['ticker']}: {item['error'][:50]}")

                # 保存失败列表
                log_file = f"update_failed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write("更新失败股票列表\n")
                    f.write("=" * 60 + "\n\n")
                    for item in failed_list:
                        f.write(f"{item['ticker']}: {item['error']}\n")
                print(f"📝 完整失败列表已保存到: {log_file}")

            return {
                'status': 'updated',
                'updated': success_count,
                'failed': len(failed_list),
                'elapsed_time': elapsed_time,
                'failed_list': failed_list
            }

        except KeyboardInterrupt:
            print(f"\n\n⚠️  用户中断！")
            return {'status': 'interrupted'}

        except Exception as e:
            print(f"❌ 增量更新失败: {e}")
            import traceback
            traceback.print_exc()
            return {'status': 'error', 'message': str(e)}

    def _get_latest_trading_day(self, reference_date):
        """获取最新交易日（跳过周末）"""
        current = reference_date

        for _ in range(7):
            if current.weekday() < 5:  # 0=周一, 4=周五
                return current.strftime('%Y-%m-%d')
            current -= pd.Timedelta(days=1)

        return reference_date.strftime('%Y-%m-%d')

    def _update_indices(self, latest_trading_day):
        """更新核心指数数据"""
        index_list = INDEX_LIST

        updated_count = 0
        for ticker in index_list:
            try:
                query = f"""
                    SELECT MAX(date) as last_date
                    FROM {self.db.db_path}.index_daily_kline
                    WHERE ticker = %(ticker)s
                """
                result = self.db.client.query_df(query, parameters={'ticker': ticker})

                if result.empty or pd.isna(result['last_date'].iloc[0]):
                    last_date='2006-1-1'
                else:
                    last_date = pd.to_datetime(result['last_date'].iloc[0])

                if last_date >= pd.to_datetime(latest_trading_day):
                    continue

                code = ticker.split('.')[0]
                market = ticker.split('.')[1]

                if market == 'SS':
                    hist = ak.stock_zh_index_daily(symbol=f"sh{code}")
                elif market == 'SZ':
                    hist = ak.stock_zh_index_daily(symbol=f"sz{code}")
                else:
                    continue

                if hist is None or hist.empty:
                    continue

                column_mapping = {
                    'date': 'Date',
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume'
                }
                hist.rename(columns=column_mapping, inplace=True)
                hist['Date'] = pd.to_datetime(hist['Date'])
                hist = hist[hist['Date'] > last_date]

                if not hist.empty:
                    hist.set_index('Date', inplace=True)
                    saved = self.db.save_index_daily_data(ticker, hist)
                    if saved > 0:
                        updated_count += 1
                        print(f"   ✅ {ticker} 补充 {saved} 条")

                time.sleep(0.2)

            except Exception as e:
                print(f"   ⚠️ {ticker} 失败: {e}")
                continue

        if updated_count == 0:
            print("   ✅ 指数数据已是最新")


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

    def check_buy_signal(self,data_daily):
        """买入信号检查"""
        # JQ: 排除ST, 上市>120天 (假设外部已处理或数据长度已隐含)
        if len(data_daily) < 105:
            return False, "数据不足"

        prev_data = data_daily.iloc[:-1]
        current_row = data_daily.iloc[-1]
        pre_row = data_daily.iloc[-2]

        if current_row['Close']/pre_row['Close'] -1>0.03 :
            return False, "当日涨幅大于2个点"
        # 1. 100天创新高 (JQ: past_high = close_df.max())
        # 注意：JQ是用过去100天(不含今日)的Close最大值
        past_100_high = prev_data['Close'].iloc[-100:].max()
        if current_row['Close'] <= past_100_high:
            return False, "未创新高"

        # 2. 检查阴线长上影 (JQ: 条件6)
        if self.check_shadow_line(current_row):
            return False, "长上影线"

        # 条件3：近期10天无涨停 (不含今日)
        recent_15_pct = prev_data['Close'].iloc[-10:].pct_change()
        if (recent_15_pct > 0.098).any():
            return False, "近期有涨停"

        # 4. TD九转检查 (JQ: 条件4 & 5)
        td_seq = self.calc_td_sequence(data_daily['Close'].values)
        curr_td = td_seq[-1]
        # JQ: abs(curr_td) == 8 or abs(curr_td) == 9 continue
        if abs(curr_td)== 8:
            return False, "TD接近9转"
        #
        # # JQ: sum(abs(recent_td) == 9) >= 2 continue
        # if np.sum(np.abs(td_seq[-30:-1]) == 9) >= 2:
        #     return False, "近期多次9转"

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
                should_sell, reason, sell_price = self.check_sell_signal(
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
                should_buy, _ = self.check_buy_signal(data_window)

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

def main():
    import sys
    interactive = sys.stdin.isatty()

    # 非交互模式自动下载
    if not interactive:
        choice = '1'
        batch_size = 100
    else:
        print("=" * 60)
        print("     A股量化交易系统 (自动断点续传版)")
        print("=" * 60)
        print("\n请选择功能：")
        print("1. 下载/补全 A股历史数据")
        print("2. 运行历史回测")
        print("3. 更新数据")
        print("0. 退出")
        choice = input("\n请输入选项 (0-3): ").strip()

        if choice == '1':
            batch_str = input("每批下载数量 (默认100): ").strip()
            batch_size = int(batch_str) if batch_str.isdigit() else 100
        else:
            batch_size = 100

    db = StockDatabase(DB_PATH)
    collector = DataCollector(db)

    try:
        if choice == '1':
            print(f"\n📥 开始下载数据（批次大小: {batch_size}）...")
            collector.download_all_stocks_data(DATA_START_DATE, batch_size=batch_size)
            collector.download_all_indexs_data(DATA_START_DATE)
            collector.download_financial_data(DATA_START_DATE)
            print("\n✅ 数据下载完成！")
        elif choice == '2':
            backtester = Backtester(db, INITIAL_CASH)
            backtester.run_backtest(BACKTEST_START, BACKTEST_END)
        elif choice == '3':
            collector.incremental_update()
            print("✅ 数据更新完成！")
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
