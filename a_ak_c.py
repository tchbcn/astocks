import akshare as ak
import pandas as pd
import numpy as np
import time,requests
import logging, warnings
from datetime import datetime
import clickhouse_connect
import tushare as ts
from clickhouse_connect.driver.client import Client
import sqlite3
import os

# 数据库配置
USE_CLICKHOUSE = False  # 设置为 True 启用 ClickHouse，False 使用 SQLite
CH_HOST = 'localhost'
CH_PORT = 8123
CH_USER = 'default'
CH_PASSWORD = ''
CH_DATABASE = 'stock_db'
DB_PATH = 'stock_data.db'  # SQLite 数据库文件路径

INITIAL_CASH = 100000
DATA_START_DATE = '2023-05-01'
BACKTEST_START = '2024-01-01'
BACKTEST_END = '2025-12-11'
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
warnings.simplefilter(action='ignore', category=FutureWarning)

class SQLiteDatabase:
    """SQLite数据库管理类（后备方案）"""

    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.init_database()

    def init_database(self):
        """初始化SQLite数据库和表结构"""
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()

        # 股票列表表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_list (
                ticker TEXT PRIMARY KEY,
                name TEXT,
                short_name TEXT,
                market TEXT,
                market_cap REAL,
                shares_outstanding REAL,
                total_shares REAL,
                is_st INTEGER,
                last_update TEXT
            )
        ''')

        # 日K线数据表
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

        # 财务数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_data (
                ticker TEXT,
                report_date TEXT,
                ann_date TEXT,
                total_shares REAL,
                eps REAL,
                update_time TEXT,
                PRIMARY KEY (ticker, report_date)
            )
        ''')

        # 分钟K线数据表
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

        # 指数日K线数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS index_daily_kline (
                ticker TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                amount REAL,
                PRIMARY KEY (ticker, date)
            )
        ''')

        self.conn.commit()
        print(f"✅ SQLite 数据库 '{self.db_path}' 初始化完成")

    def execute(self, query, params=None, fetch=False):
        """执行SQL查询"""
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        if fetch:
            return cursor.fetchall()
        else:
            self.conn.commit()
            return cursor.lastrowid

    def insert_or_replace(self, table, data_dict):
        """插入或替换数据"""
        if not data_dict:
            return
        keys = data_dict.keys()
        columns = ', '.join(keys)
        placeholders = ', '.join(['?' for _ in keys])
        query = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
        values = tuple(data_dict.values())
        self.execute(query, values)

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()

class StockDatabase:
    """股票数据库管理类 (支持 ClickHouse 和 SQLite 双后端)"""

    def __init__(self, db_path):
        self.db_path = db_path
        self.client = None
        self.sqlite_db = None
        self.use_clickhouse = USE_CLICKHOUSE
        self.init_database()

    def init_database(self):
        """初始化数据库连接和表结构"""
        if self.use_clickhouse:
            try:
                # 尝试连接 ClickHouse
                self.client = clickhouse_connect.get_client(
                    host=CH_HOST,
                    port=CH_PORT,
                    username=CH_USER,
                    password=CH_PASSWORD,
                    database='default'
                )
                # 测试连接
                self.client.command('SELECT 1')
                # 创建目标数据库
                self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.db_path}")
                print(f"✅ ClickHouse 数据库 '{self.db_path}' 连接成功")
                self._init_clickhouse_tables()
            except Exception as e:
                print(f"⚠️  ClickHouse 连接失败: {e}")
                print("   回退到 SQLite 模式")
                self.use_clickhouse = False
                self.sqlite_db = SQLiteDatabase(self.db_path)
        else:
            self.sqlite_db = SQLiteDatabase(self.db_path)

    def _init_clickhouse_tables(self):
        """初始化 ClickHouse 表结构"""
        # 股票列表表
        self.client.command(f'''
            CREATE TABLE IF NOT EXISTS {self.db_path}.stock_list (
                ticker String,
                name String,
                short_name String,
                market String,
                market_cap Float64,
                shares_outstanding Float64,
                total_shares Float64,
                is_st UInt8,
                last_update DateTime
            ) ENGINE = ReplacingMergeTree
            ORDER BY ticker
        ''')

        # 日K线数据表
        self.client.command(f'''
            CREATE TABLE IF NOT EXISTS {self.db_path}.daily_kline (
                ticker String,
                date Date,
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                volume Int64,
                market_cap Float64
            ) ENGINE = ReplacingMergeTree
            ORDER BY (ticker, date)
            PRIMARY KEY (ticker, date)
        ''')

        # 财务数据表
        self.client.command(f'''
            CREATE TABLE IF NOT EXISTS {self.db_path}.financial_data (
                ticker String,
                report_date Date,
                ann_date Date,
                total_shares Float64,
                eps Float64,
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

        # 指数日K线数据表
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

    def execute(self, query, params=None, fetch=False):
        """执行SQL查询（统一接口）"""
        if self.use_clickhouse:
            if params:
                result = self.client.command(query, params=params)
            else:
                result = self.client.command(query)
            if fetch:
                return result.result_rows
            return result
        else:
            return self.sqlite_db.execute(query, params, fetch)

    def insert_or_replace(self, table, data_dict):
        """插入或替换数据（统一接口）"""
        if not data_dict:
            return
        if self.use_clickhouse:
            # ClickHouse 使用 INSERT 语句
            columns = list(data_dict.keys())
            values = [data_dict[k] for k in columns]
            placeholders = ', '.join(['{}' for _ in columns])
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
            self.execute(query, values)
        else:
            self.sqlite_db.insert_or_replace(table, data_dict)

    def close(self):
        """关闭连接"""
        if self.client:
            self.client.close()
        if self.sqlite_db:
            self.sqlite_db.close()

    def get_all_a_stocks(self):
        """获取所有A股代码及核心指数"""
        print("🔍 正在从 AkShare 获取股票列表...")

        try:
            # 获取沪深A股实时行情
            stock_zh_a_spot_em_df = ak.stock_info_a_code_name()
            # 统一 ticker 格式: 代码.市场
            stock_zh_a_spot_em_df['ticker'] = stock_zh_a_spot_em_df['code'].apply(
                lambda x: f"{x}.{'SH' if x.startswith('6') else 'SZ'}"
            )
            # 只保留必要的列，避免 SQLite 类型不匹配
            stock_list_df = stock_zh_a_spot_em_df[['ticker', 'name']].copy()
            stock_list_df['market'] = stock_list_df['ticker'].apply(
                lambda x: 'SH' if '.SH' in x else 'SZ'
            )
            stock_list_df['short_name'] = stock_list_df['name']
            stock_list_df['market_cap'] = None
            stock_list_df['shares_outstanding'] = None
            stock_list_df['total_shares'] = None
            stock_list_df['is_st'] = 0
            stock_list_df['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 插入数据库
            for _, row in stock_list_df.iterrows():
                self.insert_or_replace('stock_list', row.to_dict())
            print(f"✅ 股票列表已更新，共 {len(stock_list_df)} 只 A 股")

        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            raise

    def download_stock_daily_data(self, ticker, start_date, end_date):
        """下载单只股票日K线数据"""
        # 解析 ticker，如 "000001.SZ" -> code="000001", market="SZ"
        code = ticker.split('.')[0]
        market_suffix = ticker.split('.')[1]

        # 映射 market_suffix 到 akshare 参数
        period = "daily"
        adjust = "qfq"

        try:
            # 下载数据
            df = ak.stock_zh_a_hist(symbol=code, period=period, start_date=start_date, end_date=end_date, adjust=adjust)
            if df is None or df.empty:
                print(f"  ⚠️  {ticker}: 无数据")
                return 0

            # 标准化列名
            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turnover'
            })
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df['ticker'] = ticker

            # 计算市值（如果可能）
            if 'volume' in df.columns and 'close' in df.columns:
                df['market_cap'] = df['close'] * df['volume']  # 简化估算
            else:
                df['market_cap'] = None

            # 插入数据库
            count = 0
            for _, row in df.iterrows():
                data = {
                    'ticker': row['ticker'],
                    'date': row['date'],
                    'open': float(row['open']) if pd.notnull(row['open']) else None,
                    'high': float(row['high']) if pd.notnull(row['high']) else None,
                    'low': float(row['low']) if pd.notnull(row['low']) else None,
                    'close': float(row['close']) if pd.notnull(row['close']) else None,
                    'volume': int(row['volume']) if pd.notnull(row['volume']) else None,
                    'market_cap': float(row['market_cap']) if pd.notnull(row['market_cap']) else None
                }
                self.insert_or_replace('daily_kline', data)
                count += 1
            return count
        except Exception as e:
            print(f"  ❌ {ticker} 下载失败: {e}")
            return 0

    def download_index_daily_data(self, ticker, start_date, end_date):
        """下载指数日K线数据"""
        # ticker 格式: 指数代码.SS/SZ
        code = ticker.split('.')[0]

        try:
            df = ak.stock_zh_index_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                print(f"  ⚠️  {ticker}: 无数据")
                return 0

            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turnover'
            })
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df['ticker'] = ticker

            count = 0
            for _, row in df.iterrows():
                data = {
                    'ticker': ticker,
                    'date': row['date'],
                    'open': float(row['open']) if pd.notnull(row['open']) else None,
                    'high': float(row['high']) if pd.notnull(row['high']) else None,
                    'low': float(row['low']) if pd.notnull(row['low']) else None,
                    'close': float(row['close']) if pd.notnull(row['close']) else None,
                    'volume': int(row['volume']) if pd.notnull(row['volume']) else None,
                    'amount': float(row['amount']) if pd.notnull(row['amount']) else None
                }
                self.insert_or_replace('index_daily_kline', data)
                count += 1
            return count
        except Exception as e:
            print(f"  ❌ {ticker} 下载失败: {e}")
            return 0

    def get_stock_daily_data(self, ticker, start_date, end_date):
        """从数据库获取日K线数据"""
        if self.use_clickhouse:
            query = f"""
                SELECT * FROM {self.db_path}.daily_kline
                WHERE ticker = %(ticker)s
                  AND date >= %(start_date)s
                  AND date <= %(end_date)s
                ORDER BY date
            """
            params = {'ticker': ticker, 'start_date': start_date, 'end_date': end_date}
            result = self.execute(query, params=params, fetch=True)
            df = pd.DataFrame(result, columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'market_cap'])
        else:
            query = """
                SELECT * FROM daily_kline
                WHERE ticker = ? AND date >= ? AND date <= ?
                ORDER BY date
            """
            params = (ticker, start_date, end_date)
            result = self.execute(query, params=params, fetch=True)
            df = pd.DataFrame(result, columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'market_cap'])
        return df

    def get_index_data(self, ticker, start_date, end_date):
        """从数据库获取指数数据"""
        if self.use_clickhouse:
            query = f"""
                SELECT * FROM {self.db_path}.index_daily_kline
                WHERE ticker = %(ticker)s
                  AND date >= %(start_date)s
                  AND date <= %(end_date)s
                ORDER BY date
            """
            params = {'ticker': ticker, 'start_date': start_date, 'end_date': end_date}
            result = self.execute(query, params=params, fetch=True)
            df = pd.DataFrame(result, columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        else:
            query = """
                SELECT * FROM index_daily_kline
                WHERE ticker = ? AND date >= ? AND date <= ?
                ORDER BY date
            """
            params = (ticker, start_date, end_date)
            result = self.execute(query, params=params, fetch=True)
            df = pd.DataFrame(result, columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        return df

    def get_financial_data(self, ticker):
        """从数据库获取财务数据"""
        if self.use_clickhouse:
            query = f"""
                SELECT * FROM {self.db_path}.financial_data
                WHERE ticker = %(ticker)s
                ORDER BY report_date DESC
            """
            params = {'ticker': ticker}
            result = self.execute(query, params=params, fetch=True)
            df = pd.DataFrame(result, columns=['ticker', 'report_date', 'ann_date', 'total_shares', 'eps', 'update_time'])
        else:
            query = """
                SELECT * FROM financial_data
                WHERE ticker = ?
                ORDER BY report_date DESC
            """
            params = (ticker,)
            result = self.execute(query, params=params, fetch=True)
            df = pd.DataFrame(result, columns=['ticker', 'report_date', 'ann_date', 'total_shares', 'eps', 'update_time'])
        return df

    def get_stock_list(self):
        """获取股票列表"""
        if self.use_clickhouse:
            query = f"SELECT * FROM {self.db_path}.stock_list"
            result = self.execute(query, fetch=True)
            df = pd.DataFrame(result, columns=['ticker', 'name', 'short_name', 'market', 'market_cap', 'shares_outstanding', 'total_shares', 'is_st', 'last_update'])
        else:
            query = "SELECT * FROM stock_list"
            result = self.execute(query, fetch=True)
            df = pd.DataFrame(result, columns=['ticker', 'name', 'short_name', 'market', 'market_cap', 'shares_outstanding', 'total_shares', 'is_st', 'last_update'])
        return df

    def batch_insert_daily_data(self, data_list):
        """批量插入日K线数据（用于断点续传）"""
        for data in data_list:
            self.insert_or_replace('daily_kline', data)

    def batch_insert_financial_data(self, data_list):
        """批量插入财务数据"""
        for data in data_list:
            self.insert_or_replace('financial_data', data)

    def batch_insert_index_data(self, data_list):
        """批量插入指数数据"""
        for data in data_list:
            self.insert_or_replace('index_daily_kline', data)

def main():
    """主函数：数据管理"""
    print("=" * 70)
    print("     A股量化交易系统 (自动断点续传版)")
    print("=" * 70)

    db = StockDatabase(DB_PATH)

    # 检测是否在非交互式环境
    import sys
    interactive = sys.stdin.isatty()

    while True:
        if interactive:
            print("\n请选择功能：")
            print("1. 下载/补全 A股历史数据")
            print("2. 运行历史回测")
            print("3. 更新数据")
            print("0. 退出")
            choice = input("请输入选项 (0-3): ").strip()
        else:
            # 非交互式默认执行下载
            choice = '1'

        if choice == '0':
            print("再见！")
            break

        if choice == '0':
            print("再见！")
            break

        elif choice == '1':
            # 下载/补全数据
            print("\n📥 开始下载 A股历史数据...")
            print(f"数据源: {'ClickHouse' if db.use_clickhouse else 'SQLite'}")
            print(f"存储路径: {DB_PATH}")

            # 获取股票列表
            stock_list_df = db.get_stock_list()
            if stock_list_df.empty:
                print("📋 股票列表为空，正在从 AkShare 获取...")
                try:
                    db.get_all_a_stocks()
                    stock_list_df = db.get_stock_list()
                    print(f"✅ 股票列表已更新，共 {len(stock_list_df)} 只 A股")
                except Exception as e:
                    print(f"❌ 获取股票列表失败: {e}")
                    continue
            else:
                print(f"✅ 股票列表已存在，共 {len(stock_list_df)} 只 A股")
            tickers = stock_list_df['ticker'].tolist()
            print(f"📋 共有 {len(tickers)} 只 A股需要处理")

            # 设置下载时间范围
            start_date = DATA_START_DATE.replace('-', '')
            end_date = datetime.now().strftime('%Y%m%d')
            print(f"📅 下载时间范围: {start_date} - {end_date}")

            # 断点续传：查询已下载的股票
            if db.use_clickhouse:
                query = f"SELECT DISTINCT ticker FROM {DB_PATH}.daily_kline"
                existing_tickers = set([row[0] for row in db.execute(query, fetch=True)])
            else:
                query = "SELECT DISTINCT ticker FROM daily_kline"
                existing_tickers = set([row[0] for row in db.execute(query, fetch=True)])

            need_download = [t for t in tickers if t not in existing_tickers]
            print(f"⏳ 需要下载 {len(need_download)} 只股票（已下载 {len(tickers)-len(need_download)} 只）")

            if len(need_download) == 0:
                print("✅ 所有股票数据已完整")
                continue

            # 确认下载（交互模式下需要确认，非交互模式自动确认）
            if interactive:
                confirm = input(f"\n确认下载 {len(need_download)} 只股票的数据吗？(y/N): ").strip().lower()
                if confirm != 'y':
                    print("取消下载")
                    continue
            else:
                print(f"\n自动下载 {len(need_download)} 只股票的数据...")

            # 开始下载
            total_downloaded = 0
            batch_size = 50  # 每批处理50只
            for i in range(0, len(need_download), batch_size):
                batch = need_download[i:i+batch_size]
                print(f"\n📦 正在下载批次 {i//batch_size+1}/{(len(need_download)-1)//batch_size+1} (共 {len(batch)} 只)...")
                batch_data = []
                for idx, ticker in enumerate(batch, 1):
                    try:
                        count = db.download_stock_daily_data(ticker, start_date, end_date)
                        total_downloaded += count
                        if count > 0:
                            print(f"  [{i+idx:4d}/{len(need_download)}] {ticker}: {count} 条记录")
                        else:
                            print(f"  [{i+idx:4d}/{len(need_download)}] {ticker}: 无数据")
                    except Exception as e:
                        print(f"  [{i+idx:4d}/{len(need_download)}] {ticker}: ❌ {e}")

                print(f"  批次完成，累计下载 {total_downloaded} 条记录")

                # 每批次后可以暂停（仅交互模式）
                if interactive and i + batch_size < len(need_download):
                    pause = input("按 Enter 继续下一批，或输入 'q' 退出: ").strip().lower()
                    if pause == 'q':
                        print("中断下载")
                        break
                elif not interactive and i + batch_size < len(need_download):
                    # 非交互模式自动继续
                    pass

            print(f"\n✅ 数据下载完成！共下载 {total_downloaded} 条日K记录")

            # 下载指数数据
            print("\n📥 开始下载指数数据...")
            for idx, index_ticker in enumerate(INDEX_LIST, 1):
                try:
                    count = db.download_index_daily_data(index_ticker, start_date, end_date)
                    print(f"  [{idx}/{len(INDEX_LIST)}] {index_ticker}: {count} 条记录")
                except Exception as e:
                    print(f"  [{idx}/{len(INDEX_LIST)}] {index_ticker}: ❌ {e}")

            print("\n✅ 所有数据下载完成！")

        elif choice == '2':
            # 运行历史回测
            print("\n🔄 运行历史回测")
            print("请直接运行策略文件，例如: python a_mul.py")
            print("或使用主菜单: python main.py")

        elif choice == '3':
            # 更新数据
            print("\n🔄 更新数据")
            print("请运行策略文件，选择更新选项")
            print("或使用主菜单: python main.py")

    db.close()

if __name__ == '__main__':
    main()
