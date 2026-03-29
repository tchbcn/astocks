import time
import traceback
import asyncio
import math
import json
import hmac
import hashlib
import logging
import aiohttp
from decimal import Decimal, ROUND_DOWN

# ==================== Binance API 配置 ====================
API_KEY = 'jZjxyIx0A88IiDgXukOb2EZamNKY0vWrUIzYmeWyPQ3UrUhNqVz5dAM4FhAvffcQ'  # 替换为你的 Binance API Key
API_SECRET = 'sBjHnNUBUYiLdFBPBgWRcIER88OlBk5BNtwa1CTuKnUcaopwesbyaxFP9jn0Amug'  # 替换为你的 Binance API Secret
SYMBOL = 'PIPPINUSDT'  # 交易对

# 使用 binance.me 镜像站点
USE_PROXY_DOMAIN = True
if USE_PROXY_DOMAIN:
    BASE_URL = 'https://fapi.binance.me'  # 合约API
    WS_URL = 'wss://fstream.binance.me/ws'
else:
    BASE_URL = 'https://fapi.binance.com'
    WS_URL = 'wss://fstream.binance.com/ws'

# ==================== 交易参数 ====================
TARGET_ORDER_USDT = 10
LEVERAGE = 10
TIMEFRAME = '1m'
BB_PERIOD = 20
BB_STD = 2
EMA_PERIOD = 10
MAX_HOLD_TIME_SECONDS = 300
MinProfitRate = 0.00050
DECAY_RATE_FACTOR = 2
ENTRY_DELAY_SECONDS = 0
MONITOR_INTERVAL = 0.15
COOLDOWN_AFTER_CLOSE_SECONDS = 0
TP_UPDATE_INTERVAL_SECONDS = 10
VOL_SMA_PERIOD = 20
VOL_SPIKE_THRESHOLD = 1.2
OB_DEPTH_LIMIT = 20
OB_IMBALANCE_THRESHOLD = 0.8
std_rate = 0.618
WICK_BODY_RATIO = 2
MIN_WICK_LEN_FACTOR = 0.3

# ==================== 全局变量 ====================
LIVE_ORDER_BOOK_CACHE = {'bids': {}, 'asks': {}}
ORDER_BOOK_LOCK = asyncio.Lock()
LIVE_POSITION_CACHE = {'size': 0.0, 'entry_price': 0.0}
POSITION_LOCK = asyncio.Lock()
LIVE_CANDLE_CACHE = [0, 0, 0, 0, 0, 0.0]
CACHE_LOCK = asyncio.Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

current_position = None
initial_tp = 0
last_close_time = 0
contract_details = None
price_precision = 4
quantity_precision = 3
trade_history = []
cumulative_pnl = 0.0
total_trades = 0
win_count = 0
loss_count = 0
total_win_pnl = 0.0
total_loss_pnl = 0.0


# ==================== Binance API 辅助函数 ====================
def generate_signature(params, secret):
    """生成 Binance API 签名"""
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    signature = hmac.new(
        secret.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    return signature


async def binance_request(method, endpoint, params=None, signed=False):
    """通用 Binance API 请求函数"""
    url = f"{BASE_URL}{endpoint}"

    # 添加完整的浏览器请求头
    headers = {
        'X-MBX-APIKEY': API_KEY,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }

    if params is None:
        params = {}

    if signed:
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = generate_signature(params, API_SECRET)

    # 创建 SSL 上下文
    ssl_context = None
    if USE_PROXY_DOMAIN:
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(ssl=ssl_context)

    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            if method == 'GET':
                async with session.get(url, headers=headers, params=params,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    text = await resp.text()
                    try:
                        return json.loads(text)
                    except:
                        logging.error(f"❌ 无法解析响应: {text[:200]}")
                        raise
            elif method == 'POST':
                async with session.post(url, headers=headers, params=params,
                                        timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    text = await resp.text()
                    try:
                        return json.loads(text)
                    except:
                        logging.error(f"❌ 无法解析响应: {text[:200]}")
                        raise
            elif method == 'DELETE':
                async with session.delete(url, headers=headers, params=params,
                                          timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    text = await resp.text()
                    try:
                        return json.loads(text)
                    except:
                        logging.error(f"❌ 无法解析响应: {text[:200]}")
                        raise
        except Exception as e:
            logging.error(f"❌ API 请求失败 [{method} {endpoint}]: {e}")
            raise
async def get_server_time():
    """获取服务器时间"""
    try:
        data = await binance_request('GET', '/fapi/v1/time')
        return data['serverTime']
    except Exception as e:
        logging.error(f"获取服务器时间失败: {e}")
        return int(time.time() * 1000)


async def set_leverage_async(leverage):
    """设置杠杆"""
    try:
        params = {'symbol': SYMBOL, 'leverage': leverage}
        result = await binance_request('POST', '/fapi/v1/leverage', params, signed=True)
        logging.info(f"✅ 杠杆已设置为: {leverage}x")
        return result
    except Exception as e:
        logging.error(f"❌ 设置杠杆失败: {e}")


async def set_margin_type(margin_type='CROSSED'):
    """设置保证金模式"""
    try:
        params = {'symbol': SYMBOL, 'marginType': margin_type}
        result = await binance_request('POST', '/fapi/v1/marginType', params, signed=True)
        logging.info(f"✅ 保证金模式已设置为: {margin_type}")
        return result
    except Exception as e:
        # 如果已经是该模式，会返回错误，可以忽略
        logging.warning(f"⚠️ 设置保证金模式: {e}")


async def get_exchange_info():
    """获取交易对信息"""
    global price_precision, quantity_precision
    try:
        data = await binance_request('GET', '/fapi/v1/exchangeInfo')
        for symbol_info in data['symbols']:
            if symbol_info['symbol'] == SYMBOL:
                # 获取价格和数量精度
                for filter in symbol_info['filters']:
                    if filter['filterType'] == 'PRICE_FILTER':
                        tick_size = float(filter['tickSize'])
                        price_precision = len(str(tick_size).rstrip('0').split('.')[-1])
                    elif filter['filterType'] == 'LOT_SIZE':
                        step_size = float(filter['stepSize'])
                        quantity_precision = len(str(step_size).rstrip('0').split('.')[-1])

                logging.info(f"⚙️ {SYMBOL} 价格精度: {price_precision}, 数量精度: {quantity_precision}")
                return symbol_info
        return None
    except Exception as e:
        logging.error(f"❌ 获取交易对信息失败: {e}")
        return None


def format_price(price):
    """格式化价格"""
    return float(Decimal(str(price)).quantize(Decimal(10) ** -price_precision, rounding=ROUND_DOWN))


def format_quantity(quantity):
    """格式化数量"""
    return float(Decimal(str(quantity)).quantize(Decimal(10) ** -quantity_precision, rounding=ROUND_DOWN))


async def get_klines(limit=300):
    """获取K线数据"""
    try:
        params = {
            'symbol': SYMBOL,
            'interval': TIMEFRAME,
            'limit': limit
        }
        data = await binance_request('GET', '/fapi/v1/klines', params)

        ohlcv = []
        for k in data:
            # [时间, 开, 高, 低, 收, 成交量]
            ohlcv.append([
                int(k[0]) // 1000,  # 转换为秒
                float(k[1]),  # open
                float(k[2]),  # high
                float(k[3]),  # low
                float(k[4]),  # close
                float(k[5])  # volume
            ])
        return ohlcv
    except Exception as e:
        logging.error(f"❌ 获取K线失败: {e}")
        return []


async def get_position():
    """获取当前持仓"""
    try:
        params = {'symbol': SYMBOL}
        positions = await binance_request('GET', '/fapi/v2/positionRisk', params, signed=True)

        for pos in positions:
            if pos['symbol'] == SYMBOL:
                size = float(pos['positionAmt'])
                entry_price = float(pos['entryPrice'])

                async with POSITION_LOCK:
                    LIVE_POSITION_CACHE['size'] = size
                    LIVE_POSITION_CACHE['entry_price'] = entry_price

                return {'size': size, 'entry_price': entry_price}

        return {'size': 0.0, 'entry_price': 0.0}
    except Exception as e:
        logging.error(f"❌ 获取持仓失败: {e}")
        return {'size': 0.0, 'entry_price': 0.0}


async def cancel_all_orders():
    """取消所有订单"""
    try:
        params = {'symbol': SYMBOL}
        result = await binance_request('DELETE', '/fapi/v1/allOpenOrders', params, signed=True)
        logging.info("🧹 已清理所有挂单")
        return result
    except Exception as e:
        logging.error(f"❌ 取消订单失败: {e}")


async def place_order(side, quantity, price=None, order_type='LIMIT', time_in_force='GTC', reduce_only=False):
    """下单"""
    try:
        params = {
            'symbol': SYMBOL,
            'side': side.upper(),
            'type': order_type,
            'quantity': format_quantity(abs(quantity))
        }

        if order_type == 'LIMIT':
            params['price'] = format_price(price)
            params['timeInForce'] = time_in_force

        if reduce_only:
            params['reduceOnly'] = 'true'

        result = await binance_request('POST', '/fapi/v1/order', params, signed=True)
        logging.info(f"✅ 订单已提交: {result['orderId']}")
        return result
    except Exception as e:
        logging.error(f"❌ 下单失败: {e}")
        return None


async def get_order_status(order_id):
    """查询订单状态"""
    try:
        params = {'symbol': SYMBOL, 'orderId': order_id}
        result = await binance_request('GET', '/fapi/v1/order', params, signed=True)
        return result
    except Exception as e:
        logging.error(f"❌ 查询订单失败: {e}")
        return None


# ==================== WebSocket 数据流 ====================
async def price_data_streamer():
    """WebSocket 价格和订单簿数据流"""
    global LIVE_CANDLE_CACHE, LIVE_ORDER_BOOK_CACHE
    current_minute_start_time = 0

    # 构建 WebSocket 订阅流
    streams = [
        f"{SYMBOL.lower()}@aggTrade",  # 实时成交
        f"{SYMBOL.lower()}@depth20@100ms",  # 订单簿深度
        f"{SYMBOL.lower()}@kline_{TIMEFRAME}"  # K线
    ]

    ws_url = f"{WS_URL}/stream?streams={'/'.join(streams)}"

    # 创建 SSL 上下文
    ssl_context = None
    if USE_PROXY_DOMAIN:
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    while True:
        try:
            logging.info(f"🔗 正在连接 WebSocket: {ws_url}")

            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.ws_connect(ws_url) as ws:
                    logging.info(f"✅ WebSocket 已连接")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            stream = data.get('stream', '')
                            result = data.get('data', {})

                            # 处理实时成交更新价格
                            if 'aggTrade' in stream:
                                current_price = float(result['p'])
                                current_time = int(time.time())
                                new_start_time = (current_time // 60) * 60

                                async with CACHE_LOCK:
                                    if new_start_time != current_minute_start_time:
                                        LIVE_CANDLE_CACHE = [new_start_time, current_price, current_price,
                                                             current_price, current_price, 0.0]
                                        current_minute_start_time = new_start_time
                                    else:
                                        LIVE_CANDLE_CACHE[2] = max(LIVE_CANDLE_CACHE[2], current_price)
                                        LIVE_CANDLE_CACHE[3] = min(LIVE_CANDLE_CACHE[3], current_price)
                                        LIVE_CANDLE_CACHE[4] = current_price

                            # 处理订单簿
                            elif 'depth' in stream:
                                async with ORDER_BOOK_LOCK:
                                    LIVE_ORDER_BOOK_CACHE['bids'] = {float(b[0]): float(b[1]) for b in result['bids']}
                                    LIVE_ORDER_BOOK_CACHE['asks'] = {float(a[0]): float(a[1]) for a in result['asks']}

                            # 处理K线
                            elif 'kline' in stream:
                                k = result['k']
                                if k['x']:  # K线已收盘
                                    pass  # 可以在这里处理已收盘的K线

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logging.error(f"❌ WebSocket 错误")
                            break

        except Exception as e:
            logging.error(f"❌ WebSocket 连接错误: {e}")
            logging.info("🔄 5秒后重新连接...")
            await asyncio.sleep(5)


async def get_current_price_fast():
    """快速获取当前价格"""
    async with CACHE_LOCK:
        return LIVE_CANDLE_CACHE[4]


async def get_current_position_fast():
    """快速获取持仓"""
    async with POSITION_LOCK:
        return LIVE_POSITION_CACHE.copy()


async def get_depth_pressure_fast(limit=OB_DEPTH_LIMIT):
    """快速获取订单簿压力"""
    async with ORDER_BOOK_LOCK:
        bids_data = LIVE_ORDER_BOOK_CACHE['bids']
        asks_data = LIVE_ORDER_BOOK_CACHE['asks']

        sorted_bids = sorted(bids_data.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(asks_data.items(), key=lambda x: x[0], reverse=False)

        bids_vol = sum([size for price, size in sorted_bids[:limit]])
        asks_vol = sum([size for price, size in sorted_asks[:limit]])

    total_vol = bids_vol + asks_vol
    if total_vol == 0:
        return 0.5

    return bids_vol / total_vol


async def get_order_book_price_fast(signal_side, depth_level=3):
    """快速获取订单簿价格"""
    async with ORDER_BOOK_LOCK:
        if signal_side == 'buy':
            bids_data = LIVE_ORDER_BOOK_CACHE['bids']
            sorted_bids = sorted(bids_data.items(), key=lambda x: x[0], reverse=True)
            if len(sorted_bids) >= depth_level:
                return sorted_bids[depth_level - 1][0]
        else:
            asks_data = LIVE_ORDER_BOOK_CACHE['asks']
            sorted_asks = sorted(asks_data.items(), key=lambda x: x[0], reverse=False)
            if len(sorted_asks) >= depth_level:
                return sorted_asks[depth_level - 1][0]

    return None


async def get_live_candle():
    """获取实时K线"""
    async with CACHE_LOCK:
        return list(LIVE_CANDLE_CACHE)


# ==================== 策略指标计算 ====================
def calculate_ema_js_style(closes, period):
    """计算EMA"""
    if not closes:
        return 0
    k = 2 / (period + 1)
    ema = closes[0]
    for price in closes[1:]:
        ema = price * k + ema * (1 - k)
    return ema


def calculate_indicators_js_match(ohlcv_data):
    """计算技术指标"""
    closes = [x[4] for x in ohlcv_data]
    volumes = [x[5] for x in ohlcv_data]

    # Bollinger Bands
    bb_slice = closes[-BB_PERIOD:]
    ma = sum(bb_slice) / BB_PERIOD
    variance = sum([((x - ma) ** 2) for x in bb_slice]) / BB_PERIOD
    std_dev = math.sqrt(variance)
    upper = ma + (BB_STD * std_dev)
    lower = ma - (BB_STD * std_dev)

    # EMA
    ema = calculate_ema_js_style(closes, EMA_PERIOD)

    # Volume SMA
    vol_slice = volumes[-VOL_SMA_PERIOD:]
    vol_sma = sum(vol_slice) / VOL_SMA_PERIOD

    return ma, std_dev, upper, lower, ema, vol_sma


async def prepare_data_for_strategy(limit=300):
    """准备策略数据"""
    candles = await get_klines(limit)
    if not candles:
        return None

    live_candle = await get_live_candle()

    if live_candle[4] == 0:
        return None

    history_data = candles[:-1]
    combined_data = history_data + [live_candle]
    return combined_data


async def update_and_get_signal():
    """更新并获取交易信号"""
    data = await prepare_data_for_strategy(limit=300)
    if not data:
        return None, None, None

    ma, std_dev, upper, lower, ema, vol_sma = calculate_indicators_js_match(data)
    if ma is None:
        return None, None, None

    current_price = data[-1][4]
    prev_candle = data[-2]
    open_price = prev_candle[1]
    prev_vol = prev_candle[5]

    if std_dev / current_price > 0.01:
        return None, None, None

    EMA_TREND = 169
    ema169 = calculate_ema_js_style([x[4] for x in data[-EMA_TREND:]], EMA_TREND)

    ema_side = None
    if current_price > ema169 * 0.998:
        ema_side = 'buy'
    if current_price < ema169 * 1.002:
        ema_side = 'sell'

    last_dev = abs(prev_candle[2] - prev_candle[3]) * std_rate
    ob_ratio = await get_depth_pressure_fast()
    is_vol_active = prev_vol > (vol_sma * 0.5)

    # K线形态分析
    live_candle = await get_live_candle()
    o, h, l, c = live_candle[1], live_candle[2], live_candle[3], live_candle[4]

    body_size = abs(c - o)
    upper_wick = h - max(c, o)
    lower_wick = min(c, o) - l

    safe_body = body_size if body_size > 1e-10 else 1e-10
    upper_ratio = upper_wick / safe_body
    lower_ratio = lower_wick / safe_body

    candle_signal = None
    if upper_ratio > WICK_BODY_RATIO and upper_wick > (std_dev * MIN_WICK_LEN_FACTOR):
        candle_signal = 'sell'

    if lower_ratio > WICK_BODY_RATIO and lower_wick > (std_dev * MIN_WICK_LEN_FACTOR):
        candle_signal = 'buy'

    # 信号判断
    if current_price < (open_price - last_dev) and ema_side == 'buy':
        if ob_ratio >= OB_IMBALANCE_THRESHOLD and is_vol_active:
            if candle_signal is None or candle_signal == 'buy':
                logging.info(f"⚡ [强力买入] 价格超跌且盘口支撑强. Ratio:{ob_ratio:.2f}")
                return 'buy', std_dev, current_price

    elif current_price > (open_price + last_dev) and ema_side == 'sell':
        if ob_ratio <= (1 - OB_IMBALANCE_THRESHOLD) and is_vol_active:
            if candle_signal is None or candle_signal == 'sell':
                logging.info(f"⚡ [强力做空] 价格冲高且盘口压制强. Ratio:{ob_ratio:.2f}")
                return 'sell', std_dev, current_price

    return None, None, None


# ==================== 交易执行 ====================
def calculate_and_log_pnl(entry_price, exit_price, side, entry_time):
    """计算并记录PNL"""
    global cumulative_pnl, total_trades, win_count, loss_count
    global total_win_pnl, total_loss_pnl, COOLDOWN_AFTER_CLOSE_SECONDS

    direction = 1 if side == 'buy' else -1
    pnl_percent = direction * (exit_price - entry_price) / entry_price * LEVERAGE
    pnl_usdt = pnl_percent * TARGET_ORDER_USDT

    hold_time = time.time() - entry_time
    logging.info(f"💰 [本笔交易] PNL: {pnl_usdt:+.2f} USDT ({pnl_percent * 100:+.2f}%) | 持仓时间: {hold_time:.0f}s")

    trade_history.append(pnl_usdt)
    cumulative_pnl += pnl_usdt
    total_trades += 1

    if pnl_usdt > 0:
        win_count += 1
        total_win_pnl += pnl_usdt
        COOLDOWN_AFTER_CLOSE_SECONDS = 0
    else:
        loss_count += 1
        total_loss_pnl += abs(pnl_usdt)
        COOLDOWN_AFTER_CLOSE_SECONDS = 10

    win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
    odds_ratio = (total_win_pnl / win_count) / (total_loss_pnl / loss_count) if win_count > 0 and loss_count > 0 else 0

    logging.info(
        f"📈 [累计统计] 总笔数: {total_trades} | 累计PNL: {cumulative_pnl:+.2f} USDT | 胜率: {win_rate:.2f}% | 赔率: {odds_ratio:.2f}")


async def forced_close_position(side, size, reason="强制平仓"):
    """强制平仓"""
    global current_position, last_close_time

    logging.warning(f"🚨 [执行平仓] 原因: {reason} | 方向: {side} | 数量: {abs(size)}")

    close_side = 'SELL' if side == 'buy' else 'BUY'

    try:
        # 市价平仓
        result = await place_order(close_side, abs(size), order_type='MARKET', reduce_only=True)

        if result:
            exit_price = await get_current_price_fast()

            if current_position:
                calculate_and_log_pnl(current_position['entry_price'], exit_price, side, current_position['entry_time'])

            last_close_time = time.time()
            await cancel_all_orders()
            current_position = None
            logging.info(f"⏰ 进入冷却期 {COOLDOWN_AFTER_CLOSE_SECONDS}秒")

    except Exception as e:
        logging.error(f"❌ 平仓失败: {e}")


async def safe_place_market_order(side, notional_usdt, current_std_dev, signal_price):
    """安全下单"""
    global current_position, initial_tp

    price = await get_order_book_price_fast(side)
    if not price:
        price = await get_current_price_fast()

    # 计算数量
    quantity = notional_usdt * LEVERAGE / price
    quantity = format_quantity(quantity)

    if quantity == 0:
        return

    order_side = 'BUY' if side == 'buy' else 'SELL'

    # 限价开仓
    result = await place_order(order_side, quantity, price, order_type='LIMIT', time_in_force='GTC')

    if not result:
        return

    order_id = result['orderId']
    logging.info(f"🚀 发送限价开仓单: {side} {quantity}，订单ID: {order_id}")

    # 等待成交
    start_time = time.time()
    filled = False

    while time.time() - start_time < 15:
        await asyncio.sleep(1)
        order_info = await get_order_status(order_id)

        if order_info and order_info['status'] == 'FILLED':
            filled = True
            price = float(order_info['avgPrice'])
            break
        elif order_info and order_info['status'] in ['CANCELED', 'EXPIRED', 'REJECTED']:
            logging.warning(f"⚠️ 订单未成交: {order_info['status']}")
            return

    if not filled:
        logging.warning(f"⏰ 订单超时，尝试撤销")
        await cancel_all_orders()
        return

    # 计算止盈止损
    price_delta_tp = max(current_std_dev, MinProfitRate * price)
    price_delta_sl = price_delta_tp * 2

    if side == 'buy':
        tp_price = price + price_delta_tp
        sl_price = price - price_delta_sl
    else:
        tp_price = price - price_delta_tp
        sl_price = price + price_delta_sl

    tp_price = format_price(tp_price)
    sl_price = format_price(sl_price)
    initial_tp = tp_price

    logging.info(f"🔄 正在挂止盈止损单... TP:{tp_price} | SL:{sl_price}")

    # 挂止盈单
    tp_side = 'SELL' if side == 'buy' else 'BUY'
    tp_order = await place_order(tp_side, quantity, tp_price, order_type='LIMIT', time_in_force='GTC', reduce_only=True)

    # 挂止损单（使用STOP_MARKET）
    sl_params = {
        'symbol': SYMBOL,
        'side': tp_side,
        'type': 'STOP_MARKET',
        'stopPrice': format_price(sl_price),
        'quantity': format_quantity(quantity),
        'reduceOnly': 'true'
    }

    try:
        sl_order = await binance_request('POST', '/fapi/v1/order', sl_params, signed=True)
        logging.info(f"✅ 止损单挂单成功 ID: {sl_order['orderId']}")
    except Exception as e:
        logging.error(f"❌ 止损单挂单失败: {e}")
        sl_order = None

    # 更新持仓状态
    current_position = {
        'symbol': SYMBOL,
        'side': side,
        'entry_price': price,
        'size': quantity if side == 'buy' else -quantity,
        'entry_time': time.time(),
        'tp_price_trigger': tp_price,
        'sl_price_trigger': sl_price,
        'tp_order_id': tp_order['orderId'] if tp_order else None,
        'sl_order_id': sl_order['orderId'] if sl_order else None
    }


async def update_dynamic_tp_trigger():
    """更新动态止盈"""
    global current_position, initial_tp

    if not current_position:
        return

    pos = current_position
    time_held = time.time() - pos['entry_time']
    decay_ratio = time_held / MAX_HOLD_TIME_SECONDS
    decay_factor = math.exp(-DECAY_RATE_FACTOR * decay_ratio)
    decay_factor = max(0.0, decay_factor)

    min_tp_change = MinProfitRate * pos['entry_price']
    current_price = await get_current_price_fast()

    if not current_price:
        return

    price_change_tp = abs(initial_tp - pos['entry_price']) * decay_factor
    price_change_tp = max(price_change_tp, min_tp_change)

    if pos['side'] == 'buy':
        new_tp_price = pos['entry_price'] + price_change_tp
    else:
        new_tp_price = pos['entry_price'] - price_change_tp

    new_tp_price = format_price(new_tp_price)

    if new_tp_price == pos.get('tp_price_trigger'):
        return

    logging.info(f"✨ [动态TP] T+{time_held:.0f}s | 衰减率:{decay_factor:.2f} | 新TP价:{new_tp_price}")

    # 检查是否应该立即平仓
    should_close_now = (pos['side'] == 'buy' and current_price >= new_tp_price) or \
                       (pos['side'] == 'sell' and current_price <= new_tp_price)

    if should_close_now:
        logging.warning(f"⚠️ [动态TP即时平仓] 现价 {current_price} 已达到新TP目标 {new_tp_price}")
        await forced_close_position(pos['side'], abs(pos['size']), reason="动态TP达到目标")
        return

    # 取消旧的TP订单并创建新的
    try:
        if pos.get('tp_order_id'):
            await cancel_all_orders()

        # 创建新的TP订单
        tp_side = 'SELL' if pos['side'] == 'buy' else 'BUY'
        quantity = abs(pos['size'])

        tp_order = await place_order(tp_side, quantity, new_tp_price,
                                     order_type='LIMIT', time_in_force='GTC', reduce_only=True)

        if tp_order:
            pos['tp_price_trigger'] = new_tp_price
            pos['tp_order_id'] = tp_order['orderId']
            logging.info(f"✅ [TP更新成功] ID:{tp_order['orderId']} | 新TP触发价:{new_tp_price}")

    except Exception as e:
        logging.error(f"❌ 更新止盈失败: {e}")


async def monitor_and_close():
    """监控和平仓逻辑"""
    global current_position, last_close_time

    # 获取交易信号
    entry_side, atr, entry_price = await update_and_get_signal()

    if entry_side:
        logging.info(f"💡 脉冲反转信号触发: 准备 {entry_side}")
        await asyncio.sleep(ENTRY_DELAY_SECONDS)

        if current_position is None:
            await safe_place_market_order(entry_side, TARGET_ORDER_USDT, atr, entry_price)
            await asyncio.sleep(0.5)
        else:
            if current_position['side'] != entry_side:
                await forced_close_position(current_position['side'], abs(current_position['size']),
                                            reason="反转平仓")

    # 检测缓存遗漏订单
    if current_position is None:
        loss_position_data = await get_current_position_fast()
        real_size = float(loss_position_data['size'])

        if real_size > 0:
            await forced_close_position('buy', real_size, reason="丢失订单平仓")
        elif real_size < 0:
            await forced_close_position('sell', abs(real_size), reason="丢失订单平仓")
        return

    # 实时监控持仓
    pos = current_position
    current_price = await get_current_price_fast()

    if not current_price:
        return

    # 检查 API 真实持仓
    try:
        position = await get_position()
        real_size = float(position['size'])

        if real_size == 0:
            logging.warning(f"📉 [检测不到仓位] 交易所持仓为0，可能已止盈或止损")

            if current_position:
                exit_price = current_price if current_price else pos['entry_price']
                calculate_and_log_pnl(pos['entry_price'], exit_price, pos['side'], pos['entry_time'])

            last_close_time = time.time()
            current_position = None
            await cancel_all_orders()
            return

    except Exception as e:
        logging.error(f"❌ 获取持仓详情失败: {e}")
        return

    # 检查时间止损
    time_held = time.time() - pos['entry_time']

    if time_held >= MAX_HOLD_TIME_SECONDS:
        logging.warning(f"⏰ [时间止损] 持仓 {time_held:.0f}s 超时")
        await forced_close_position(pos['side'], abs(pos['size']), reason="超时")
        return

    # 打印 PNL
    if current_price:
        pnl_multiplier = 1 if pos['side'] == 'buy' else -1
        pnl_percent = ((current_price / pos['entry_price'] - 1) * 100 * LEVERAGE * pnl_multiplier)
        logging.info(f"📊 [监控] {pos['side']} PNL:{pnl_percent:+.2f}% | 持仓:{time_held:.0f}s | "
                     f"SL:{pos['sl_price_trigger']} | TP:{pos['tp_price_trigger']}")


async def user_data_stream():
    """用户数据流 - 监听账户更新"""
    try:
        # 获取 listenKey
        result = await binance_request('POST', '/fapi/v1/listenKey', signed=True)
        listen_key = result['listenKey']
        logging.info(f"✅ 获取到 listenKey: {listen_key[:10]}...")

        # 启动 keepalive 任务
        async def keepalive():
            while True:
                await asyncio.sleep(30 * 60)  # 每30分钟续期
                try:
                    await binance_request('PUT', '/fapi/v1/listenKey', signed=True)
                    logging.debug("🔄 listenKey 续期成功")
                except Exception as e:
                    logging.error(f"❌ listenKey 续期失败: {e}")

        asyncio.create_task(keepalive())

        # 连接 WebSocket
        ws_url = f"{WS_URL}/{listen_key}"

        # 创建 SSL 上下文
        ssl_context = None
        if USE_PROXY_DOMAIN:
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.ws_connect(ws_url) as ws:
                logging.info("✅ 用户数据流已连接")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        event_type = data.get('e')

                        # 账户更新
                        if event_type == 'ACCOUNT_UPDATE':
                            for position in data.get('a', {}).get('P', []):
                                if position['s'] == SYMBOL:
                                    async with POSITION_LOCK:
                                        LIVE_POSITION_CACHE['size'] = float(position['pa'])
                                        LIVE_POSITION_CACHE['entry_price'] = float(position['ep'])

                                    logging.debug(f"✅ [持仓更新] Size: {position['pa']} | Entry: {position['ep']}")

                        # 订单更新
                        elif event_type == 'ORDER_TRADE_UPDATE':
                            order = data.get('o', {})
                            logging.info(f"📝 [订单更新] {order['s']} {order['S']} {order['X']} - {order['o']}")

    except Exception as e:
        logging.error(f"❌ 用户数据流错误: {e}")
        await asyncio.sleep(5)
        # 递归重连
        asyncio.create_task(user_data_stream())


async def scalper():
    """主交易循环"""
    global current_position, last_close_time

    logging.info("⚙️ 初始化...")

    # 初始化设置
    await get_exchange_info()
    await set_margin_type('CROSSED')
    await set_leverage_async(LEVERAGE)
    await cancel_all_orders()

    # 检查并平掉已有持仓（可选）
    existing_pos = await get_position()
    if existing_pos['size'] != 0:
        logging.warning(f"⚠️ 检测到现有持仓: {existing_pos['size']}")
        side = 'buy' if existing_pos['size'] > 0 else 'sell'
        # 如果需要自动平仓，取消下面的注释
        # await forced_close_position(side, abs(existing_pos['size']), reason="启动前清仓")

    # 启动数据流
    asyncio.create_task(price_data_streamer())
    logging.info("✅ 价格流任务已启动")

    asyncio.create_task(user_data_stream())
    logging.info("✅ 用户数据流任务已启动")

    # 等待数据流初始化
    await asyncio.sleep(3)

    logging.info("🚀 开始交易...")

    last_tp_update_time = 0

    while True:
        try:
            # 冷却期检查
            if last_close_time > 0:
                rem = COOLDOWN_AFTER_CLOSE_SECONDS - (time.time() - last_close_time)
                if rem > 0:
                    logging.info(f"❄️ 冷却中... 剩余 {rem:.0f}s")
                    await asyncio.sleep(MONITOR_INTERVAL)
                    continue

            # 监控和交易
            await monitor_and_close()

            # 更新动态止盈
            if current_position and (time.time() - last_tp_update_time) >= TP_UPDATE_INTERVAL_SECONDS:
                await update_dynamic_tp_trigger()
                last_tp_update_time = time.time()

            await asyncio.sleep(MONITOR_INTERVAL)

        except Exception as e:
            logging.error(f"🔥 主循环未捕获异常: {e}")
            logging.error(traceback.format_exc())
            await asyncio.sleep(1)


async def main():
    """程序入口"""
    try:
        await scalper()
    except KeyboardInterrupt:
        logging.info("⚠️ 收到中断信号，正在退出...")
        # 清理工作
        if current_position:
            logging.warning("⚠️ 检测到持仓，请手动处理")
        await cancel_all_orders()
    except Exception as e:
        logging.error(f"❌ 程序异常退出: {e}")
        logging.error(traceback.format_exc())


if __name__ == '__main__':
    # 设置事件循环策略（Windows 上需要）
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # 运行主程序
    asyncio.run(main())