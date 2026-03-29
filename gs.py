import time,traceback,asyncio, math,json,urllib.request,websockets ,hmac, hashlib,logging
from gate_api import ApiClient, Configuration, FuturesApi, FuturesOrder, FuturesPriceTriggeredOrder, FuturesInitialOrder
from gate_api.exceptions import GateApiException
# 总笔数: 36 | 累计PNL: -2.25 USDT | 胜率: 58.33% | 赔率: 0.47
API_KEY = 'aa4fd585ed9af476f4c8fbe025edcf35'  # 请替换回你的 Key
API_SECRET = '97715e80212ad8ecde82333446fa72907052cd790f08bfe57fc18e09991b5733'  # 请替换回你的 Secret
SYMBOL = 'FARTCOIN_USDT'
SETTLE = 'usdt'
TARGET_ORDER_USDT = 50
LEVERAGE = 10
TIMEFRAME = '1m'
BB_PERIOD = 20
BB_STD = 2
EMA_PERIOD = 10
MAX_HOLD_TIME_SECONDS = 300
MinProfitRate = 0.00050  #这里是名义价值比例，maker的资金费率是名义价值的0.02%，也就是万分之2(0.0002)，每个单保证金的0.002,即千分之2(0.2%)
DECAY_RATE_FACTOR = 2  # 控制衰减速度。值越大，衰减越快。
ENTRY_DELAY_SECONDS = 0
TRADE_WAIT_TIME=10
MONITOR_INTERVAL = 0.15
COOLDOWN_AFTER_CLOSE_SECONDS = 0
TP_UPDATE_INTERVAL_SECONDS = 10
TRIGGER_ORDER_EXPIRATION = 86400 * 7
VOL_SMA_PERIOD = 20# [新增] 交易量与订单簿分析参数 (调整值以提高触发率)
VOL_SPIKE_THRESHOLD = 1.2    # 保留或调小
OB_IMBALANCE_THRESHOLD = 0.8
std_rate=0.618 # 原 0.618，改为 0.5
STD_THRESHOLD=0.01
WICK_BODY_RATIO = 2
MIN_WICK_LEN_FACTOR = 0.3# 最小波动过滤：防止死水微澜时的 1 跳波动触发信号，影线长度必须至少大于多少倍的 std_dev (标准差)
WS_URL = 'wss://fx-ws.gateio.ws/v4/ws/usdt'  # 【WS修改】Gate.io 合约 WS 地址
OB_DEPTH_LIMIT = 20
LIVE_ORDER_BOOK_CACHE = {'bids': {}, 'asks': {}}
ORDER_BOOK_LOCK = asyncio.Lock() # 【新增】订单簿锁
LAST_ORDER_BOOK_UPDATE_ID = 0
LIVE_POSITION_CACHE = {'size': 0.0, 'entry_price': 0.0}
POSITION_LOCK = asyncio.Lock() # 【新增】持仓锁
LIVE_CANDLE_CACHE = [0, 0, 0, 0, 0, 0.0]
CACHE_LOCK = asyncio.Lock()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
current_candle = {}
last_close_time = 0
config = Configuration(key=API_KEY, secret=API_SECRET)
api_client = ApiClient(config)
futures_api = FuturesApi(api_client)
current_position = None
initial_tp = 0
ohlcv_cache = []
last_close_time = 0
contract_details = None
last_tp_update_time = 0
price_precision = 4
trade_history = []
cumulative_pnl = 0.0
total_trades = 0
win_count = 0
loss_count = 0
total_win_pnl = 0.0
total_loss_pnl = 0.0
# ================== 辅助函数 ==================
def auto_sync_time():
    try:
        url = "https://api.gateio.ws/api/v4/spot/time"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            server_time_ms = int(data['server_time'])
            local_time_ms = int(time.time() * 1000)
            offset_ms = server_time_ms - local_time_ms

            if abs(offset_ms) > 1000:
                print(f"⚠️ 检测到时间偏差: {offset_ms}ms. 正在修正本地时间...")
                original_time = time.time
                time.time = lambda: original_time() + (offset_ms / 1000.0)
                print("✅ 时间已同步.")
    except Exception as e:
        print(f"⚠️ 时间同步失败 (非致命): {e}")
def get_contract_detail():
    global contract_details, price_precision
    if contract_details:
        return contract_details

    contract = futures_api.get_futures_contract(SETTLE, SYMBOL)
    contract_details = contract
    price_step_str = contract.order_price_round
    price_step = float(price_step_str)
    price_precision = int(round(-math.log10(price_step)))

    logging.info(f"⚙️ {SYMBOL} 价格精度设置为: {price_precision}")

    return contract
def set_leverage_sync(leverage):
    try:
        futures_api.update_position_leverage(SETTLE, SYMBOL, leverage=str(leverage))
        logging.info(f"✅ 杠杆已设置为: {leverage}x")
    except Exception:
        pass
def usdt_to_contract_size(usdt_amount, price):
    contract = get_contract_detail()
    if not contract: return 0
    quanto = float(contract.quanto_multiplier)
    if price == 0: return 0
    return int(round((usdt_amount / price) / quanto))
def generate_auth_payload(channel, api_key, api_secret):

    clean_api_secret = api_secret.strip()
    t = str(int(time.time()))
    message = f"channel={channel}&event=subscribe&time={t}"
    signature = hmac.new(
        clean_api_secret.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha512
    ).hexdigest()
    payload = {
        # JSON 消息体中的 time 字段使用整数秒
        "time": int(t),
        "channel": channel,
        "event": "subscribe",
        "auth": {
            "method": "api_key",
            "KEY": api_key,
            "SIGN": signature
        },
        "payload": [SYMBOL]
    }
    return payload
async def price_data_streamer():
    global LIVE_CANDLE_CACHE, LIVE_POSITION_CACHE, LIVE_ORDER_BOOK_CACHE, LAST_ORDER_BOOK_UPDATE_ID
    current_minute_start_time = 0
    ORDER_BOOK_UPDATE_CHANNEL = "futures.order_book_update"
    UPDATE_FREQUENCY = "100ms"
    LEVEL = "100"
    LEVEL_INT = int(LEVEL)

    while True:
        logging.info(f"🔗 正在连接 WebSocket: {WS_URL} ...")

        try:
            async with websockets.connect(WS_URL) as websocket:

                # 订阅 Tickers
                subscribe_ticker_msg = {
                    "time": int(time.time()),
                    "channel": "futures.tickers",
                    "event": "subscribe",
                    "payload": [SYMBOL]
                }
                await websocket.send(json.dumps(subscribe_ticker_msg))

                # 订阅 Positions
                subscribe_position_msg = generate_auth_payload("futures.positions", API_KEY, API_SECRET)
                await websocket.send(json.dumps(subscribe_position_msg))

                # 订阅增量订单簿
                subscribe_order_book_update_msg = {
                    "time": int(time.time()),
                    "channel": ORDER_BOOK_UPDATE_CHANNEL,
                    "event": "subscribe",
                    "payload": [SYMBOL, UPDATE_FREQUENCY, LEVEL]
                }
                await websocket.send(json.dumps(subscribe_order_book_update_msg))

                logging.info(
                    f"✅ WebSocket 已连接并订阅 {SYMBOL} 价格、持仓和增量订单簿 (Level: {LEVEL}, Freq: {UPDATE_FREQUENCY})")

                # 每次重新连接时重置
                LAST_ORDER_BOOK_UPDATE_ID = 0

                async for message in websocket:
                    data = json.loads(message)

                    event = data.get('event')
                    channel = data.get('channel')
                    result = data.get('result', [])

                    # 忽略控制消息
                    if event in ('subscribe', 'error'):
                        if event == 'subscribe':
                            if data.get('error'):
                                logging.error(f"❌ {channel} 订阅失败: {data.get('error')}")
                            else:
                                logging.info(f"✅ {channel} 订阅确认成功。")
                        elif event == 'error':
                            logging.error(f"❌ WebSocket 接收到错误消息: {data}")
                        continue

                    if not channel or not result:
                        continue

                    # ===== 处理 Tickers 价格流 =====
                    if channel == 'futures.tickers':
                        result = result[0] if isinstance(result, list) and result else {}
                        tick = result
                        current_price = float(tick['last'])
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

                        logging.debug(f"ℹ️ [价格更新] Latest Price: {current_price}")

                    # ===== 处理 Positions 持仓 =====
                    elif channel == 'futures.positions':
                        my_pos = next((p for p in result if p.get('contract') == SYMBOL), None)
                        if my_pos:
                            async with POSITION_LOCK:
                                LIVE_POSITION_CACHE['size'] = float(my_pos.get('size', 0))
                                LIVE_POSITION_CACHE['entry_price'] = float(my_pos.get('entry_price', 0))

                            logging.debug(
                                f"✅ [持仓更新] Size: {LIVE_POSITION_CACHE['size']} | Entry: {LIVE_POSITION_CACHE['entry_price']}")

                    # ===== 处理增量订单簿 =====
                    elif channel == ORDER_BOOK_UPDATE_CHANNEL:
                        ob_data = result

                        is_full_snapshot = ob_data.get('full', False)
                        update_id_start = ob_data.get('U')
                        update_id_end = ob_data.get('u')

                        # 序列 ID 校验
                        if not is_full_snapshot and LAST_ORDER_BOOK_UPDATE_ID is not None and LAST_ORDER_BOOK_UPDATE_ID != 0:
                            expected_start_id = LAST_ORDER_BOOK_UPDATE_ID + 1
                            if update_id_start != expected_start_id:
                                logging.error(
                                    f"❌ 订单簿序列ID校验失败！期望U: {expected_start_id}, 实际U: {update_id_start}. 正在重置连接...")
                                raise Exception("Order book sequence mismatch detected.")

                        async with ORDER_BOOK_LOCK:
                            # === 全量快照处理 ===
                            if is_full_snapshot:
                                bids_list = ob_data.get('b', [])
                                asks_list = ob_data.get('a', [])

                                # ✅ 内部始终维护为字典格式
                                LIVE_ORDER_BOOK_CACHE['bids'] = {float(p): float(s) for p, s in bids_list}
                                LIVE_ORDER_BOOK_CACHE['asks'] = {float(p): float(s) for p, s in asks_list}

                                LAST_ORDER_BOOK_UPDATE_ID = update_id_end
                                logging.info(f"🔄 [订单簿] 接收到全量快照, 初始ID设置为: {LAST_ORDER_BOOK_UPDATE_ID}")

                            # === 增量更新处理 ===
                            else:
                                # ✅ 关键修复：确保在处理增量更新前，数据结构是字典
                                if not isinstance(LIVE_ORDER_BOOK_CACHE['bids'], dict):
                                    LIVE_ORDER_BOOK_CACHE['bids'] = {}
                                if not isinstance(LIVE_ORDER_BOOK_CACHE['asks'], dict):
                                    LIVE_ORDER_BOOK_CACHE['asks'] = {}

                                # 更新 bids
                                for item in ob_data.get('b', []):
                                    price = float(item['p'])
                                    size = float(item['s'])
                                    if size == 0.0:
                                        LIVE_ORDER_BOOK_CACHE['bids'].pop(price, None)
                                    else:
                                        LIVE_ORDER_BOOK_CACHE['bids'][price] = size

                                # 更新 asks
                                for item in ob_data.get('a', []):
                                    price = float(item['p'])
                                    size = float(item['s'])
                                    if size == 0.0:
                                        LIVE_ORDER_BOOK_CACHE['asks'].pop(price, None)
                                    else:
                                        LIVE_ORDER_BOOK_CACHE['asks'][price] = size

                                LAST_ORDER_BOOK_UPDATE_ID = update_id_end

                            logging.debug(f"✅ [订单簿更新] LOB updated. New ID: {LAST_ORDER_BOOK_UPDATE_ID}")

        except Exception as e:
            logging.error(f"❌ WebSocket 连接错误: {e}")
            logging.info("🔄 5秒后重新连接...")
            await asyncio.sleep(5)
async def get_current_price_fast():
    async with CACHE_LOCK:
        price = LIVE_CANDLE_CACHE[4]
        return price
async def get_current_position_fast():
    async with POSITION_LOCK:
        # 返回持仓量和开仓价，确保返回的结构与原 API 返回的数据可兼容（至少包含 size 和 entry_price）
        return LIVE_POSITION_CACHE
async def get_depth_pressure_fast(limit=OB_DEPTH_LIMIT):
    async with ORDER_BOOK_LOCK:
        bids_data = LIVE_ORDER_BOOK_CACHE['bids']
        asks_data = LIVE_ORDER_BOOK_CACHE['asks']
        if isinstance(bids_data, dict):
            # 字典格式：先排序再取前 N 档
            sorted_bids = sorted(bids_data.items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(asks_data.items(), key=lambda x: x[0], reverse=False)

            bids_vol = sum([size for price, size in sorted_bids[:limit]])
            asks_vol = sum([size for price, size in sorted_asks[:limit]])
        else:
            # 列表格式（容错处理）
            bids_vol = sum([b[1] for b in bids_data[:limit]])
            asks_vol = sum([a[1] for a in asks_data[:limit]])

    total_vol = bids_vol + asks_vol
    if total_vol == 0:
        return 0.5

    ratio = bids_vol / total_vol
    return ratio
async def get_order_book_price_fast(signal_side, depth_level=3):

    async with ORDER_BOOK_LOCK:
        if signal_side == 'buy':
            bids_data = LIVE_ORDER_BOOK_CACHE['bids']

            # ✅ 根据数据类型进行不同的处理
            if isinstance(bids_data, dict):
                # 字典格式：排序后取指定档位
                sorted_bids = sorted(bids_data.items(), key=lambda x: x[0], reverse=True)
                if len(sorted_bids) >= depth_level:
                    return sorted_bids[depth_level - 1][0]  # (price, size) -> price
            else:
                # 列表格式（容错处理）
                if len(bids_data) >= depth_level:
                    return bids_data[depth_level - 1][0]  # [price, amount] -> price

        else:  # signal_side == 'sell'
            asks_data = LIVE_ORDER_BOOK_CACHE['asks']

            # ✅ 根据数据类型进行不同的处理
            if isinstance(asks_data, dict):
                # 字典格式：排序后取指定档位
                sorted_asks = sorted(asks_data.items(), key=lambda x: x[0], reverse=False)
                if len(sorted_asks) >= depth_level:
                    return sorted_asks[depth_level - 1][0]  # (price, size) -> price
            else:
                # 列表格式（容错处理）
                if len(asks_data) >= depth_level:
                    return asks_data[depth_level - 1][0]  # [price, amount] -> price

    # 如果缓存为空或深度不足，回退到 API（仅用于初始化或意外情况）
    logging.warning(f"⚠️ 盘口缓存不足，尝试 API 获取 {signal_side} 侧价格...")
    return None
async def get_live_candle():
    """安全地读取当前 1m 实时 K 线。"""
    async with CACHE_LOCK:
        # 返回一个副本，防止外部意外修改
        return list(LIVE_CANDLE_CACHE)

async def cancel_all_orders_sync():

    futures_api.cancel_futures_orders(SETTLE, contract=SYMBOL)

    try:
        # 清理所有触发单
        api_client.call_api(f'/futures/{SETTLE}/price_orders', 'DELETE',
                            query_params={'contract': SYMBOL, 'status': 'open'}, auth_settings=['apiv4'])
        logging.info("🧹 已清理所有挂单")
    except Exception:
        pass

async def clear_loss_position():
    loss_position_data = await get_current_position_fast()
    real_size = float(loss_position_data['size'])
    if real_size!=0:
        close_price=await get_current_price_fast()
        order = FuturesOrder(
            contract=SYMBOL,
            size=-real_size,
            price=close_price,
            tif='gtc',
            text='t-close'
        )
        res = await asyncio.to_thread(futures_api.create_futures_order, SETTLE, order)
        order_id = res.id
        logging.warning(f"🚨 [执行平仓] 原因: 丢失订单| 数量: {real_size}张，[平仓订单发送成功] {-real_size}张,订单ID: {order_id}")
        await asyncio.sleep(1)
def calculate_and_log_pnl(entry_price, exit_price, side, entry_time):
    """计算 PNL 并更新统计"""
    global cumulative_pnl, total_trades, win_count, loss_count, total_win_pnl, total_loss_pnl, trade_history,COOLDOWN_AFTER_CLOSE_SECONDS

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
        COOLDOWN_AFTER_CLOSE_SECONDS=0
    else:
        loss_count += 1
        total_loss_pnl += abs(pnl_usdt)
        COOLDOWN_AFTER_CLOSE_SECONDS=10

    win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
    odds_ratio = (total_win_pnl / win_count) / (total_loss_pnl / loss_count) if win_count > 0 and loss_count > 0 else 0

    logging.info(
        f"📈 [累计统计] 总笔数: {total_trades} | 累计PNL: {cumulative_pnl:+.2f} USDT | 胜率: {win_rate:.2f}% | 赔率: {odds_ratio:.2f}")
async def prepare_data_for_strategy(limit=300):
    candles = await asyncio.to_thread(get_ohlcv_sync, limit)
    if not candles: return None
    live_candle = await get_live_candle()

    # 如果 WS 还没启动导致价格为 0，则暂时不返回数据
    if live_candle[4] == 0:
        return None

    history_data = candles[:-1]
    # 使用 WS 维护的实时 K 线替代简陋的构建
    combined_data = history_data + [live_candle]
    return combined_data
def get_ohlcv_sync(limit):
    try:
        candlesticks = futures_api.list_futures_candlesticks(SETTLE, SYMBOL, interval=TIMEFRAME, limit=limit)
        data = []
        for c in candlesticks:
            # 统一数据结构为: [time, open, high, low, close, volume] (长度 6, 索引 0-5)
            # 这里的顺序是为了与 TradingView/通用策略库习惯保持一致
            data.append([c.t, float(c.o), float(c.h), float(c.l), float(c.c), float(c.v)])
        return data
    except Exception:
        return []
def calculate_ema_js_style(closes, period):
    if not closes: return 0
    k = 2 / (period + 1)
    ema = closes[0]
    for price in closes[1:]:
        ema = price * k + ema * (1 - k)
    return ema
def calculate_indicators_js_match(ohlcv_data):
    closes = [x[4] for x in ohlcv_data]
    volumes = [x[5] for x in ohlcv_data] # Gate API k线数据: [t, v, c, h, l, o] -> 注意 Gate API 顺序可能不同，下面会修正 get_ohlcv_sync
    # 1. Bollinger Bands
    bb_slice = closes[-BB_PERIOD:]
    ma = sum(bb_slice) / BB_PERIOD
    variance = sum([((x - ma) ** 2) for x in bb_slice]) / BB_PERIOD
    std_dev = math.sqrt(variance)


    # 3. [新增] Volume SMA
    vol_slice = volumes[-VOL_SMA_PERIOD:]
    vol_sma = sum(vol_slice) / VOL_SMA_PERIOD

    return std_dev, vol_sma
def create_price_triggered_order(symbol, trigger_price, side, size, order_type='TP'):
    if order_type == 'TP':
        rule = 1 if side == 'buy' else 2
        text_label = 't-tp-trigger'
    else:  # SL
        rule = 2 if side == 'buy' else 1
        text_label = 't-sl-trigger'

    # 触发单的大小是当前持仓的反向（平仓）
    close_size = -size

    initial_order = FuturesInitialOrder(
        contract=symbol, size=close_size, price='0',
        close=False,  # 双向持仓模式下，触发单建议设为False，配合reduce_only
        reduce_only=True, tif='ioc', text=text_label
    )

    trigger_order = FuturesPriceTriggeredOrder(
        initial=initial_order,
        trigger=dict(strategy_type=0, price_type=0, price=str(trigger_price), rule=rule,
                     expiration=TRIGGER_ORDER_EXPIRATION)
    )
    return trigger_order
async def forced_close_position(reason="强制平仓"):
    """ 强制市价平仓 """
    global current_position, last_close_time,last_tp_update_time
    loss_position_data = await get_current_position_fast()
    real_size = float(loss_position_data['size'])
    if real_size==0:
        logging.info('没有仓位，无法平仓')
        return
    if real_size>0:
        side='buy'
        close_side='sell'
    else:
        side = 'sell'
        close_side='buy'
    close_price= await  get_order_book_price_fast(close_side)
    try:
        order = FuturesOrder(
            contract=SYMBOL,
            size=-real_size,
            price=close_price,
            tif='gtc',
            text='t-close'
        )
        res = await asyncio.to_thread(futures_api.create_futures_order, SETTLE, order)
        order_id=res.id
        logging.warning(f"🚨 [执行平仓] 原因: {reason} | 平仓方向: {close_side} | 数量: {abs(real_size)}张")
        logging.info(f"✅ [平仓订单发送成功] 订单ID: {order_id}")
        # 3. 等待成交确认 (非常重要)
        start_time = time.time()
        poll_interval = 1.0  # 每隔 1 秒查询一次

        while time.time() - start_time < TRADE_WAIT_TIME:
            try:

                order_info = await asyncio.to_thread(futures_api.get_futures_order, SETTLE, order_id)
                if (order_info.status == 'finished') and (order_info.finish_as == 'filled'):
                    exit_price = close_price
                    # 仅在 current_position 存在时才计算 PNL
                    if current_position:
                        calculate_and_log_pnl(current_position['entry_price'], exit_price, side,
                                              current_position['entry_time'])

                    last_close_time = time.time()
                    current_position = None
                    await cancel_all_orders_sync()
                    await asyncio.sleep(0.5)  # 给点时间更新实际仓
                    await clear_loss_position()
                    logging.info(f"⏰ 进入冷却期 {COOLDOWN_AFTER_CLOSE_SECONDS}秒")
                    auto_sync_time()
                    return
                else:
                    logging.debug(f"平仓订单 {order_id} 尚未成交，继续等待...")

            except Exception as e:
                # 这里的异常可能是网络问题或订单 ID 暂时查不到
                logging.error(f"❌ 查询订单 {order_id} 发生错误: {e}")

            # 暂停，等待下一轮查询
            await asyncio.sleep(poll_interval)
        logging.info(f'平仓挂单成交超时{TRADE_WAIT_TIME}s,准备撤单，等下一轮检查再平仓')
        await cancel_all_orders_sync()
        last_tp_update_time=time.time() # 避免存在仓位的时候，又撤单
    except GateApiException as e:
        logging.error(f"❌ 平仓失败: {e}")
async def update_and_get_signal():
    data = await prepare_data_for_strategy(limit=200)
    if data is None:
        return None, None, None
    # 2. 计算指标
    std_dev, vol_sma = calculate_indicators_js_match(data)

    # 3. 获取实时数据
    current_price = data[-1][4]
    # 【修正点 1】: 使用前一根已收盘 K 线的成交量来判断市场活跃度
    prev_candle = data[-2]
    open_price = prev_candle[4]  # 保持原有逻辑
    prev_vol = prev_candle[5]  # 获取前一根 K 线的成交量
    if std_dev/current_price>STD_THRESHOLD:
        return None, None, None

    # EMA_TREND = 169
    # ema169 = calculate_ema_js_style([x[4] for x in data[-EMA_TREND:]], EMA_TREND)
    # current_price = data[-1][4]
    # if current_price > ema169 * 0.998:
    #     ema_side='buy'
    # if current_price < ema169 * 1.002:
    #     ema_side = 'sell'
    ema_side=None
    if data[-1][4] > data[-1][1] and  data[-2][4] > data[-2][1] :
        # print('连续上涨2，sell')
        ema_side='sell'
    if data[-1][4] <data[-1][1] and  data[-2][4]< data[-2][1]:
        # print('连续下跌2，buy')
        ema_side = 'buy'
    if ema_side==None:
        return None, None, None
    # 乖离率计算
    last_dev = abs(prev_candle[2] - prev_candle[3]) * std_rate  # High - Low
    # 异步获取盘口深度分析
    ob_ratio = await get_depth_pressure_fast()
    # [修正点 2] 成交量分析: 活跃度基于前一根 K 线成交量
    is_vol_active = prev_vol > (vol_sma * 0.5)

    # 4.上影线判断，先避免踩坑
    # 获取当前实时 K 线 (Live Candle)
    # 格式: [start_time, open, high, low, close, volume]
    live_candle = await get_live_candle()

    # 解析 OHLC
    o = live_candle[1]
    h = live_candle[2]
    l = live_candle[3]
    c = live_candle[4]  # 也就是 current_price

    # 5. 计算实体和影线长度
    # 实体高度 (Body)
    body_size = abs(c - o)
    # print(f"{o},{h},{l},{c},{body_size}")

    # 上影线 (Upper Wick): 最高价 - 实体上沿(max(o,c))
    upper_wick = h - max(c, o)

    # 下影线 (Lower Wick): 实体下沿(min(o,c)) - 最低价
    lower_wick = min(c, o) - l

    # 4. 计算比例 (处理实体为0的除零错误)
    # 如果实体极小(例如十字星)，我们认为它是极小的数，避免报错
    safe_body = body_size if body_size > 1e-10 else 1e-10

    upper_ratio = upper_wick / safe_body
    lower_ratio = lower_wick / safe_body

    # 打印调试日志 (可选，观察形态)
    # logging.info(f"🕯️ K线形态: UpRatio:{upper_ratio:.2f} | LowRatio:{lower_ratio:.2f} | Body:{body_size:.8f}")

    # ================= 信号判定 =================
    candle_signal=None
    # 信号 1: 卖出信号 (冲高失败)
    # 条件: 上影线比例达标 AND 上影线绝对长度不是噪音
    if upper_ratio > WICK_BODY_RATIO:
        # 过滤: 影线长度至少要有一定波动幅度，防止横盘时的噪音
        if upper_wick > (std_dev * MIN_WICK_LEN_FACTOR):
            # logging.info(f"⬇️ [形态卖出] 冲高回落! 上影线比例: {upper_ratio:.2f} > {WICK_BODY_RATIO}")
            candle_signal='sell'

    # 信号 2: 买入信号 (探底回升)
    # 条件: 下影线比例达标 AND 下影线绝对长度不是噪音
    if lower_ratio > WICK_BODY_RATIO:
        # 过滤: 影线长度至少要有一定波动幅度
        if lower_wick > (std_dev * MIN_WICK_LEN_FACTOR):
            # logging.info(f"⬆️ [形态买入] 金针探底! 下影线比例: {lower_ratio:.2f} > {WICK_BODY_RATIO}")
            candle_signal='buy'

    # ================= 乖离率信号判定 =================

    # 1. 顺势做多 (超跌反弹/支撑位买入)
    if current_price < (open_price - last_dev) and ema_side=='buy':
        # logging.info(f"脉冲买： Ratio:{ob_ratio:.2f}")
        if ob_ratio >= OB_IMBALANCE_THRESHOLD and is_vol_active:
            logging.info(f"⚡ [强力买入] 价格超跌且盘口支撑强. Ratio:{ob_ratio:.2f}")
            if candle_signal is None:
                return 'buy', std_dev, current_price
            else:
                if candle_signal=='buy':
                    return 'buy', std_dev, current_price
                else:
                    logging.info(f"但是判断为冲高回落，不能再买了！")
        else:
            pass

    # 2. 顺势做空 (冲高回落/压力位卖出)
    elif current_price > (open_price + last_dev) and ema_side=='sell':
        if ob_ratio <= (1 - OB_IMBALANCE_THRESHOLD) and is_vol_active:
            logging.info(f"⚡ [强力做空] 价格冲高且盘口压制强. Ratio:{ob_ratio:.2f}")
            if candle_signal is None:
                return 'sell', std_dev, current_price
            else:
                if candle_signal == 'sell':
                    return 'sell', std_dev, current_price
                else:
                    logging.info(f"但是判断为已经金针探底，不能再做空了！")
        else:
            pass

    return None, None, None
async def update_dynamic_tp_trigger():
    global current_position,initial_tp,last_tp_update_time
    if not current_position:
        return
    if time.time()-last_tp_update_time<TP_UPDATE_INTERVAL_SECONDS:
        return
    last_tp_update_time=time.time()
    pos = current_position
    # 1. 计算时间衰减因子和新的 TP 目标价,如果没有变化，就不更新了
    time_held = time.time() - pos['entry_time']
    decay_ratio = time_held / MAX_HOLD_TIME_SECONDS
    decay_factor = math.exp(-DECAY_RATE_FACTOR * decay_ratio)
    decay_factor = max(0.0, decay_factor)

    min_tp_change = MinProfitRate * pos['entry_price']
    price_change_tp = abs(initial_tp - pos['entry_price']) * decay_factor
    price_change_tp = max(price_change_tp, min_tp_change)
    if pos['side'] == 'buy':
        new_tp_price = pos['entry_price'] + price_change_tp
    else:
        new_tp_price = pos['entry_price'] - price_change_tp

    new_tp_price = round(new_tp_price, price_precision)
    if new_tp_price == pos.get('tp_price_trigger'):
        return
    # 2.删除旧订单
    try:
        old_tp_id = pos.get('tp_trigger_id')
        # 撤销旧的 TP 触发单 (仅撤销 TP，SL 保持不变)
        if old_tp_id:
            # 仅撤销单个触发单
            cancel_res = futures_api.cancel_futures_order(SETTLE, old_tp_id)

            # logging.info(f"🧹 已撤销旧TP触发单: {old_tp_id}")
    except Exception as e:
        logging.error(f"❌ 删除旧的止盈失败,止盈订单或已成交或者网络问题、或者因为挂单超时把它撤了{e}")
        await asyncio.sleep(0.5)#
        pos= await get_current_position_fast()
        real_size=float(pos['size'])
        if real_size!=0:
            if real_size>0:
                side='sell'
            else:
                side='buy'
            logging.info(f'查到是因为挂单超时把它撤了,准备创建新的止盈单和止损单')
            sl_price=float(current_position['sl_price_trigger'])
            sl_order = create_price_triggered_order(SYMBOL, sl_price, side, real_size, 'SL')
            sl_res = await asyncio.to_thread(futures_api.create_price_triggered_order, SETTLE, sl_order)
            logging.info(f"✅ SL触发单重新挂单成功 ID: {sl_res.id}")
        else:
            current_position=None
            logging.info(f'止盈订单或已成交')
            return

    # logging.info(f"✨ [动态TP] T+{(time_held):.0f}s | 衰减率:{decay_factor:.2f}  | 新TP价:{new_tp_price}")
    # 3.创建并发送新的 TP 触发单
    try:
        order = FuturesOrder(contract=SYMBOL, size=-pos['size'], price=new_tp_price, tif='gtc', text='t-entry')
        new_tp_res = await asyncio.to_thread(futures_api.create_futures_order, SETTLE, order)
        new_tp_id = new_tp_res.id
        #  更新持仓记录
        pos['tp_price_trigger'] = new_tp_price
        pos['tp_trigger_id'] = new_tp_id
        logging.info(f"✅ [TP更新成功] ID:{new_tp_id} | 新TP触发价:{new_tp_price}")
    except Exception as e:
        logging.error(f"❌创建止盈订单失败，可能价格冲突，直接市价平仓 {e}")
        await forced_close_position('止盈价格冲突，直接市价平仓')
        return
async def get_active_trigger_orders_sync(symbol):
    try:
        orders = await asyncio.to_thread(
            futures_api.list_price_triggered_orders, SETTLE, status='open', contract=symbol
        )
        return orders
    except GateApiException as e:
        if str(e).startswith('(404)'):  # 404 Not Found 可能是没有订单
            return []
        logging.error(f"❌ 查询活动触发单失败: {e}")
        return []
async def safe_place_market_order( side, notional_usdt, current_std_dev):
    global current_position, initial_tp, price_precision

    price = await get_order_book_price_fast(side)
    if not price: return

    size_abs = usdt_to_contract_size(notional_usdt, price)
    if size_abs == 0: return
    real_size = size_abs if side == 'buy' else -size_abs
    # 2. 🚀 第一步：发送纯净的开仓市价单（不带止盈止损）
    order = FuturesOrder(contract=SYMBOL, size=real_size, price=price, tif='gtc', text='t-entry')
    res = await asyncio.to_thread(futures_api.create_futures_order, SETTLE, order)
    order_id=res.id
    logging.info(f"🚀 发送限价开仓单: {side} {real_size}张...，返回订单ID:{res.id}")
    # 3. 等待成交确认 (非常重要)
    start_time = time.time()
    poll_interval = 1.0  # 每隔 1 秒查询一次

    while  time.time() - start_time< 15:
        try:

            order_info = await asyncio.to_thread(futures_api.get_futures_order, SETTLE, order_id)
            if (order_info.status == 'finished') and (order_info.finish_as=='filled'):
                logging.info(f"🚀 限价开仓单: {side} {real_size}张，订单ID:{res.id}已成交")
                actual_entry_price = price
                # 5. 计算止盈止损价格 (基于真实成交价)
                price_delta_tp = max(current_std_dev, MinProfitRate * actual_entry_price)
                price_delta_sl = price_delta_tp * 1.52

                if side == 'buy':
                    tp_price = actual_entry_price + price_delta_tp
                    sl_price = actual_entry_price - price_delta_sl
                else:
                    tp_price = actual_entry_price - price_delta_tp
                    sl_price = actual_entry_price + price_delta_sl

                # 精度处理
                tp_price = round(tp_price, price_precision)
                sl_price = round(sl_price, price_precision)
                initial_tp = tp_price
                # 立即 更新本地状态
                current_position = {
                    'symbol': SYMBOL,
                    'side': side,
                    'entry_price': actual_entry_price,
                    'size': real_size,
                    'entry_time': time.time(),
                    'tp_price_trigger': tp_price,
                    'sl_price_trigger': sl_price,
                    'tp_trigger_id': '',
                    'sl_trigger_id': '',
                    'peak_price': actual_entry_price,
                    # 初始化最差价格 (用于临近止损监控)
                    'worst_loss_price': actual_entry_price
                }
                # 6. 🚀 第二步：发送止盈和止损的条件单 (Trigger Orders)
                logging.info(
                    f"🔄 正在挂止盈止损单... TP:{tp_price} |{(tp_price / actual_entry_price - 1) * 100 * 10:+.2f}%| SL:{sl_price}|{(sl_price / actual_entry_price - 1) * 100 * 10:+.2f}%|")

                sl_order = create_price_triggered_order(SYMBOL, sl_price, side, real_size, 'SL')

                # 分别发送，避免一个失败影响另一个
                order = FuturesOrder(contract=SYMBOL, size=-real_size, price=tp_price, tif='poc', text='t-entry')
                tp_res = await asyncio.to_thread(futures_api.create_futures_order, SETTLE, order)
                logging.info(f"✅ TP触发单挂单成功 ID: {tp_res.id}")

                sl_res = await asyncio.to_thread(futures_api.create_price_triggered_order, SETTLE, sl_order)
                logging.info(f"✅ SL触发单挂单成功 ID: {sl_res.id}")

                # 7. 更新本地状态
                current_position = {
                    'symbol': SYMBOL,
                    'side': side,
                    'entry_price': actual_entry_price,
                    'size': real_size,
                    'entry_time': time.time(),
                    'tp_price_trigger': tp_price,
                    'sl_price_trigger': sl_price,
                    'tp_trigger_id': tp_res.id,
                    'sl_trigger_id': sl_res.id,
                    'peak_price': actual_entry_price,
                    # 初始化最差价格 (用于临近止损监控)
                    'worst_loss_price': actual_entry_price
                }
                return
            else:
                logging.debug(f"订单 {order_id}未成交 ，继续等待...")

        except Exception as e:
            # 这里的异常可能是网络问题或订单 ID 暂时查不到
            logging.error(f"❌ 查询订单 {order_id} 发生错误: {e}")

        # 暂停，等待下一轮查询
        await asyncio.sleep(poll_interval)
    logging.info(f'开单成交超时{TRADE_WAIT_TIME}s,准备撤单')
    await cancel_all_orders_sync()

async def monitor_and_close():
    global current_position, last_close_time,last_tp_update_time
    # 监控信号
    entry_side, std_dev, entry_price = await update_and_get_signal()
    if entry_side:
        logging.info(f"💡 脉冲反转信号触发: 准备 {entry_side}")
        await asyncio.sleep(ENTRY_DELAY_SECONDS)
        ex_pos = await get_current_position_fast()
        if (current_position is None) and (float(ex_pos['size'])) == 0:
            await safe_place_market_order( entry_side, TARGET_ORDER_USDT, std_dev)
            last_tp_update_time = time.time()
            await asyncio.sleep(0.5)  # 开仓后等待一段时间
        else:
            if current_position is not None:
                if current_position['side'] != entry_side:
                    await forced_close_position(reason="反转平仓")
    # 检测缓存遗漏订单
    if current_position is None:
        await clear_loss_position()
        return
    # 实时监控订单
    pos = current_position
    current_price = await get_current_price_fast()
    if not current_price: return

    # 1. 检查 API 真实持仓
    try:
        # 获取持仓信息
        position = await get_current_position_fast()
        real_size = float(position['size'])
        if real_size == 0:
            logging.warning(f"📉 [检测不到仓位] 交易所持仓为0，可能已止盈或止损或者没有成交。")
            current_price = await get_current_price_fast()

            # 确保在平仓前 current_position 还有值
            if current_position:
                # 使用当前价格作为退出价格，如果 API 没返回成交价
                exit_price = current_price if current_price else pos['entry_price']
                calculate_and_log_pnl(pos['entry_price'], exit_price, pos['side'], pos['entry_time'])

            last_close_time = time.time()
            current_position = None
            # 清理剩余的触发单，以防 SL/TP 成交后另一个订单未取消
            await cancel_all_orders_sync()
            return
    except GateApiException as e:
        # 如果 API 返回 404/400 可能是合约不存在持仓，视为平仓
        if str(e).startswith('(404)') or str(e).startswith('(400)'):
            logging.warning("⚠️ 获取持仓信息失败 (可能无持仓)，视为平仓结束。")
            # 如果交易所没有持仓，本地记录也应该清除
            if current_position:
                current_position = None
                last_close_time = time.time()
            return

        logging.error(f"❌ 获取持仓详情失败: {e}")
        return

    # 2. 检查时间止损
    time_held = time.time() - pos['entry_time']
    if time_held >= MAX_HOLD_TIME_SECONDS:
        logging.warning(f"⏰ [时间止损] 持仓 {time_held:.0f}s 超时")
        await forced_close_position(reason="超时")
        return

    # 3. 打印 PNL (简单估算)
    current_price = await get_current_price_fast()
    if current_price:
        pnl_multiplier = 1 if pos['side'] == 'buy' else -1
        pnl_percent = ((current_price / pos['entry_price'] - 1) * 100 * LEVERAGE * pnl_multiplier)
        logging.info(
                f"📊 [监控] {pos['side']} PNL:{pnl_percent:+.2f}% |持仓金额{pos['size'] * pos['entry_price']*LEVERAGE}u |持仓:{time_held:.0f}s | SL触发价:{pos['sl_price_trigger']} | TP触发价:{pos['tp_price_trigger']}")
async def scalper():
    global current_position,last_close_time
    logging.info("⚙️ 初始化...")
    auto_sync_time()

    await asyncio.to_thread(set_leverage_sync, LEVERAGE)
    await asyncio.to_thread(get_contract_detail)
    await cancel_all_orders_sync() # 确保清除可能存在的旧订单
    asyncio.create_task(price_data_streamer())
    logging.info("✅ 价格流任务已启动。")
    while True:
        try:
            if last_close_time > 0:
                rem = COOLDOWN_AFTER_CLOSE_SECONDS - (time.time() - last_close_time)
                if rem > 0:
                    logging.info(f"❄️ 冷却中... 剩余 {rem:.0f}s")
                    await asyncio.sleep(MONITOR_INTERVAL)
                    continue
            # 1. 监控和时间止损
            await monitor_and_close()
            await update_dynamic_tp_trigger()

            await asyncio.sleep(MONITOR_INTERVAL)
        except Exception as e:
            logging.error(f"🔥 主循环未捕获异常: {e}")
            logging.error(traceback.format_exc())
            await asyncio.sleep(1)

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scalper())


