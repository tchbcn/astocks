import datetime
import pytz
import g_win0
import g_f
# -------------------------- 核心配置参数（含夏令时/冬令时动态配置）--------------------------
# 1. 固定时间配置（北京时间）
A股_OPEN_MORNING = (9, 30)  # A股早盘开盘时间
A股_CLOSE_MORNING = (11, 30)  # A股早盘收盘时间
A股_OPEN_AFTERNOON = (13, 0)  # A股午后开盘时间
A股_CLOSE_AFTERNOON = (15, 0)  # A股午后收盘时间
EU_OPEN = (18, 0)  # 欧股开盘时间（无夏令时影响）


# 2. 美股时段动态配置（根据夏令时/冬令时自动切换）
def get_us_time_config():
    """根据当前日期判断美股是夏令时还是冬令时，返回动态时间配置（北京时间）"""
    tz_sh = pytz.timezone('Asia/Shanghai')
    tz_ny = pytz.timezone('America/New_York')
    now_sh = datetime.datetime.now(tz_sh)
    now_ny = now_sh.astimezone(tz_ny)

    # 美股夏令时（EDT）：每年3月第二个周日 02:00（纽约时间）至 11月第一个周日 02:00（纽约时间）
    # 美股冬令时（EST）：其余时间
    is_dst = now_ny.dst() != datetime.timedelta(0)  # 判断是否为夏令时（DST=Daylight Saving Time）

    if is_dst:
        return {
            "US_OPEN_PRE": (20, 30),  # 美股开盘预热（夏令时：美股21:30开盘，提前1小时预热）
            "US_OPEN": (21, 30),  # 美股开盘时间（北京时间）
            "US_CLOSE_EARLY": (23, 0)  # 美股早盘结束（北京时间）
        }
    else:
        return {
            "US_OPEN_PRE": (21, 30),  # 美股开盘预热（冬令时：美股22:30开盘，提前1小时预热）
            "US_OPEN": (22, 30),  # 美股开盘时间（北京时间）
            "US_CLOSE_EARLY": (0, 0)  # 美股早盘结束（北京时间，次日0点）
        }


# 3. 行情信号阈值（适配1分钟级判断）
BTC_VOLATILITY_THRESHOLD = 0.1  # BTC 1分钟波动阈值（%）
ORDER_BOOK_INCREMENT = 30  # 挂单量增量阈值（%）
FUNDING_RATE_CHANGE = 0.02  # 资金费率1分钟变动阈值（%）
VOLUME_THRESHOLD = 50  # 山寨币1分钟成交量阈值（万美元）


# -------------------------- 模拟数据接口（请替换为Gate.io真实接口）--------------------------
def get_btc_1min_volatility():
    """获取BTC 1分钟涨跌幅（%），替换为Gate.io API"""
    return 0.08  # 模拟数据，实际替换为真实接口返回值


def get_order_book_change():
    """获取买一至买三挂单量1分钟增量（%），替换为Gate.io API"""
    return 35  # 模拟数据，实际替换为真实接口返回值


def get_funding_rate_change():
    """获取资金费率1分钟变动（%），替换为Gate.io API"""
    return 0.03  # 模拟数据，实际替换为真实接口返回值


def get_altcoin_1min_volume():
    """获取山寨币1分钟成交量（万美元），替换为Gate.io API"""
    return 60  # 模拟数据，实际替换为真实接口返回值


# -------------------------- 核心判断函数 --------------------------
def get_current_time_period():
    """判断当前北京时间所属时段（含美股夏令时/冬令时适配）"""
    tz_sh = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz_sh)
    hour, minute = now.hour, now.minute
    weekday = now.weekday()  # 0=周一，4=周五，5=周六，6=周日
    us_config = get_us_time_config()  # 动态获取美股时段配置

    # 周末判断（周六、周日）
    if weekday in (5, 6):
        if (9 <= hour < 22) and not (us_config["US_OPEN_PRE"][0] <= hour < us_config["US_OPEN"][0]):
            return "周末震荡"
        elif us_config["US_OPEN_PRE"][0] <= hour < us_config["US_OPEN"][0]:
            return "周末弱趋势"
        else:
            return "周末禁止交易"

    # 周五判断
    if weekday == 4:
        # A股交易时段（震荡）
        a股_morning = (hour == A股_OPEN_MORNING[0] and minute >= A股_OPEN_MORNING[1]) or \
                      (A股_OPEN_MORNING[0] < hour < A股_CLOSE_MORNING[0]) or \
                      (hour == A股_CLOSE_MORNING[0] and minute < A股_CLOSE_MORNING[1])
        a股_afternoon = (hour == A股_OPEN_AFTERNOON[0] and minute >= A股_OPEN_AFTERNOON[1]) or \
                        (A股_OPEN_AFTERNOON[0] < hour < A股_CLOSE_AFTERNOON[0]) or \
                        (hour == A股_CLOSE_AFTERNOON[0] and minute < A股_CLOSE_AFTERNOON[1])

        if a股_morning or a股_afternoon:
            return "周五A股时段震荡"
        elif 15 <= hour < 18:
            return "周五A股闭市弱趋势"
        elif 18 <= hour < us_config["US_OPEN_PRE"][0]:
            return "周五欧美预热弱趋势"
        elif us_config["US_OPEN_PRE"][0] <= hour < us_config["US_OPEN"][0]:
            return "周五美股开盘中强趋势"
        else:
            return "周五禁止交易"

    # 周一至周四判断
    else:
        # A股交易时段（强震荡）
        a股_morning = (hour == A股_OPEN_MORNING[0] and minute >= A股_OPEN_MORNING[1]) or \
                      (A股_OPEN_MORNING[0] < hour < A股_CLOSE_MORNING[0]) or \
                      (hour == A股_CLOSE_MORNING[0] and minute < A股_CLOSE_MORNING[1])
        a股_afternoon = (hour == A股_OPEN_AFTERNOON[0] and minute >= A股_OPEN_AFTERNOON[1]) or \
                        (A股_OPEN_AFTERNOON[0] < hour < A股_CLOSE_AFTERNOON[0]) or \
                        (hour == A股_CLOSE_AFTERNOON[0] and minute < A股_CLOSE_AFTERNOON[1])

        if a股_morning or a股_afternoon:
            return "工作日A股时段强震荡"
        elif 11 <= hour < 13:
            return "工作日A股午间震荡弱趋势"
        elif 15 <= hour < 16:
            return "工作日A股闭市弱趋势"
        elif 16 <= hour < 18:
            return "工作日震荡为主"
        elif 18 <= hour < us_config["US_OPEN_PRE"][0]:
            return "工作日欧股开盘弱趋势转强"
        elif us_config["US_OPEN_PRE"][0] <= hour < us_config["US_CLOSE_EARLY"][0] or \
                (us_config["US_CLOSE_EARLY"][0] == 0 and hour >= us_config["US_OPEN_PRE"][0]):
            return "工作日欧美重叠强趋势"
        elif us_config["US_CLOSE_EARLY"][0] <= hour < 24:
            return "工作日美股早盘中强趋势"
        else:
            return "工作日禁止交易"


def is_trend_condition_met(time_period):
    """根据时段+行情信号判断是否满足趋势条件"""
    # 强趋势时段（直接满足趋势条件，无需额外信号）
    strong_trend_periods = ["工作日欧美重叠强趋势", "周五美股开盘中强趋势"]
    if time_period in strong_trend_periods:
        return True

    # 弱趋势时段（需满足2个及以上行情信号）
    weak_trend_periods = ["工作日A股闭市弱趋势", "工作日欧股开盘弱趋势转强", "工作日美股早盘中强趋势",
                          "周五A股闭市弱趋势", "周五欧美预热弱趋势", "周末弱趋势"]
    if time_period in weak_trend_periods:
        signals = 0
        if get_btc_1min_volatility() >= BTC_VOLATILITY_THRESHOLD:
            signals += 1
        if get_order_book_change() >= ORDER_BOOK_INCREMENT:
            signals += 1
        if abs(get_funding_rate_change()) >= FUNDING_RATE_CHANGE:
            signals += 1
        if get_altcoin_1min_volume() >= VOLUME_THRESHOLD:
            signals += 1
        return signals >= 2

    # 震荡时段（直接返回False）
    return False


# -------------------------- 交易函数占位符（请替换为你的实际交易代码）--------------------------
def oscillation_trade():
    """震荡行情交易函数（你后续替换为自己的代码）"""
    print("当前为震荡行情，执行震荡交易逻辑")  # 一行占位符，替换为你的实际代码


def trend_trade():
    """趋势行情交易函数（你后续替换为自己的代码）"""
    print("当前为趋势行情，执行趋势交易逻辑")  # 一行占位符，替换为你的实际代码


# -------------------------- 主运行逻辑 --------------------------
if __name__ == "__main__":
    # 1. 获取当前时段（含夏令时/冬令时适配）
    current_period = get_current_time_period()
    us_config = get_us_time_config()
    print(f"当前时段：{current_period}")
    print(
        f"美股当前时段配置（夏令时={us_config['US_OPEN'] == (21, 30)}）：预热{us_config['US_OPEN_PRE']}，开盘{us_config['US_OPEN']}，早盘结束{us_config['US_CLOSE_EARLY']}")

    # 2. 判断是否禁止交易
    if "禁止交易" in current_period:
        print("当前为禁止交易时段，暂停所有操作")
    else:
        # 3. 判断行情性质并调用对应函数
        if is_trend_condition_met(current_period):
            trend_trade()  # 趋势行情：调用趋势交易函数
        else:
            oscillation_trade()  # 震荡行情：调用震荡交易函数