# A股量化交易系统 (astocks)

专注于A股市场的量化交易策略与研究。

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 运行系统

```bash
# 显示菜单选择策略
python main.py

# 直接运行特定策略
python a_mul.py

# 快速测试环境
python main.py --test

# 列出所有策略
python main.py --list
```

## 📦 项目结构

```
astocks/
├── main.py              # 主入口（统一启动）
├── a/                   # A股策略子模块
│   ├── a_h.py          # 策略H（基础版）
│   ├── a_h_c.py        # 策略H-C（复合版）
│   └── a_h_c_t.py      # 策略H-C-T（增强版）
├── a_ak_c.py           # 数据下载核心（akshare + 断点续传）
├── a_cap_sl.py         # 市值流动性策略
├── a_one.py            # 单因子策略
├── a_high.py           # 高频策略
├── a_mul.py            # 多因子微盘股策略（推荐）
├── gs_aShare.py        # 两会特别策略（AI+机器人+半导体）
├── log.py              # 日志模块
├── t_simple.py         # AkShare API快速测试
├── u.py                # 数据诊断工具
├── README.md           # 项目说明
├── requirements.txt    # Python依赖
└── .gitignore         # Git忽略规则
```

## 🎯 策略说明

| 策略 | 文件 | 说明 | 适用场景 |
|------|------|------|----------|
| 多因子微盘股 | `a_mul.py` | 市值+动量多因子，10只持仓 | 小盘股回测 |
| 两会特别版 | `gs_aShare.py` | 政策主题（AI/机器人/半导体） | 事件驱动 |
| 市值流动性 | `a_cap_sl.py` | 市值+流动性因子 | 中盘股 |
| 单因子 | `a_one.py` | 简化单因子模型 | 学习研究 |
| 高频 | `a_high.py` | 短周期交易 | 高频数据 |
| 策略H系列 | `a_h*.py` | 策略变体 | 算法研究 |

## 📊 数据源

- **akshare**：免费A股数据（默认）
- **tushare**：需要token，数据更全面（可选）

配置tushare token（可选）：
```python
# 在策略文件中设置
import tushare as ts
ts.set_token('YOUR_TOKEN')
```

## 🔧 数据库

支持两种后端：

### SQLite（默认，开箱即用）
无需配置，自动创建 `stock_data.db`

### ClickHouse（高性能，推荐大量数据）
修改配置文件：
```python
CH_HOST = 'localhost'
CH_PORT = 8123
CH_USER = 'default'
CH_PASSWORD = ''
CH_DATABASE = 'stock_db'
```

## 📈 运行示例

```bash
# 1. 数据下载（首次运行）
python a_ak_c.py
# 按菜单选择下载/补全数据

# 2. 运行回测
python a_mul.py
# 选择 "2. 运行历史回测"

# 3. 查看结果
# 回测结果保存在内存中，可导出为CSV
```

## 📁 数据目录

首次运行后会自动生成：
- `data/` - 原始数据缓存
- `store/` - ClickHouse本地存储
- `*.db` - SQLite数据库文件
- `*.log` - 运行日志

> 注意：大文件已加入`.gitignore`，不会上传到Git

## ⚙️ 参数配置

每个策略文件头部都有配置区，可修改：
- `INITIAL_CASH`：初始资金（默认10万）
- `BACKTEST_START/END`：回测起止日期
- `DATA_START_DATE`：数据起始日期
- 策略特定参数（如因子权重、止损止盈等）

## 🧪 快速测试

运行以下命令验证环境：
```bash
python main.py --test
python t_simple.py
python u.py
```

## 📚 依赖说明

### 必需
- `akshare` - A股数据源
- `pandas`, `numpy` - 数据处理
- `clickhouse-connect` - ClickHouse驱动（如使用）
- `tushare` - 备用数据源（可选）

### 机器学习（部分策略需要）
- `scikit-learn`
- `tensorflow` 或 `torch`

## ⚠️ 风险提示

本项目仅供学习研究使用，不构成投资建议。实盘交易请谨慎评估风险，建议先用模拟盘验证策略。

## 📝 更新日志

- **v1.0** (2025-03-29) - 初始版本，独立自buyers项目
  - 专注A股量化交易
  - 移除所有加密货币代码
  - 统一主入口
  - 完善文档
