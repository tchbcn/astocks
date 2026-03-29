# A股量化交易系统 (astocks)

专注于A股市场的量化交易策略与研究。

## 项目结构

```
astocks/
├── a/                    # A股策略子模块
│   ├── a_h.py           # A股策略H
│   ├── a_h_c.py         # A股策略H-C
│   └── a_h_c_t.py       # A股策略H-C-T
├── a_ak_c.py            # A股数据下载核心（akshare）
├── a_cap_sl.py          # 市值流动性策略
├── a_one.py             # 单因子策略
├── a_high.py            # 高频策略
├── a_mul.py             # 多因子微盘股策略
├── gs_aShare.py         # A股两会特别策略
├── log.py               # 日志模块
├── t_simple.py          # 快速测试脚本
└── u.py                 # 诊断工具
```

## 依赖

- Python 3.9+
- akshare (A股数据)
- pandas
- numpy
- clickhouse-connect (可选，用于高性能数据库)
- tushare (可选，需要token)
- scikit-learn
- tensorflow / torch (部分策略需要)

## 快速开始

1. 克隆仓库
2. 安装依赖：`pip install -r requirements.txt`
3. 配置数据库连接（默认使用SQLite，可选ClickHouse）
4. 运行策略：
   ```bash
   python a_mul.py      # 多因子微盘股策略
   python gs_aShare.py  # 两会特别策略
   python t_simple.py   # 快速测试
   ```

## 策略说明

- **多因子微盘股策略 (a_mul.py)**：基于市值、动量等多因子选股，适合小盘股回测
- **两会特别策略 (gs_aShare.py)**：紧跟两会政策主线（AI+机器人+半导体）
- **市值流动性策略 (a_cap_sl.py)**：结合市值与流动性因子
- **高频策略 (a_high.py)**：短周期交易策略
- **单因子策略 (a_one.py)**：简化版单因子回测

## 数据源

- **akshare**：免费A股数据（主要）
- **tushare**：需要token，数据更全面（可选）

## 目录说明

- `a/` - 策略变体及核心算法模块
- `a_ak_c.py` - 数据下载与管理模块（支持断点续传）
- `log.py` - 统一的日志配置
- `u.py` - 数据下载诊断工具
- `t_simple.py` - AkShare API快速测试

## 风险提示

本项目仅供学习研究使用，实盘交易请谨慎评估风险。
