# A股量化交易系统 (astocks)

专注于A股市场的量化交易策略与研究。

## 项目结构

```
astocks/
├── a/                    # A股策略子模块
│   ├── a_h.py           # A股策略H
│   ├── a_h_c.py         # A股策略H-C
│   └── a_h_c_t.py       # A股策略H-C-T
├── a_ak_c.py            # A股数据下载核心模块
├── a_cap_sl.py          # 市值流动性策略
├── a_one.py             # 单因子策略
├── a_high.py            # 高频策略
├── a_mul.py             # 多因子微盘股策略
├── bs.py                # 股票数据库管理
├── g.py                 # 数据管理工具
├── gs.py                # A股策略引擎
├── gs_aShare.py         # A股两会特别策略
├── log.py               # 日志模块
├── t_simple.py          # 快速测试脚本
└── u.py                 # 诊断工具
```

## 依赖

- Python 3.9+
- akshare
- pandas
- numpy
- clickhouse-connect
- tushare (可选)
- 其他策略特定依赖

## 快速开始

1. 克隆仓库
2. 安装依赖：`pip install -r requirements.txt` (待补充)
3. 配置数据库连接（ClickHouse或SQLite）
4. 运行策略：
   ```bash
   python a_mul.py      # 多因子策略
   python gs_aShare.py  # 两会特别策略
   ```

## 策略说明

- **多因子微盘股策略**：基于市值、动量等多因子选股
- **两会特别策略**：紧跟两会政策主线（AI+机器人+半导体）
- **市值流动性策略**：结合市值与流动性因子
- **高频策略**：短周期交易策略

## 数据源

- 主要：akshare (免费)
- 可选：tushare (需要token)

## 风险提示

本项目仅供学习研究使用，实盘交易请谨慎评估风险。
