#!/bin/bash
# 激活虚拟环境并运行数据管理（自动下载模式）
cd "$(dirname "$0")"
source venv/bin/activate
python a_ak_c.py "$@"
