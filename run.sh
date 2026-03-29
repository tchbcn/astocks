#!/bin/bash
# 激活虚拟环境并运行主程序
cd "$(dirname "$0")"
source venv/bin/activate
python main.py "$@"
