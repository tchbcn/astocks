#!/bin/bash
# 激活虚拟环境并直接运行多因子策略
cd "$(dirname "$0")"
source venv/bin/activate
python a_mul.py "$@"
