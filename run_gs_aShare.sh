#!/bin/bash
# 激活虚拟环境并运行两会特别策略
cd "$(dirname "$0")"
source venv/bin/activate
python gs_aShare.py "$@"
