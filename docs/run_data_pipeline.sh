#!/bin/bash
# 生成数据 + Flink 集群自动处理
# ODS->DWD 已作为 Flink 常驻 Job 运行，无需手动触发
# 生成后等 DWS/ADS 自动周期刷新
set -e
PY="/opt/DataGenerate/venv/bin/python"
GEN="/opt/DataGenerate"
LOG="/opt/flink-jobs/pipeline.log"

PC=$((RANDOM % 6 + 5))
OC=$((PC / 2 + RANDOM % (PC / 2 + 1)))
((OC < 1)) && OC=1

exec >> "$LOG" 2>&1
echo "=== $(date '+%Y-%m-%d %H:%M:%S') GEN: P=$PC O=$OC ==="
cd "$GEN"
$PY scripts/product_generator.py --output kafka://localhost:9092/ods_products_data --count "$PC" --file-backup output/products.json | tail -1
$PY scripts/order_generator.py --output kafka://localhost:9092/ods_orders_data --count "$OC" --products output/products.json | tail -1
echo "=== DONE (Flink auto-consume) ==="
