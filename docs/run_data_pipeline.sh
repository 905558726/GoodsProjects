#!/bin/bash
# ==============================================================================
# 电商数仓端到端数据管道 — Flink 集群常驻模式
# 数据生成后立即触发 DWS/ADS 链路处理
# ==============================================================================
set -e
LOG_FILE="/opt/flink-jobs/pipeline.log"
JAR="/opt/flink-jobs/flink-data-warehouse.jar"
PY="/opt/DataGenerate/venv/bin/python"
GEN="/opt/DataGenerate"
FLINK="/opt/flink-1.19.3/bin/flink"
export JAVA_HOME=/opt/jdk17

PC=$((RANDOM % 6 + 5))
OC=$((PC / 2 + RANDOM % (PC / 2 + 1)))
((OC < 1)) && OC=1

exec >> "$LOG_FILE" 2>&1
echo
echo "=== $(date '+%Y-%m-%d %H:%M:%S') START: P=$PC O=$OC ==="

echo "[1/4] Generate -> Kafka"
cd "$GEN"
$PY scripts/product_generator.py --output kafka://localhost:9092/ods_products_data --count "$PC" | tail -1
$PY scripts/order_generator.py --output kafka://localhost:9092/ods_orders_data --count "$OC" | tail -1

# ODS->DWD 已作为 Flink Streaming Job 常驻运行，数据自动消费
# 等待几秒确保数据已被消费
sleep 8

echo "[2/4] DWD->DWS (flink run)"
$FLINK run -c com.dw.job.DwdToDwsJob "$JAR" 2>&1 | grep -E 'rows|done|Error'

echo "[3/4] DWS->ADS (flink run)"
$FLINK run -c com.dw.job.DwsToAdsJob "$JAR" 2>&1 | grep -E 'rows|done'

echo "=== $(date '+%Y-%m-%d %H:%M:%S') DONE ==="
