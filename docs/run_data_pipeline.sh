#!/bin/bash
# ==============================================================================
# 电商数仓端到端数据管道 — Flink 集群提交模式
# 日志: /opt/flink-jobs/pipeline.log
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

echo "[1/4] Generate"
cd "$GEN"
$PY scripts/product_generator.py --output kafka://localhost:9092/ods_products_data --count "$PC" | tail -1
$PY scripts/order_generator.py --output kafka://localhost:9092/ods_orders_data --count "$OC" | tail -1

echo "[2/4] ODS->DWD (flink run)"
$FLINK run -c com.dw.job.OdsToDwdJob "$JAR" --kafka localhost:9092 --db jdbc:postgresql://localhost:5432/DataWarehouse 2>&1 | grep -E 'Products|Orders|FINISHED|Error'

echo "[3/4] DWD->DWS (flink run)"
$FLINK run -c com.dw.job.DwdToDwsJob "$JAR" 2>&1 | grep -E 'rows|done|Error'

echo "[4/4] DWS->ADS (flink run)"
$FLINK run -c com.dw.job.DwsToAdsJob "$JAR" 2>&1 | grep -E 'rows|done'

echo "=== $(date '+%Y-%m-%d %H:%M:%S') DONE ==="
