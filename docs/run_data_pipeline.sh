#!/bin/bash
set -e
LOG_FILE="/opt/flink-jobs/pipeline.log"
JAR="/opt/flink-jobs/flink-data-warehouse.jar"
PY="/opt/DataGenerate/venv/bin/python"
GEN="/opt/DataGenerate"

PC=$((RANDOM % 13 + 3))
OC=$((RANDOM % 41 + 10))

exec >> "$LOG_FILE" 2>&1
echo
echo "=== $(date '+%Y-%m-%d %H:%M:%S') START: P=$PC O=$OC ==="

echo "[1/4] Generate"
cd "$GEN"
$PY scripts/product_generator.py --output kafka://localhost:9092/ods_products_data --count "$PC" | tail -1
$PY scripts/order_generator.py --output kafka://localhost:9092/ods_orders_data --count "$OC" | tail -1

echo "[2/4] ODS->DWD"
java -Xmx256M -cp "$JAR" com.dw.job.OdsToDwdJob | tail -1

echo "[3/4] DWD->DWS"
java -Xmx256M -cp "$JAR" com.dw.job.DwdToDwsJob | tail -4

echo "[4/4] DWS->ADS"
java -Xmx256M -cp "$JAR" com.dw.job.DwsToAdsJob | tail -5

echo "=== $(date '+%Y-%m-%d %H:%M:%S') DONE ==="
