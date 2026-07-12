#!/bin/bash
# ==============================================================================
# 电商数仓端到端数据管道
# 资源约束: 1.6GB 内存 / 2 核, 串行执行避免压崩服务
# 日志: /opt/flink-jobs/pipeline.log
# ==============================================================================
set -e
LOG_FILE="/opt/flink-jobs/pipeline.log"
JAR="/opt/flink-jobs/flink-data-warehouse.jar"
PY="/opt/DataGenerate/venv/bin/python"
GEN="/opt/DataGenerate"

# 少量数据: 商品 2~8, 订单 5~25, 避免 Kafka 堆积和 DB 压力
PC=$((RANDOM % 7 + 2))
OC=$((RANDOM % 21 + 5))

exec >> "$LOG_FILE" 2>&1
echo
echo "=== $(date '+%Y-%m-%d %H:%M:%S') START: P=$PC O=$OC ==="

echo "[1/4] Generate"
cd "$GEN"
$PY scripts/product_generator.py --output kafka://localhost:9092/ods_products_data --count "$PC" | tail -1
$PY scripts/order_generator.py --output kafka://localhost:9092/ods_orders_data --count "$OC" | tail -1

echo "[2/4] ODS->DWD"
java -Xmx256M -cp "$JAR" com.dw.job.OdsToDwdJob | tail -1

# 每个 Job 结束后等待 5s 释放内存
sleep 5

echo "[3/4] DWD->DWS"
java -Xmx256M -cp "$JAR" com.dw.job.DwdToDwsJob | tail -4

sleep 5

echo "[4/4] DWS->ADS"
java -Xmx256M -cp "$JAR" com.dw.job.DwsToAdsJob | tail -5

echo "=== $(date '+%Y-%m-%d %H:%M:%S') DONE ==="
