#!/bin/bash
# ==============================================================================
# 电商数仓端到端数据管道 — 持续随机生成 + Flink 实时消费
# 用法: nohup /opt/flink-jobs/run_loop.sh &
# 停止: kill $(cat /tmp/pipeline_loop.pid)
# ==============================================================================
echo $$ > /tmp/pipeline_loop.pid

while true; do
  /opt/flink-jobs/run_data_pipeline.sh
  SLEEP=$((RANDOM % 11 + 5))
  echo "$(date) Next run in ${SLEEP}s..."
  sleep $SLEEP
done
