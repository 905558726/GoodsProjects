#!/bin/bash
# 后台持续运行: nohup /opt/flink-jobs/run_loop.sh &
# 停止: kill $(cat /tmp/pipeline_loop.pid)
echo $$ > /tmp/pipeline_loop.pid

# 随机间隔 30~60 秒, 模拟用户下单的随机性
while true; do
  /opt/flink-jobs/run_data_pipeline.sh
  SLEEP=$((RANDOM % 31 + 30))
  echo "$(date) Next run in ${SLEEP}s..."
  sleep $SLEEP
done
