#!/bin/bash
# 后台持续运行: nohup ./run_loop.sh &
# 停止: kill $(cat /tmp/pipeline_loop.pid)
echo $$ > /tmp/pipeline_loop.pid
while true; do
  /opt/flink-jobs/run_data_pipeline.sh
  # 随机间隔 120~600 秒 (2~10 分钟)
  SLEEP=$((RANDOM % 481 + 120))
  echo "Next run in ${SLEEP}s..."
  sleep $SLEEP
done
