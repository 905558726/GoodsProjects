#!/bin/bash
# 后台持续运行: nohup /opt/flink-jobs/run_loop.sh &
# 停止: kill $(cat /tmp/pipeline_loop.pid)
echo $$ > /tmp/pipeline_loop.pid

# 服务器资源约束: 仅 1.6GB 内存, 2 核
# 策略: 少量数据 + 长间隔 + 串行执行, 避免压崩服务
while true; do
  /opt/flink-jobs/run_data_pipeline.sh
  # 随机间隔 600~1200 秒 (10~20 分钟), 避免频繁执行耗尽资源
  SLEEP=$((RANDOM % 601 + 600))
  echo "$(date) Next run in ${SLEEP}s..."
  sleep $SLEEP
done
