## ADDED Requirements

### Requirement: Kafka Source消费商品数据
系统 SHALL 从Kafka Topic `ods_products_data` 消费商品JSON消息，解析为ProductEvent对象，支持从最早offset消费和Checkpoint恢复。

#### Scenario: 成功消费商品消息
- **WHEN** Kafka Topic中有新的商品JSON消息
- **THEN** Flink Source将消息反序列化为ProductEvent，包含product_id、name、brand、category、sub_category、price等完整字段

#### Scenario: JSON解析失败
- **WHEN** 消息JSON格式不合法或必填字段缺失
- **THEN** 系统记录错误日志并跳过该消息（死信队列），不影响后续消息处理

### Requirement: Kafka Source消费订单数据
系统 SHALL 从Kafka Topic `ods_orders_data` 消费订单JSON消息，解析为OrderEvent对象，包含嵌套的items、buyer、shipping_address、payment、logistics结构。

#### Scenario: 成功消费订单消息
- **WHEN** Kafka Topic中有新的订单JSON消息
- **THEN** Flink Source将消息反序列化为OrderEvent，嵌套结构完整解析

#### Scenario: 订单items为空数组
- **WHEN** 订单消息中items数组为空
- **THEN** 系统记录告警日志并跳过该订单（无效订单）

### Requirement: 商品数据清洗写入DWD
系统 SHALL 对ProductEvent进行字段清洗（price保留2位小数、created_at标准化为TIMESTAMP、NULL字段填充默认值），并写入dwd.dwd_product表。

#### Scenario: 商品数据清洗成功
- **WHEN** ProductEvent数据经过清洗转换
- **THEN** dwd.dwd_product表插入或更新该商品记录，以product_id为主键UPSERT

#### Scenario: 商品price为负数
- **WHEN** 商品price字段为负数
- **THEN** 系统将price修正为0.00并标记该记录为异常数据

### Requirement: 订单数据拆分写入DWD
系统 SHALL 将OrderEvent拆分为订单主表记录和订单明细记录，order_status映射为中文状态码，payment和logistics提取到订单主表，items展开为独立明细行。

#### Scenario: 订单拆分写入成功
- **WHEN** OrderEvent包含2个items
- **THEN** dwd.dwd_order表写入1条订单主记录，dwd.dwd_order_item表写入2条明细记录，通过order_id关联

#### Scenario: 订单状态为已取消
- **WHEN** order_status为"已取消"且无物流信息
- **THEN** dwd.dwd_order中payment字段为NULL，logistics字段为NULL，actual_amount为0

### Requirement: Flink作业容错与Checkpoint
系统 SHALL 启用Flink Checkpoint机制（间隔60秒），支持Exactly-Once语义，作业失败重启后从最近Checkpoint恢复。

#### Scenario: 作业异常重启
- **WHEN** Flink作业因异常崩溃后重启
- **THEN** 从最近成功Checkpoint恢复消费offset和状态，不丢失不重复处理数据

### Requirement: OdsToDwdJob远程部署与验证
系统 SHALL 编译打包后部署至服务器 `120.79.232.83`，成功消费 Kafka 数据并写入 PostgreSQL DWD 层，通过 SQL 查询验证数据完整性和正确性。

#### Scenario: Jar包编译与上传
- **WHEN** 执行 `mvn clean package -pl flink-data-warehouse -DskipTests`
- **THEN** 生成 `flink-data-warehouse-1.0.jar`，scp 上传至 `root@120.79.232.83:/opt/flink-jobs/`

#### Scenario: Kafka Topic自动创建
- **WHEN** 首次启动 OdsToDwdJob 且 Topic `ods_products_data` / `ods_orders_data` 不存在
- **THEN** 自动创建 Topic（或通过 `kafka-topics.sh --create` 手动创建），partition=1, replication=1

#### Scenario: 消费商品数据并写入DWD
- **WHEN** 通过 `ssh root@120.79.232.83 "sh /usr/local/bin/generate.sh -p 10 -o 20"` 生成 10 商品 + 20 订单灌入 Kafka 后，启动 OdsToDwdJob
- **THEN** `PGPASSWORD=<DB_PASSWORD> psql -h <DB_HOST> -U <DB_USER> -d DataWarehouse -c "SELECT count(*) FROM dwd.dwd_product"` 返回 10，`SELECT count(*) FROM dwd.dwd_order` 返回 20

#### Scenario: 低内存模式运行
- **WHEN** 服务器内存不足 512MB 空闲
- **THEN** Job 以 `java -Xmx256M` 启动，不触发 OOM Killer，正常完成消费

#### Scenario: 数据幂等重跑
- **WHEN** 同一批数据再次消费（重启 Job 从 earliest offset 消费）
- **THEN** DWD 表中记录数不变（UPSERT 语义），无重复数据
