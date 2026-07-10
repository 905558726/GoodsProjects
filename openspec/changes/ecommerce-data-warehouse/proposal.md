## Why

构建一个完整的电商实时数据仓库，将Kafka中的模拟电商流数据（商品、订单）经过Flink实时计算，完成ODS→DWD→DWS→ADS四层数据加工，最终通过Spring Boot可视化平台展示业务指标。解决实时数据处理和可视化分析的需求，形成端到端的数据处理能力。

## What Changes

- **新增 Flink 数据仓库项目（flink-data-warehouse）**：包含3个独立Flink作业Jar包，分别完成ODS→DWD数据清洗、DWD→DWS宽表构建与汇总、DWS→ADS指标计算
- **新增 Spring Boot 可视化项目（dw-visualization）**：提供Web页面展示ADS层指标，包括品类营收、销售趋势、用户价值、商品热销排行、区域销售分析
- **新增 PostgreSQL 数据持久化**：DWD→schema `dwd`，DWS→schema `dws`，ADS→schema `ads`，三层数据独立管理
- **新增 数据模型定义**：覆盖商品、订单、订单明细、宽表、汇总表、指标表共13张表
- **新增 Kafka 消费配置**：连接120.79.232.83服务器，消费ods_products_data和ods_orders_data两个topic

## Capabilities

### New Capabilities

- `flink-ods-to-dwd`: Flink作业，从Kafka消费ODS层原始数据，进行数据清洗（去重、格式校验、字段标准化、JSON解析），将商品数据写入dwd.dwd_product，订单数据拆分后写入dwd.dwd_order和dwd.dwd_order_item
- `flink-dwd-to-dws`: Flink作业，从DWD层读取数据，构建订单商品宽表dws.dws_order_wide，按天汇总商品销售dws.dws_product_sales_1d、品类销售dws.dws_category_sales_1d、用户行为dws.dws_user_order_1d
- `flink-dws-to-ads`: Flink作业，从DWS层读取汇总数据，计算业务指标：品类营收排行ads.ads_category_revenue、每日销售趋势ads.ads_daily_sales_trend、用户价值分析ads.ads_user_value、商品热销排行ads.ads_product_ranking、区域销售分析ads.ads_regional_sales
- `spring-visualization`: Spring Boot Web应用，提供REST API和前端页面，以图表形式展示ADS层5张指标表数据，支持时间筛选、品类筛选、TopN切换

### Modified Capabilities

<!-- 无现有能力修改，全新项目 -->

## Impact

- **基础设施依赖**: Kafka (120.79.232.83:9092)、PostgreSQL (120.79.232.83:5432/DataWarehouse)
- **开发环境**: JDK 17、Maven 3.6.1、本地Maven仓库 E:\.maven\local_maven
- **代码仓库**: E:\.java\GoodsProjects，绑定 GitHub 远程仓库 `https://github.com/905558726/GoodsProjects.git`，严格遵循 Conventional Commits 提交规范
- **Flink运行**: Flink Jar 部署至 `root@120.79.232.83:/opt/flink-jobs/` 以独立 JVM 进程运行（`-Xmx256M`）
- **部署上线**: Flink Jar 通过 SSH 远程部署测试，Spring Boot 本地运行可视化
