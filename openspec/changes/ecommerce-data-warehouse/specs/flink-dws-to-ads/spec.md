## ADDED Requirements

### Requirement: 品类营收排行指标
系统 SHALL 从dws_category_sales_1d汇总计算品类营收排行，按总营收降序排列，包含销售额、销量、订单数、SKU数、占比，写入ads.ads_category_revenue。

#### Scenario: 计算品类营收排行
- **WHEN** DWS层有多个品类的日销售汇总数据
- **THEN** ads_category_revenue按总营收降序排列，每个品类计算sales_amount占总体的百分比，rank为排名序号

#### Scenario: 品类数据更新
- **WHEN** 新一天的销售数据进入DWS层
- **THEN** ads_category_revenue全量刷新，重新计算累计排名和占比

### Requirement: 每日销售趋势指标
系统 SHALL 从dws_category_sales_1d按天汇总所有品类，生成每日总销售趋势（销售额、订单数、客单价），写入ads.ads_daily_sales_trend。

#### Scenario: 计算每日趋势
- **WHEN** DWS层有连续多天的日销售数据
- **THEN** ads_daily_sales_trend每行包含dt、total_sales_amount、total_order_count、avg_order_amount（客单价=销售额/订单数）

#### Scenario: 某天无销售数据
- **WHEN** 某天所有品类均无销售记录
- **THEN** ads_daily_sales_trend中该天不产生记录（不补零）

### Requirement: 用户价值分析RFM指标
系统 SHALL 从dws_user_order_1d计算每个用户的RFM指标（最近购买日期Recency、购买频率Frequency、购买金额Monetary），写入ads.ads_user_value。

#### Scenario: 计算用户RFM
- **WHEN** DWS层有用户多天的购买行为数据
- **THEN** ads_user_value包含user_phone、last_order_date（最近购买日期）、order_frequency（总下单次数）、total_monetary（累计消费金额）、avg_order_amount（平均客单价）

#### Scenario: 用户价值分层
- **WHEN** RFM指标计算完成
- **THEN** 系统基于total_monetary将用户分为3层：高价值（≥10000元）、中价值（1000-10000元）、低价值（<1000元），写入value_tier字段

### Requirement: 商品热销排行指标
系统 SHALL 从dws_product_sales_1d汇总计算商品销量TopN排行，写入ads.ads_product_ranking。

#### Scenario: 商品热销排行Top20
- **WHEN** DWS层有商品日销售数据
- **THEN** ads_product_ranking取累计销量前20的商品，包含product_id、product_name、category、brand、total_quantity、total_amount、rank

#### Scenario: 新增热销商品
- **WHEN** 原来不在Top20的商品累计销量进入前20
- **THEN** ads_product_ranking全量刷新，原Top20末尾商品被替换

### Requirement: 区域销售分析指标
系统 SHALL 从dws_order_wide按省市区汇总销售数据，写入ads.ads_regional_sales。

#### Scenario: 省级销售汇总
- **WHEN** dws_order_wide中有多个省份的订单数据
- **THEN** ads_regional_sales按province汇总，包含total_orders、total_amount、avg_order_amount、buyer_count（去重买家数），按total_amount降序排列

#### Scenario: 同省不同市区分别统计
- **WHEN** 广东省下有深圳市和广州市的订单
- **THEN** ads_regional_sales中广东省的总计数据为两市之和，同时保留city和district维度的明细记录

### Requirement: DwsToAdsJob远程部署与验证
系统 SHALL 编译打包后部署至服务器 `120.79.232.83`，从 DWS 层读取数据计算 ADS 五张指标表，通过 SQL 查询验证计算结果正确性。

#### Scenario: Jar包编译与上传
- **WHEN** DWS 层数据就绪后编译并上传 Jar
- **THEN** Jar 成功部署至 `/opt/flink-jobs/`，Job 以 `java -Xmx256M` 启动

#### Scenario: 品类营收排行验证
- **WHEN** DwsToAdsJob 执行完成
- **THEN** `SELECT * FROM ads.ads_category_revenue ORDER BY rank` 返回品类营收排行，rank 连续无跳号，percentage 合计约等于 100%

#### Scenario: 每日销售趋势验证
- **WHEN** DwsToAdsJob 执行完成
- **THEN** `SELECT * FROM ads.ads_daily_sales_trend ORDER BY dt` 每天一条记录，avg_order_amount = total_sales_amount / total_order_count

#### Scenario: 用户价值RFM验证
- **WHEN** DwsToAdsJob 执行完成
- **THEN** `SELECT * FROM ads.ads_user_value` value_tier 分层正确：高价值（total_monetary ≥ 10000）、中价值（1000-10000）、低价值（< 1000）

#### Scenario: 商品热销排行验证
- **WHEN** DwsToAdsJob 执行完成
- **THEN** `SELECT * FROM ads.ads_product_ranking ORDER BY rank LIMIT 10` 返回 Top10 热销商品，按 total_quantity 降序，rank 连续

#### Scenario: 区域销售分析验证
- **WHEN** DwsToAdsJob 执行完成
- **THEN** `SELECT * FROM ads.ads_regional_sales ORDER BY total_amount DESC` 返回省市区三级区域销售数据
