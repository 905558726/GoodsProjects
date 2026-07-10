## ADDED Requirements

### Requirement: 订单商品宽表构建
系统 SHALL 从dwd.dwd_order、dwd.dwd_order_item、dwd.dwd_product三表关联，构建dws.dws_order_wide宽表，每条记录包含完整的订单信息、商品信息、买家信息、物流信息。

#### Scenario: 宽表关联成功
- **WHEN** DWD层有新订单数据和对应商品数据
- **THEN** dws.dws_order_wide表写入一条记录，包含order_id、product_id、buyer_name、province、city、category、brand、price、quantity、subtotal、order_status、payment_method、logistics_company等全部字段

#### Scenario: 商品数据尚未到达
- **WHEN** 订单引用的product_id在dwd_product中不存在
- **THEN** 宽表中商品相关字段（brand、category等）使用订单items中的快照数据填充，不依赖dwd_product

### Requirement: 商品日销售汇总
系统 SHALL 按天汇总每个商品的总销量、总销售额、订单数、平均售价，写入dws.dws_product_sales_1d表。

#### Scenario: 当天多笔订单同一商品
- **WHEN** 同一天内有多笔订单包含同一个product_id
- **THEN** dws.dws_product_sales_1d表按(product_id, dt) UPSERT，quantity和amount累加，avg_price=amount/quantity重新计算

#### Scenario: 当天无销售的商品
- **WHEN** 某个商品当天没有任何订单
- **THEN** 该商品不出现在dws_product_sales_1d当天的记录中

### Requirement: 品类日销售汇总
系统 SHALL 按天汇总每个品类的总销量、总销售额、订单数、SKU数，写入dws.dws_category_sales_1d表。

#### Scenario: 多子类品类汇总
- **WHEN** "电子产品"品类下有"手机"和"笔记本电脑"两个子类的订单
- **THEN** dws_category_sales_1d按category汇总所有子类数据，统计该品类下的去重product_id数量作为sku_count

### Requirement: 用户日行为汇总
系统 SHALL 按天汇总每个买家（user）的下单次数、购买金额、购买商品件数、订单状态分布，写入dws.dws_user_order_1d表。

#### Scenario: 用户同一天多笔订单
- **WHEN** 同一buyer_phone在当天有3笔订单
- **THEN** dws_user_order_1d按(buyer_phone, dt)汇总，order_count=3，total_amount为3笔订单actual_amount之和，total_quantity为所有商品件数之和

#### Scenario: 用户订单状态分布统计
- **WHEN** 用户当天有已完成和已取消两种状态的订单
- **THEN** dws_user_order_1d中completed_count和cancelled_count分别统计对应状态的订单数

### Requirement: DwdToDwsJob远程部署与验证
系统 SHALL 编译打包后部署至服务器 `120.79.232.83`，从 DWD 层读取数据构建 DWS 宽表和汇总表，通过 SQL 查询验证数据正确性。

#### Scenario: Jar包编译与上传
- **WHEN** DWD 层数据就绪后编译并上传 Jar
- **THEN** Jar 成功部署至 `/opt/flink-jobs/`，Job 以 `java -Xmx256M` 启动

#### Scenario: 宽表构建验证
- **WHEN** DwdToDwsJob 执行完成
- **THEN** `SELECT count(*) FROM dws.dws_order_wide` 返回记录数与 dwd_order_item 一致，抽检 buyer_name、province、city、category、logistics_company 字段非空

#### Scenario: 商品日汇总验证
- **WHEN** DwdToDwsJob 执行完成
- **THEN** `SELECT * FROM dws.dws_product_sales_1d` 返回有销售的商品汇总，quantity 和 amount 累加正确，avg_price = amount / quantity

#### Scenario: 品类日汇总验证
- **WHEN** DwdToDwsJob 执行完成
- **THEN** `SELECT * FROM dws.dws_category_sales_1d` 各品类 sku_count 为去重 product_id 数量

#### Scenario: 用户日行为验证
- **WHEN** DwdToDwsJob 执行完成
- **THEN** `SELECT * FROM dws.dws_user_order_1d` 用户汇总数据与 DWD 订单原始数据可追溯对照
