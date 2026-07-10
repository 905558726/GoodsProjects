## 1. 项目骨架搭建与Git仓库初始化

- [ ] 1.1 初始化Git仓库：`git init`，创建 `.gitignore` 文件（详细规则见下方规范）
- [ ] 1.2 关联远程仓库：`git remote add origin https://github.com/905558726/GoodsProjects.git`
- [ ] 1.3 创建父POM（GoodsProjects/pom.xml）：定义Maven坐标、JDK 17编译器、Flink 1.18.1、Spring Boot 3.2.x、MyBatis-Plus版本管理
- [ ] 1.4 创建flink-data-warehouse子模块：pom.xml依赖flink-streaming-java、flink-connector-kafka、flink-connector-jdbc、flink-json、jackson、postgresql驱动、lombok
- [ ] 1.5 创建dw-visualization子模块：pom.xml依赖spring-boot-starter-web、spring-boot-starter-thymeleaf、mybatis-plus-spring-boot3-starter、postgresql驱动、lombok
- [ ] 1.6 配置settings.xml：设置localRepository为E:\.maven\local_maven，配置JDK 17 profile
- [ ] 1.7 编写建表SQL脚本（docs/init_database.sql）：创建dwd/dws/ads三个schema及全部13张表
- [ ] 1.8 首次提交：`git add . && git commit -m "init: 电商数仓项目骨架搭建" && git push origin main`

### `.gitignore` 文件规范

以下内容必须创建为项目根目录的 `.gitignore` 文件，所有规则按类别分组：

```gitignore
# =============================================================================
# 1. Markdown 文档（排除所有 .md 文件，README.md 和 OpenSpec 设计文档除外）
# =============================================================================
# 忽略所有 markdown 文件
*.md
# 例外：保留 README.md 和 OpenSpec 设计文档
!README.md
!openspec/**/*.md

# =============================================================================
# 2. 编译产物
# =============================================================================
target/
*.class
*.jar
*.war
*.ear

# =============================================================================
# 3. IDE 与编辑器
# =============================================================================
.idea/
*.iml
*.iws
*.ipr
.vscode/
.settings/
.project
.classpath
.factorypath
*.swp
*.swo
*~
.DS_Store
Thumbs.db

# =============================================================================
# 4. 配置文件（包含敏感信息，禁止提交）
# =============================================================================
# Flink 作业配置（含数据库密码、Kafka地址）
**/config.properties
**/config-*.properties
**/app-config.properties

# Spring Boot 配置（含数据源密码）
**/application.yml
**/application-*.yml
**/application.yaml
**/application-*.yaml
**/application.properties
**/application-*.properties

# 例外：保留模板文件（不含敏感信息）
!**/application-template.yml
!**/application-template.properties
!**/config-template.properties

# MyBatis 配置
**/mybatis-config.xml

# =============================================================================
# 5. 日志文件
# =============================================================================
logs/
*.log
*.log.*

# =============================================================================
# 6. 运行时数据
# =============================================================================
# Flink Checkpoint 与 Savepoint
checkpoint/
checkpoints/
savepoint/
savepoints/
# Flink 状态后端
rocksdb/
state/

# 临时文件
tmp/
temp/
*.tmp
*.bak

# =============================================================================
# 7. 依赖与包管理
# =============================================================================
# Maven
dependency-reduced-pom.xml

# =============================================================================
# 8. 操作系统生成文件
# =============================================================================
ehthumbs.db
Desktop.ini
$RECYCLE.BIN/

# =============================================================================
# 9. 敏感信息
# =============================================================================
# 密钥与凭证
*.pem
*.key
*.p12
*.pfx
*.jks
*.keystore
*.truststore
credentials.*
secrets.*
.env
.env.*
!**/.env.example

# =============================================================================
# 10. OpenSpec 设计文档（纳入版本管理，不排除）
# =============================================================================
# openspec/ 目录内的 .md 文件已在第1节通过 !openspec/**/*.md 保留

# =============================================================================
# 11. CLAUDE.md（纳入版本管理）
# =============================================================================
# CLAUDE.md 已在第1节通过 !CLAUDE.md 保留
```
**注意**: 实际 `.gitignore` 文件已按此规范创建并纳入版本管理（参见 `70c1bd2` 提交）。

**`.gitignore` 规则说明：**

| 规则类别 | 排除内容 | 保留例外 |
|---------|----------|----------|
| Markdown 文档 | 所有 `*.md` | `README.md`、`openspec/**/*.md` |
| 编译产物 | `target/`、`*.class`、`*.jar` | 无 |
| IDE 配置 | `.idea/`、`*.iml`、`.vscode/` | 无 |
| 配置文件 | `config*.properties`、`application*.yml` | `*-template.*` |
| 日志文件 | `logs/`、`*.log` | 无 |
| 运行时数据 | `checkpoint/`、`savepoint/`、`state/` | 无 |
| 密钥凭证 | `*.pem`、`*.key`、`.env` | `.env.example` |
| OpenSpec | 不排除（纳入版本管理） | `!openspec/**/*.md` |

**配置文件处理原则：**
- 含密码/地址/密钥的配置文件一律不入库
- 提供 `*-template.*` 模板文件供开发者复制后填入真实值
- 示例模板（如 `application-template.yml`）中的密码字段标注 `<YOUR_PASSWORD>` 占位符

## 2. 公共数据模型定义 (flink-data-warehouse model)

- [ ] 2.1 创建ProductEvent模型类：映射Kafka商品JSON的所有字段
- [ ] 2.2 创建OrderEvent及嵌套Buyer/ShippingAddress/Payment/Logistics/OrderItem模型类：映射Kafka订单JSON的完整嵌套结构
- [ ] 2.3 创建DwdProduct/DwdOrder/DwdOrderItem实体类：对应DWD层表的POJO，使用@JsonProperty映射
- [ ] 2.4 创建DwsOrderWide/DwsProductSales1d/DwsCategorySales1d/DwsUserOrder1d实体类：对应DWS层表
- [ ] 2.5 创建AdsCategoryRevenue/AdsDailySalesTrend/AdsUserValue/AdsProductRanking/AdsRegionalSales实体类：对应ADS层表
- [ ] 2.6 Git提交数据模型层：`git add . && git commit -m "feat: 添加数仓全链路数据模型定义 (Kafka Event + DWD/DWS/ADS 实体)" && git push origin main`

## 3. DWD层Flink作业 (OdsToDwdJob)

- [ ] 3.1 实现KafkaProductSource：从ods_products_data消费，JSON反序列化为ProductEvent
- [ ] 3.2 实现KafkaOrderSource：从ods_orders_data消费，JSON反序列化为OrderEvent
- [ ] 3.3 实现ProductCleanFunction：ProductEvent→DwdProduct清洗（price精度、时间标准化、NULL填充、异常数据过滤）
- [ ] 3.4 实现OrderSplitFunction：OrderEvent→Tuple2<DwdOrder, List<DwdOrderItem>>拆分（items展开、状态映射、金额校验）
- [ ] 3.5 实现PostgresProductSink：DwdProduct UPSERT写入dwd.dwd_product（JDBC Sink）
- [ ] 3.6 实现PostgresOrderSink和OrderItemSink：分别UPSERT写入dwd.dwd_order和dwd.dwd_order_item
- [ ] 3.7 组装OdsToDwdJob主类：构建StreamExecutionEnvironment，配置Checkpoint(60s)，连接Source→Process→Sink
- [ ] 3.8 实现数据处理死信队列：JSON解析失败的消息记录异常日志并跳过
- [ ] 3.9 Git提交DWD层作业：`git add . && git commit -m "feat: 实现 OdsToDwdJob (Kafka消费→清洗→PostgreSQL DWD层写入)" && git push origin main`

## 4. DWS层Flink作业 (DwdToDwsJob)

- [ ] 4.1 实现PostgresSource：从dwd.dwd_order/dwd.dwd_order_item/dwd.dwd_product JDBC读取数据
- [ ] 4.2 实现OrderWideFlatFunction：三表关联构建dws_order_wide宽表记录
- [ ] 4.3 实现ProductSalesAggFunction：按(product_id, dt)窗口聚合，计算quantity/amount/avg_price
- [ ] 4.4 实现CategorySalesAggFunction：按(category, dt)聚合，计算quantity/amount/order_count/sku_count
- [ ] 4.5 实现UserOrderAggFunction：按(buyer_phone, dt)聚合，计算order_count/total_amount/total_quantity/状态分布
- [ ] 4.6 实现DwsPostgresSink：四张DWS表UPSERT写入对应的dws schema
- [ ] 4.7 组装DwdToDwsJob主类：构建执行环境，配置窗口策略（TumblingEventTimeWindows 1天），连接Source→Aggregate→Sink
- [ ] 4.8 Git提交DWS层作业：`git add . && git commit -m "feat: 实现 DwdToDwsJob (DWD读取→宽表关联→日汇总聚合→DWS层写入)" && git push origin main`

## 5. ADS层Flink作业 (DwsToAdsJob)

- [ ] 5.1 实现DwsSource：从DWS四张表JDBC读取汇总数据
- [ ] 5.2 实现CategoryRevenueFunction：累加品类销售，计算rank、percentage，写入ads_category_revenue
- [ ] 5.3 实现DailyTrendFunction：全品类按天汇总，计算客单价，写入ads_daily_sales_trend
- [ ] 5.4 实现UserValueFunction：计算RFM指标（recency/frequency/monetary）和value_tier分层，写入ads_user_value
- [ ] 5.5 实现ProductRankingFunction：按累计销量TopN排序，写入ads_product_ranking
- [ ] 5.6 实现RegionalSalesFunction：按province/city/district汇总，写入ads_regional_sales
- [ ] 5.7 实现AdsPostgresSink：五张ADS表UPSERT写入对应的ads schema
- [ ] 5.8 组装DwsToAdsJob主类：构建批处理执行环境，全量刷新ADS指标表
- [ ] 5.9 Git提交ADS层作业：`git add . && git commit -m "feat: 实现 DwsToAdsJob (DWS读取→RFM/排行/趋势/区域指标计算→ADS层写入)" && git push origin main`

## 6. Flink配置与工具类

- [ ] 6.1 创建AppConfig配置类：Kafka地址、PostgreSQL连接信息、Checkpoint路径等可配置参数
- [ ] 6.2 创建config.properties配置文件：120.79.232.83相关连接参数
- [ ] 6.3 创建JsonUtils工具类：Jackson ObjectMapper统一配置（忽略未知字段、日期格式、时区）
- [ ] 6.4 创建JdbcUtils工具类：PostgreSQL JDBC连接工厂、UPSERT SQL生成
- [ ] 6.5 Git提交配置与工具类：`git add . && git commit -m "feat: 添加 Flink 配置管理、JSON/JDBC 工具类" && git push origin main`

## 7. 可视化项目后端 (Spring Boot)

- [ ] 7.1 创建Spring Boot启动类和application.yml：端口8080，Thymeleaf模板，MyBatis-Plus，PostgreSQL数据源
- [ ] 7.2 创建CategoryRevenueController/Service/Mapper：GET /api/ads/category-revenue
- [ ] 7.3 创建DailySalesTrendController/Service/Mapper：GET /api/ads/daily-sales-trend?startDate=&endDate=
- [ ] 7.4 创建UserValueController/Service/Mapper：GET /api/ads/user-value?valueTier=
- [ ] 7.5 创建ProductRankingController/Service/Mapper：GET /api/ads/product-ranking?topN=&category=
- [ ] 7.6 创建RegionalSalesController/Service/Mapper：GET /api/ads/regional-sales?level=province
- [ ] 7.7 创建DashboardController/Service：聚合首页KPI数据（总销售额/订单数/用户数/SKU数）
- [ ] 7.8 Git提交后端代码：`git add . && git commit -m "feat: 实现 Spring Boot 可视化后端 (Controller/Service/Mapper + REST API)" && git push origin main`

## 8. 可视化前端页面 (Thymeleaf + ECharts)

- [ ] 8.1 创建公共布局模板templates/layout.html：左侧导航栏（仪表盘、品类营收、销售趋势、用户价值、商品排行、区域销售），CDN引入ECharts和Bootstrap CSS
- [ ] 8.2 创建首页仪表盘页面templates/index.html：4个KPI卡片、品类营收Top5饼图、近7天趋势迷你折线图
- [ ] 8.3 创建品类营收页面templates/category-revenue.html：横向柱状图
- [ ] 8.4 创建销售趋势页面templates/daily-sales-trend.html：折线+柱状双轴组合图，日期范围筛选
- [ ] 8.5 创建用户价值页面templates/user-value.html：价值层级饼图+RFM散点气泡图
- [ ] 8.6 创建商品排行页面templates/product-ranking.html：品类筛选下拉框+TopN选择+排名表格+柱状图
- [ ] 8.7 创建区域销售页面templates/regional-sales.html：中国地图热力图+省份排行榜
- [ ] 8.8 Git提交前端页面：`git add . && git commit -m "feat: 实现 Thymeleaf+ECharts 可视化前端 (6个页面+公共布局)" && git push origin main`

## 9. OdsToDwdJob 远程部署测试

**测试前置步骤（所有 Job 测试共用）：**
- 通过 SSH 连接服务器：`ssh root@120.79.232.83`
- 数据生成脚本路径：`/usr/local/bin/generate.sh`
- 语法：`sh /usr/local/bin/generate.sh -p <商品数> -o <订单数>`
- 每次测试前根据测试目的选择合适的数据量（功能验证 `-p 10 -o 20`，性能验证 `-p 200 -o 500`）

- [ ] 9.1 编译 OdsToDwdJob Jar：`mvn clean package -pl flink-data-warehouse -DskipTests`
- [ ] 9.2 上传至服务器：`scp flink-data-warehouse/target/flink-data-warehouse-1.0.jar root@120.79.232.83:/opt/flink-jobs/`
- [ ] 9.3 初始化数据库：执行 `docs/init_database.sql` 创建 dwd/dws/ads schema 和表
- [ ] 9.4 在服务器上创建 Kafka Topic（若不存在）：`ssh root@120.79.232.83 "/opt/kafka_3.8.0/bin/kafka-topics.sh --create --topic ods_products_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"` 和 `ods_orders_data`
- [ ] 9.5 触发数据生成（功能验证级别）：`ssh root@120.79.232.83 "sh /usr/local/bin/generate.sh -p 10 -o 20"` 生成 10 个商品 + 20 个订单灌入 Kafka
- [ ] 9.6 确认 Kafka 有数据：`ssh root@120.79.232.83 "/opt/kafka_3.8.0/bin/kafka-console-consumer.sh --topic ods_products_data --bootstrap-server localhost:9092 --from-beginning --max-messages 3"` 抽检拓扑消息
- [ ] 9.7 启动 OdsToDwdJob：`ssh root@120.79.232.83 "java -Xmx256M -cp /opt/flink-jobs/flink-data-warehouse-1.0.jar com.dw.job.OdsToDwdJob"`，观察日志确认消费并写入成功
- [ ] 9.8 验证 dwd_product：`PGPASSWORD=<DB_PASSWORD> psql -h <DB_HOST> -U <DB_USER> -d DataWarehouse -c "SELECT count(*) FROM dwd.dwd_product"` → 预期 10 条
- [ ] 9.9 验证 dwd_order：`SELECT count(*) FROM dwd.dwd_order` → 预期 20 条
- [ ] 9.10 验证 dwd_order_item：`SELECT count(*) FROM dwd.dwd_order_item` → items 展开后的明细数
- [ ] 9.11 抽检数据质量：检查 price 精度（DECIMAL(15,2)）、日期格式（TIMESTAMP）、NULL 填充、order_status 映射
- [ ] 9.12 停止 Job 进程，确认资源释放（内存恢复至 150Mi+）
- [ ] 9.13 Git提交测试结果：`git add . && git commit -m "test: OdsToDwdJob 远程部署测试通过 (generate.sh -p 10 -o 20)" && git push origin main`

## 10. DwdToDwsJob 远程部署测试

- [ ] 10.1 确认 DWD 层三张表有数据（完成 OdsToDwd 测试后）
- [ ] 10.2 编译并上传 Jar（若代码有变更）
- [ ] 10.3 启动 DwdToDwsJob：`ssh root@120.79.232.83 "java -Xmx256M -cp /opt/flink-jobs/flink-data-warehouse-1.0.jar com.dw.job.DwdToDwsJob"`
- [ ] 10.4 验证 dws_order_wide：`SELECT count(*) FROM dws.dws_order_wide` → 宽表记录数应与 dwd_order_item 一致
- [ ] 10.5 验证 dws_product_sales_1d：`SELECT count(*) FROM dws.dws_product_sales_1d` → 有销售的商品数
- [ ] 10.6 验证 dws_category_sales_1d：`SELECT * FROM dws.dws_category_sales_1d` → 各品类汇总数据
- [ ] 10.7 验证 dws_user_order_1d：`SELECT * FROM dws.dws_user_order_1d` → 用户购买行为汇总
- [ ] 10.8 抽检宽表字段完整性：随机查一条 dws_order_wide 确认 buyer/province/city/logistics 字段完整
- [ ] 10.9 停止 Job 进程
- [ ] 10.10 Git提交测试结果：`git add . && git commit -m "test: DwdToDwsJob 远程部署测试通过 (generate.sh -p 10 -o 20)" && git push origin main`

## 11. DwsToAdsJob 远程部署测试

- [ ] 11.1 确认 DWS 层四张表有数据（完成 DwdToDws 测试后）
- [ ] 11.2 编译并上传 Jar（若代码有变更）
- [ ] 11.3 启动 DwsToAdsJob：`ssh root@120.79.232.83 "java -Xmx256M -cp /opt/flink-jobs/flink-data-warehouse-1.0.jar com.dw.job.DwsToAdsJob"`
- [ ] 11.4 验证 ads_category_revenue：`SELECT * FROM ads.ads_category_revenue ORDER BY rank` → 品类营收排行
- [ ] 11.5 验证 ads_daily_sales_trend：`SELECT * FROM ads.ads_daily_sales_trend ORDER BY dt` → 每日销售趋势
- [ ] 11.6 验证 ads_user_value：`SELECT * FROM ads.ads_user_value` → RFM 指标和 value_tier 分层
- [ ] 11.7 验证 ads_product_ranking：`SELECT * FROM ads.ads_product_ranking ORDER BY rank LIMIT 10` → 热销 Top10
- [ ] 11.8 验证 ads_regional_sales：`SELECT * FROM ads.ads_regional_sales ORDER BY total_amount DESC` → 区域销售排行
- [ ] 11.9 停止 Job 进程
- [ ] 11.10 Git提交测试结果：`git add . && git commit -m "test: DwsToAdsJob 远程部署测试通过 (generate.sh -p 10 -o 20)" && git push origin main`

## 12. Spring Boot 可视化本地测试

- [ ] 12.1 编译 dw-visualization：`mvn clean package -pl dw-visualization -DskipTests`
- [ ] 12.2 本地启动：`java -jar dw-visualization/target/dw-visualization-1.0.jar`
- [ ] 12.3 浏览器访问 `http://localhost:8080` 验证仪表盘首页：KPI 卡片数据正确
- [ ] 12.4 验证 /category-revenue：图表渲染正常，数据与 ads_category_revenue 一致
- [ ] 12.5 验证 /daily-sales-trend：日期筛选功能正常，图表数据正确
- [ ] 12.6 验证 /user-value：RFM 散点图和价值分层饼图渲染正常
- [ ] 12.7 验证 /product-ranking：TopN 切换和品类筛选功能正常
- [ ] 12.8 验证 /regional-sales：中国地图热力图正常渲染
- [ ] 12.9 验证所有 API 返回 JSON 格式正确（用浏览器 F12 或 curl 测试）
- [ ] 12.10 Git提交测试结果：`git add . && git commit -m "test: Spring Boot 可视化本地测试通过" && git push origin main`

## 13. 编写项目文档与最终提交

- [ ] 13.1 编写项目 README.md：项目结构说明、环境要求、编译运行步骤、Flink Job 提交命令
- [ ] 13.2 编写测试报告模板：包含测试时间、数据量、每个验证步骤的通过/失败状态、异常截图
- [ ] 13.3 最终提交：`git add . && git commit -m "docs: 完成项目文档与测试报告" && git push origin main`

## Git 提交规范

所有提交遵循 Conventional Commits 规范，分支策略如下：

| 提交类型 | 格式 | 说明 |
|---------|------|------|
| 初始化 | `init: <描述>` | 项目骨架、配置初始化 |
| 新功能 | `feat: <描述>` | 每个功能模块完成后提交 |
| 测试 | `test: <描述>` | 每个 Job 远程部署测试通过后提交 |
| 文档 | `docs: <描述>` | README、设计文档、SQL 脚本 |
| 修复 | `fix: <描述>` | Bug 修复 |
| 重构 | `refactor: <描述>` | 代码重构不改变功能 |

**提交时机规则：**
- 每完成一个功能模块（数据模型 / Flink Job / 后端 / 前端）必须提交并推送
- 每完成一个 Job 的远程部署测试并验证通过后必须提交测试结果
- 禁止将多个不相关的改动合并为一个 commit
- 提交前必须执行 `git status` 确认改动范围，避免误提交敏感信息（数据库密码等）
