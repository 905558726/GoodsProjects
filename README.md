# GoodsProjects - 电商实时数据仓库

基于 Apache Flink + Kafka + PostgreSQL + Spring Boot 的电商实时数据仓库项目，完成 ODS→DWD→DWS→ADS 四层数据加工，并通过 Web 页面进行可视化分析。

## 项目架构

```
Kafka (ods_products_data / ods_orders_data)
         │
         ▼
┌─────────────────────────────────────────┐
│         Flink 数据仓库 (3个Job)          │
│                                         │
│  OdsToDwdJob: ODS层 → DWD层 (数据清洗)    │
│  DwdToDwsJob: DWD层 → DWS层 (汇总宽表)    │
│  DwsToAdsJob: DWS层 → ADS层 (业务指标)    │
└─────────────────────────────────────────┘
         │
         ▼
PostgreSQL (DataWarehouse)
├── dwd schema: 明细数据层
├── dws schema: 汇总数据层
├── ads schema: 指标应用层
└── dim schema: 维度数据层
         │
         ▼
┌─────────────────────────────────────────┐
│     Spring Boot 可视化平台 (8080)         │
│                                         │
│  仪表盘 / 品类营收 / 销售趋势             │
│  用户价值 / 商品排行 / 区域销售 / 商品库   │
└─────────────────────────────────────────┘
```

## 数据仓库分层

### DWD 层（Data Warehouse Detail — 明细数据）

| Schema | 表名 | 说明 |
|--------|------|------|
| dwd | dwd_product | 清洗后的商品数据 |
| dwd | dwd_order | 清洗后的订单主表 |
| dwd | dwd_order_item | 订单商品明细（items 展开） |

### DWS 层（Data Warehouse Summary — 汇总数据）

| Schema | 表名 | 说明 |
|--------|------|------|
| dws | dws_order_wide | 订单商品宽表 |
| dws | dws_product_sales_1d | 商品日销售汇总 |
| dws | dws_category_sales_1d | 品类日销售汇总 |
| dws | dws_user_order_1d | 用户日行为汇总 |

### ADS 层（Application Data Service — 应用指标）

| Schema | 表名 | 说明 |
|--------|------|------|
| ads | ads_category_revenue | 品类营收排行 |
| ads | ads_daily_sales_trend | 每日销售趋势 |
| ads | ads_user_value | 用户价值分析 (RFM) |
| ads | ads_product_ranking | 商品热销排行 |
| ads | ads_regional_sales | 区域销售分析 |

### 维度层

| Schema | 表名 | 说明 |
|--------|------|------|
| dim | dim_goods_info | 商品基础信息库 |

## 项目结构

```
GoodsProjects/
├── pom.xml                              # 父 POM（版本管理）
├── README.md                            # 项目说明
├── CLAUDE.md                            # Claude Code 配置
├── docs/
│   ├── init_database.sql                # 建表 SQL 脚本
│   ├── run_data_pipeline.sh             # 数据生成 + 灌入 Kafka 脚本
│   └── run_loop.sh                      # 持续随机生成循环脚本
├── openspec/                            # OpenSpec 设计文档
│   └── changes/ecommerce-data-warehouse/
│       ├── proposal.md                  # 需求提案
│       ├── design.md                    # 技术设计
│       ├── tasks.md                     # 任务清单
│       └── specs/                       # 规格说明
├── flink-data-warehouse/                # Flink 数据处理模块
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/dw/
│       │   ├── model/                   # 数据模型 (POJO)
│       │   │   ├── ProductEvent.java    #   Kafka 商品消息
│       │   │   ├── OrderEvent.java      #   Kafka 订单消息（含嵌套类）
│       │   │   ├── DwdProduct.java      #   DWD 商品实体
│       │   │   ├── DwdOrder.java        #   DWD 订单实体
│       │   │   ├── DwdOrderItem.java    #   DWD 订单明细实体
│       │   │   └── DwsOrderWide.java    #   DWS 订单宽表
│       │   ├── job/                     # Flink 作业
│       │   │   ├── OdsToDwdJob.java     #   ODS→DWD 清洗作业（Kafka→JDBC Sink）
│       │   │   ├── DwdToDwsJob.java     #   DWD→DWS 汇总作业（定时 SQL 刷新）
│       │   │   └── DwsToAdsJob.java     #   DWS→ADS 指标作业（定时 SQL 刷新）
│       │   ├── function/                # 处理函数
│       │   │   ├── ProductCleanFunction.java
│       │   │   └── OrderSplitFunction.java
│       │   ├── sink/                    # PostgreSQL Sink
│       │   │   └── PostgresJdbcSink.java
│       │   ├── source/                  # Kafka 反序列化
│       │   │   ├── ProductDeserializer.java
│       │   │   └── OrderDeserializer.java
│       │   ├── config/
│       │   │   └── AppConfig.java       # 应用配置常量
│       │   └── util/
│       │       ├── JsonUtils.java       # JSON 工具
│       │       └── JdbcUtils.java       # JDBC 工具
│       └── resources/
│           ├── config.properties        # 实际配置（.gitignore 排除）
│           └── config-template.properties  # 配置模板
└── dw-visualization/                    # Spring Boot 可视化模块
    ├── pom.xml
    └── src/
        ├── main/
        │   ├── java/com/dw/vis/
        │   │   ├── DwVisualizationApplication.java  # 启动类
        │   │   ├── controller/           # 控制器
        │   │   │   ├── DashboardController.java  # REST API（/api/ads/*）
        │   │   │   └── PageController.java       # 页面路由
        │   │   ├── service/              # 业务服务层
        │   │   │   └── DashboardService.java
        │   │   ├── mapper/               # MyBatis Mapper
        │   │   │   ├── CategoryRevenueMapper.java
        │   │   │   ├── DailySalesTrendMapper.java
        │   │   │   ├── UserValueMapper.java
        │   │   │   ├── ProductRankingMapper.java
        │   │   │   ├── RegionalSalesMapper.java
        │   │   │   └── GoodsInfoMapper.java
        │   │   └── entity/               # 实体类
        │   │       ├── DashboardKpi.java
        │   │       ├── CategoryRevenue.java
        │   │       ├── DailySalesTrend.java
        │   │       ├── UserValue.java
        │   │       ├── ProductRanking.java
        │   │       ├── RegionalSales.java
        │   │       └── GoodsInfo.java
        │   └── resources/
        │       ├── application.yml       # 应用配置（.gitignore 排除）
        │       ├── application-template.yml  # 配置模板
        │       └── templates/            # Thymeleaf 页面模板
        │           ├── index.html        #   仪表盘首页
        │           ├── category-revenue.html
        │           ├── daily-sales-trend.html
        │           ├── user-value.html
        │           ├── product-ranking.html
        │           ├── regional-sales.html
        │           └── goods-info.html   #   商品库
        └── test/                         # 测试目录
```

## 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| JDK | 17 | 编译 & 运行环境 |
| Maven | 3.6+ | 项目构建管理 |
| Flink | 1.18.1 | 流批一体计算引擎 |
| Spring Boot | 3.2.7 | Web 可视化框架 |
| Kafka | 3.x | 消息队列 |
| PostgreSQL | 16 | 数据仓库存储 |

## 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/905558726/GoodsProjects.git
cd GoodsProjects
```

### 2. 初始化数据库

连接 PostgreSQL 执行建表脚本：

```bash
psql -h <host> -p 5432 -U <user> -d DataWarehouse -f docs/init_database.sql
```

### 3. 编译项目

```bash
# 编译全部模块
mvn clean package -DskipTests

# 仅编译 Flink 模块
cd flink-data-warehouse
mvn clean package -DskipTests

# 仅编译可视化模块
cd dw-visualization
mvn clean package -DskipTests
```

### 4. 启动 Flink 作业

需要先确保 Kafka Topic 有数据流入（运行数据生成脚本）：

```bash
# 作业1: ODS → DWD 数据清洗（需先启动，流式持续运行）
flink run -c com.dw.job.OdsToDwdJob flink-data-warehouse/target/flink-data-warehouse-1.0.jar

# 作业2: DWD → DWS 宽表汇总
flink run -c com.dw.job.DwdToDwsJob flink-data-warehouse/target/flink-data-warehouse-1.0.jar

# 作业3: DWS → ADS 指标计算
flink run -c com.dw.job.DwsToAdsJob flink-data-warehouse/target/flink-data-warehouse-1.0.jar
```

### 5. 启动可视化平台

```bash
cd dw-visualization
mvn spring-boot:run

# 或直接运行 Jar
java -jar target/dw-visualization-1.0.jar
```

浏览器访问：`http://localhost:8080`

## 数据生成

上游使用 Python 脚本模拟生成电商数据并灌入 Kafka，详见数据生成项目。

在数据生成服务器上运行：

```bash
# 单次随机生成
sh run_data_pipeline.sh

# 持续循环生成（随机间隔 5-15 秒）
sh run_loop.sh
```

## 可视化页面

| 页面 | 路径 | 说明 |
|------|------|------|
| 仪表盘 | `/` | 核心 KPI 卡片 + 品类饼图 + 近7天趋势 |
| 品类营收 | `/category-revenue` | 品类营收横向柱状图排行 |
| 销售趋势 | `/daily-sales-trend` | 折线图+柱状图双轴组合，支持日期筛选 |
| 用户价值 | `/user-value` | RFM 散点气泡图 + 价值分层饼图 |
| 商品排行 | `/product-ranking` | TopN 排行榜 + 品类筛选 |
| 区域销售 | `/regional-sales` | 省/市级横向柱状图 + Top10 表格 |
| 商品库 | `/goods-info` | 分页表格，品类/品牌/关键词筛选 |

## 技术选型

| 技术 | 版本 | 说明 |
|------|------|------|
| Apache Flink | 1.18.1 | 流批一体计算引擎，DataStream API |
| Flink Kafka Connector | 3.1.0-1.18 | FLIP-27 新 Source API |
| Flink JDBC Connector | 3.1.2-1.18 | PostgreSQL Sink，UPSERT 语义 |
| Spring Boot | 3.2.7 | Web 应用框架 |
| Thymeleaf | 3.1.2 | 服务端模板引擎 |
| ECharts | 5.5.0 (CDN) | 前端图表库 |
| MyBatis-Plus | 3.5.7 | 数据库 ORM |
| HikariCP | (内置于 Spring Boot) | 数据库连接池 |
| PostgreSQL JDBC | 42.7.3 | 数据库驱动 |
| Jackson | 2.17.2 | JSON 序列化 |
| Lombok | 1.18.34 | 代码简化 |
| Maven | 3.6+ | 项目构建管理 |
| JDK | 17 | Java 开发环境 |

## 相关文档

- [需求提案](openspec/changes/ecommerce-data-warehouse/proposal.md)
- [技术设计](openspec/changes/ecommerce-data-warehouse/design.md)
- [规格说明](openspec/changes/ecommerce-data-warehouse/specs/)
- [任务清单](openspec/changes/ecommerce-data-warehouse/tasks.md)

## License

MIT
