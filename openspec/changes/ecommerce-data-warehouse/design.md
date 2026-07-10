## Context

本项目构建电商实时数据仓库，数据源为Python脚本生成的模拟电商数据，通过Kafka推送至两个Topic（ods_products_data、ods_orders_data），需用Flink消费并完成ODS→DWD→DWS→ADS四层加工，最终通过Spring Boot可视化展示。

- **数据源**: Kafka (120.79.232.83:9092)，Topic: ods_products_data, ods_orders_data
- **目标存储**: PostgreSQL 16 (120.79.232.83:5432/DataWarehouse)，schema: dwd/dws/ads
- **开发环境**: JDK 17 (C:\Program Files\Java\jdk-17)，Maven 3.6.1 (D:\maven\apache-maven-3.6.1)，本地仓库 (E:\.maven\local_maven)
- **Flink版本**: 1.18.1 (与JDK 17兼容)
- **Spring Boot版本**: 3.2.x (JDK 17原生支持)
- **远程服务器**: `ssh root@120.79.232.83`，配置如下：
  - OS: Ubuntu, CPU: 2核, 内存: 1.6GB (空闲约 247Mi，资源紧张)
  - JDK: OpenJDK Temurin-17.0.11+9 (`/opt/jdk17`)
  - Kafka: 3.8.0 (`/opt/kafka_3.8.0`，KRaft模式)
  - PostgreSQL: 16.0 (`/opt/postgresql-16.0`，端口 5432)
  - Python数据生成: `/opt/DataGenerate/`，启动脚本 `/usr/local/bin/generate.sh`

### 数据生成脚本说明

数据生成脚本位于服务器 `/usr/local/bin/generate.sh`，用于向 Kafka 灌入模拟电商数据。

**脚本参数：**
| 参数 | 说明 | 示例 |
|------|------|------|
| `-p <N>` | 生成商品数量 | `-p 10` 生成 10 条商品 |
| `-o <N>` | 生成订单数量 | `-o 20` 生成 20 条订单 |

**使用示例：**
```bash
# 同时生成商品和订单
sh /usr/local/bin/generate.sh -p 10 -o 20

# 仅生成商品
sh /usr/local/bin/generate.sh -p 5

# 仅生成订单
sh /usr/local/bin/generate.sh -o 50

# 大规模数据测试
sh /usr/local/bin/generate.sh -p 500 -o 2000
```

**数据流路径：**
```
generate.sh → Python scripts (product_generator.py / order_generator.py)
    → Kafka localhost:9092
    → Topic: ods_products_data / ods_orders_data
    → Flink Job 消费处理
```

**脚本工作原理：**
1. 解析 `-p` / `-o` 参数
2. 切换到 `/opt/DataGenerate/` 项目目录
3. 激活 Python venv 虚拟环境
4. 调用 `product_generator.py --output kafka://localhost:9092/ods_products_data --count <N>`
5. 调用 `order_generator.py --output kafka://localhost:9092/ods_orders_data --count <N>`
6. 退出虚拟环境，恢复原始目录

**测试数据生成规范：**
- 功能验证：`-p 10 -o 20`（少量数据，验证全流程）
- 性能测试：`-p 200 -o 500`（中等数据量，测试吞吐）
- 压力测试：`-p 500 -o 2000`（大量数据，测试内存稳定性）

## Goals / Non-Goals

**Goals:**
- 3个可独立运行的Flink Jar包，覆盖全部4层数据加工
- DWD/DWS/ADS共13张表持久化至PostgreSQL对应schema
- Spring Boot Web应用，提供REST API和前端页面可视化ADS指标
- 代码可直接Maven编译打包，无需额外配置即可运行
- Flink作业支持Checkpoint容错和Exactly-Once语义

**Non-Goals:**
- 不涉及Flink集群部署运维（只提供Jar包）
- 不涉及用户权限认证系统
- 不涉及实时大屏推送（WebSocket）
- 不涉及CI/CD流水线配置

## Decisions

### 1. Flink版本选择：Flink 1.18.1
- **理由**: Flink 1.18.x 对JDK 17有完整支持，且与Kafka 3.x connector兼容稳定。Flink 2.0尚在预览阶段，不适合生产。
- **备选**: Flink 1.17.x — 同样稳定但部分JDK 17特性需额外配置。

### 2. 数据分层作业拆分：3个独立Job
- **理由**: 每层独立部署，可灵活调度（ODS→DWD可高频运行，DWS→ADS可按需运行）。单个Job失败不影响其他层。
- **备选**: 单一大Job流式串联 — 简化部署但丧失灵活性，故障影响面大。

### 3. Kafka消费模式：Flink Kafka Source (新API)
- **理由**: 使用FLIP-27新Source API，支持增量Checkpoint和更灵活的水位线控制。
- **消费策略**: earliest-offset（首次运行从最早消息消费，后续从Checkpoint恢复）

### 4. PostgreSQL写入方式：JDBC Sink with Upsert
- **理由**: 使用Flink JDBC Connector，支持INSERT...ON CONFLICT UPSERT语义，天然幂等。
- **批量写入**: 开启batch size=500，减少数据库连接开销。
- **Schema自动创建**: 首次运行时通过Flyway或SQL脚本自动建表。

### 5. DWD层设计：订单items数组展开
- **理由**: JSONB数组在数仓中难以直接JOIN和聚合，需将order.items展开为独立的dwd_order_item表，与dwd_order通过order_id关联。
- **字段清洗**: NULL值填充默认值，日期字符串标准化为TIMESTAMP，价格统一为DECIMAL(15,2)。

### 6. DWS层设计：轻度汇总，保留明细可追溯
- **dws_order_wide**: 以order_id+product_id为粒度的订单商品宽表，关联订单、商品、用户、物流信息
- **dws_*_1d**: 按天预聚合，每个汇总表按复合主键（日期+维度键）Upsert，支持Flink增量更新

### 7. ADS层设计：面向可视化的指标表
- 每天一次全量刷新或增量更新
- 包含Category Revenue（品类营收排行带同环比）、Daily Sales Trend（每日趋势）、User Value RFM（最近购买/频率/金额）、Product Ranking（TopN热销）、Regional Sales（省市区三级销售）

### 8. Spring Boot可视化技术选型
- **模板引擎**: Thymeleaf（服务端渲染，零前端构建工具链）
- **图表库**: ECharts（CDN引入，无需npm打包）
- **数据库访问**: MyBatis-Plus + HikariCP连接池
- **API风格**: RESTful，JSON响应

### 9. 项目构建方式：Maven多模块（父子POM）
- **理由**: 统一依赖版本管理，两个子模块共享公共配置。
- **父POM**: GoodsProjects/pom.xml 定义依赖版本
- **子模块**: flink-data-warehouse (Jar), dw-visualization (Spring Boot Jar)
- **Maven配置**: settings.xml中配置localRepository为E:\.maven\local_maven，profile中指定JDK 17

### 10. 数据模型主键策略
- DWD层: product_id或order_id作为主键
- DWS层: 复合主键（如 product_id + dt, category + dt）
- ADS层: 同DWS复合主键策略
- 所有表使用UPSERT语义（INSERT ON CONFLICT UPDATE），保证幂等重跑

### 11. 远程测试与部署策略
- **编译产物**: 本地 `mvn clean package` 生成 Fat Jar，然后 SCP 上传至服务器
- **Flink 运行模式**: 本地 Standalone 模式（Flink 自带的 MiniCluster），每个 Job 独立启动为 JVM 进程
- **Kafka Topic 自动创建**: Flink Kafka Source 启动时若 Topic 不存在会自动创建（`kafka-topics.sh --create` 作为备选）
- **测试流程**: 编译 Jar → SCP 上传 → 触发数据生成 → 启动 Flink Job → 查询 PostgreSQL 验证
- **内存约束**: 服务器仅 1.6GB 内存，Flink Job 单进程堆设置 `-Xmx256M`，避免 OOM

### 12. Git 版本控制策略
- **远程仓库**: `https://github.com/905558726/GoodsProjects.git`
- **提交规范**: Conventional Commits (`init:` / `feat:` / `test:` / `docs:` / `fix:` / `refactor:`)
- **提交粒度**: 每个功能模块完成后立即提交推送，每完成一个 Job 测试后提交测试结果
- **分支策略**: 单分支 `main`，每次提交 push 到远程
- **`.gitignore` 规则**:
  - **Markdown 文档**: 排除所有 `*.md`，仅保留 `README.md`
  - **编译产物**: `target/`、`*.class`、`*.jar`
  - **IDE 配置**: `.idea/`、`*.iml`、`.vscode/`
  - **配置文件**: `config*.properties`、`application*.yml`（含敏感信息，提供 `*-template.*` 模板替代）
  - **日志文件**: `logs/`、`*.log`
  - **运行时数据**: `checkpoint/`、`savepoint/`、`state/`、`tmp/`
  - **密钥凭证**: `*.pem`、`*.key`、`.env`
  - **OpenSpec 文档**: 纳入版本管理（`!openspec/**/*.md` 保留规则）

## Risks / Trade-offs

- **[风险] 服务器内存仅 1.6GB，Flink + Kafka + PostgreSQL 三者共存，资源极度紧张** → Flink Job 单进程堆限制 `-Xmx256M`，每次只运行一个 Job；批量写入 PostgreSQL 减轻连接开销；Kafka 已配置 `-Xms256M -Xmx256M` 低内存模式。单个 Job 完成后停止进程再启动下一个。
- **[风险] Kafka 与 PostgreSQL 在同一服务器，磁盘 I/O 可能成为瓶颈** → 使用 Flink 反压机制限流，JDBC Sink 批量写入 (batch size=500) 降低 DB 连接频率。
- **[风险] 数据生成脚本的 Kafka 地址为 localhost:9092** → 数据生成在服务器本地运行，localhost 即可访问；Flink 消费时需连接 `120.79.232.83:9092`（开发机无法直接访问 Kafka，Jar 只能部署到服务器运行）。
- **[风险] Flink 1.18 JDBC Connector 对 PostgreSQL UPSERT 支持需验证** → 使用自定义 JDBC Sink Function 手工生成 `INSERT ON CONFLICT ... DO UPDATE` 语句，避免依赖 Flink SQL 的隐式 Upsert。
- **[权衡] DataStream API vs Flink SQL** → DataStream API 更灵活，适合复杂 JSON 解析和嵌套结构展开。最终选择 DataStream API + ProcessFunction 为主，JDBC Sink 自定义实现。

## Test Strategy

### 每个 Flink Job 的测试流程（必须执行）

1. **编译打包**: 本地 `mvn clean package -pl flink-data-warehouse -DskipTests`
2. **上传至服务器**: `scp target/flink-data-warehouse-1.0.jar root@120.79.232.83:/opt/flink-jobs/`
3. **初始化数据库**: 执行 `docs/init_database.sql` 建表
4. **触发数据生成**: `ssh root@120.79.232.83 "sh /usr/local/bin/generate.sh -p <N> -o <N>"`
5. **确认 Kafka Topic 存在并有数据**: `ssh root@120.79.232.83 "/opt/kafka_3.8.0/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"`
6. **启动 Flink Job**: `ssh root@120.79.232.83 "java -Xmx256M -jar /opt/flink-jobs/flink-data-warehouse-1.0.jar <job-class>"`
7. **查询 PostgreSQL 验证数据**: `PGPASSWORD=<DB_PASSWORD> psql -h <DB_HOST> -U <DB_USER> -d DataWarehouse -c "SELECT count(*) FROM dwd.xxx"`
8. **确认无误后停止**: 记录测试结果，停止 Job 进程

### Spring Boot 可视化测试流程

1. 本地编译: `mvn clean package -pl dw-visualization -DskipTests`
2. 本地启动: `java -jar dw-visualization/target/dw-visualization-1.0.jar`
3. 浏览器访问 `http://localhost:8080`
4. 验证所有 6 个页面数据正确渲染

## Open Questions

- Flink Job 是否需要同时运行还是依次串行？（考虑内存限制，建议依次串行：先 OdsToDwd 消费完 Kafka 数据后停止，再依次跑 DwdToDws、DwsToAds）
- 是否需要为每层作业设置不同的并行度？（服务器 2 核，默认并行度=1）
- ADS 指标是否需要支持同比/环比？（初期只做绝对值计算，后续扩展）
