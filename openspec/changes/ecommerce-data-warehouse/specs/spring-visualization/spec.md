## ADDED Requirements

### Requirement: 品类营收排行API与页面
系统 SHALL 提供REST API `GET /api/ads/category-revenue` 查询品类营收排行数据，并渲染为ECharts柱状图页面。

#### Scenario: API返回品类营收数据
- **WHEN** 前端请求GET /api/ads/category-revenue
- **THEN** 返回JSON数组，包含category、sales_amount、order_count、percentage、rank字段，按rank升序

#### Scenario: 品类营收排行榜页面渲染
- **WHEN** 用户访问 /category-revenue 页面
- **THEN** 页面显示ECharts横向柱状图，X轴为销售额，Y轴为品类名称，柱体颜色渐变，显示占比百分比标签

### Requirement: 每日销售趋势API与页面
系统 SHALL 提供REST API `GET /api/ads/daily-sales-trend` 查询每日销售趋势，支持日期范围筛选，渲染为ECharts折线图+柱状图组合页面。

#### Scenario: API支持日期范围筛选
- **WHEN** 前端请求GET /api/ads/daily-sales-trend?startDate=2026-07-01&endDate=2026-07-10
- **THEN** 返回指定日期范围内的每日销售额、订单数、客单价数据

#### Scenario: 销售趋势折线图与柱状图组合
- **WHEN** 用户访问 /daily-sales-trend 页面
- **THEN** 页面显示销售额折线图（左Y轴）、订单数柱状图（右Y轴）双轴组合图，底部显示日期时间轴

### Requirement: 用户价值分析API与页面
系统 SHALL 提供REST API `GET /api/ads/user-value` 查询用户价值分析RFM数据，渲染为ECharts散点图和分布饼图页面。

#### Scenario: API返回用户价值数据
- **WHEN** 前端请求GET /api/ads/user-value?valueTier=高价值
- **THEN** 返回指定价值层级用户的RFM数据，按total_monetary降序

#### Scenario: 用户价值分析可视化
- **WHEN** 用户访问 /user-value 页面
- **THEN** 页面显示：（1）价值层级饼图（高/中/低价值用户占比）；（2）用户散点图（X轴=购买频率，Y轴=累计消费，气泡大小=最近购买天数）

### Requirement: 商品热销排行API与页面
系统 SHALL 提供REST API `GET /api/ads/product-ranking` 查询商品热销排行，支持TopN参数和品类筛选，渲染为ECharts排行榜表格页面。

#### Scenario: API支持TopN和品类筛选
- **WHEN** 前端请求GET /api/ads/product-ranking?topN=10&category=电子产品
- **THEN** 返回电子产品品类下销量前10的商品排行

#### Scenario: 热销商品排行榜页面
- **WHEN** 用户访问 /product-ranking 页面
- **THEN** 页面顶部提供品类下拉筛选和TopN下拉切换（10/20/50），主体显示排名表格（rank、商品名、品牌、品类、销量、销售额），左侧显示Top3的ECharts柱状图

### Requirement: 区域销售分析API与页面
系统 SHALL 提供REST API `GET /api/ads/regional-sales` 查询区域销售数据，渲染为中国地图热力图页面。

#### Scenario: API返回区域销售数据
- **WHEN** 前端请求GET /api/ads/regional-sales?level=province
- **THEN** 返回省级销售汇总数据，包含province、total_orders、total_amount字段

#### Scenario: 区域销售地图热力图
- **WHEN** 用户访问 /regional-sales 页面
- **THEN** 页面显示：（1）ECharts中国地图（需要中国GeoJSON），省份按销售额着色（深色=高销售额）；（2）右侧显示省份Top10排行榜

### Requirement: 首页仪表盘
系统 SHALL 提供首页仪表盘 `/`，聚合展示核心KPI卡片（总销售额、总订单数、活跃用户数、动销SKU数）和品类营收迷你图、每日趋势迷你图。

#### Scenario: 仪表盘加载成功
- **WHEN** 用户访问首页 /
- **THEN** 页面顶部显示4个KPI数字卡片（带动画计数效果），中部左侧显示品类营收Top5饼图，中部右侧显示近7天销售趋势迷你折线图，底部显示最新热销商品Top5列表

### Requirement: Spring Boot本地启动与验证
系统 SHALL 在本地编译 Spring Boot Jar 包后启动，浏览器访问验证所有页面数据正确渲染。

#### Scenario: 应用编译打包
- **WHEN** 执行 `mvn clean package -pl dw-visualization -DskipTests`
- **THEN** 生成 `dw-visualization-1.0.jar`，可直接运行

#### Scenario: 本地启动成功
- **WHEN** 执行 `java -jar dw-visualization/target/dw-visualization-1.0.jar`
- **THEN** 应用监听 8080 端口，Thymeleaf 模板引擎正常加载，MyBatis-Plus 连接 PostgreSQL 成功

#### Scenario: 仪表盘API数据返回
- **WHEN** 请求 `GET /api/dashboard`
- **THEN** 返回 JSON：total_sales_amount、total_order_count、active_user_count、active_sku_count 四个 KPI 值

#### Scenario: 全部6个页面可访问
- **WHEN** 浏览器依次访问 `/` `/category-revenue` `/daily-sales-trend` `/user-value` `/product-ranking` `/regional-sales`
- **THEN** 每个页面 ECharts 图表正常渲染，无 JS 错误，数据与 ADS 表一致

#### Scenario: API返回格式验证
- **WHEN** 用 curl 请求 `GET /api/ads/daily-sales-trend?startDate=2026-07-01&endDate=2026-07-10`
- **THEN** 返回 Content-Type: application/json，数组元素包含完整字段
