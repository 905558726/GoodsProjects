-- =============================================================================
-- 电商数据仓库 - 建表SQL脚本
-- 数据库: DataWarehouse
-- Schema: dwd (明细) / dws (汇总) / ads (指标)
-- 执行方式: psql -h <DB_HOST> -U <DB_USER> -d DataWarehouse -f init_database.sql
-- =============================================================================

-- =============================================================================
-- 1. 创建 Schema
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS dwd;
CREATE SCHEMA IF NOT EXISTS dws;
CREATE SCHEMA IF NOT EXISTS ads;

-- =============================================================================
-- 2. DWD 层 - 明细数据（Data Warehouse Detail）
-- 说明: 对 Kafka ODS 原始数据进行清洗、去重、字段标准化后的明细数据
-- =============================================================================

-- 2.1 商品明细表
CREATE TABLE IF NOT EXISTS dwd.dwd_product (
    product_id      VARCHAR(64)     PRIMARY KEY,
    name            VARCHAR(500)    NOT NULL,
    brand           VARCHAR(100),
    category        VARCHAR(50)     NOT NULL,
    sub_category    VARCHAR(100),
    price           DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    description     TEXT,
    image_url       VARCHAR(500),
    keywords        VARCHAR(500),
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    is_variant      BOOLEAN         NOT NULL DEFAULT FALSE,
    is_abnormal     BOOLEAN         NOT NULL DEFAULT FALSE,
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE  dwd.dwd_product IS 'DWD层-商品明细表（清洗后的商品数据）';
COMMENT ON COLUMN dwd.dwd_product.product_id   IS '商品ID（UUID），主键';
COMMENT ON COLUMN dwd.dwd_product.name         IS '商品名称';
COMMENT ON COLUMN dwd.dwd_product.brand        IS '品牌';
COMMENT ON COLUMN dwd.dwd_product.category     IS '品类（电子产品/服装/食品饮料/家居生活/美妆个护/母婴/运动户外/图书文娱）';
COMMENT ON COLUMN dwd.dwd_product.sub_category IS '子类（手机/笔记本电脑/男装/护肤等）';
COMMENT ON COLUMN dwd.dwd_product.price        IS '商品价格（元），精度2位小数';
COMMENT ON COLUMN dwd.dwd_product.description  IS '商品描述';
COMMENT ON COLUMN dwd.dwd_product.image_url    IS '商品图片URL';
COMMENT ON COLUMN dwd.dwd_product.keywords     IS '关键词（逗号分隔）';
COMMENT ON COLUMN dwd.dwd_product.created_at   IS '商品创建时间';
COMMENT ON COLUMN dwd.dwd_product.is_variant   IS '是否为变体商品（同款不同规格/颜色）';
COMMENT ON COLUMN dwd.dwd_product.is_abnormal  IS '异常数据标记（price<0或必填字段缺失时为TRUE）';
COMMENT ON COLUMN dwd.dwd_product.load_time    IS '数据加载时间';

-- 2.2 订单主表
CREATE TABLE IF NOT EXISTS dwd.dwd_order (
    order_id            VARCHAR(32)     PRIMARY KEY,
    buyer_name          VARCHAR(100),
    buyer_phone         VARCHAR(20),
    buyer_email         VARCHAR(200),
    province            VARCHAR(50),
    city                VARCHAR(50),
    district            VARCHAR(50),
    address_detail      VARCHAR(500),
    postal_code         VARCHAR(10),
    recipient_name      VARCHAR(100),
    recipient_phone     VARCHAR(20),
    order_status        VARCHAR(10)     NOT NULL DEFAULT '待付款',
    payment_method      VARCHAR(20),
    payment_amount      DECIMAL(15, 2),
    payment_time        TIMESTAMP,
    logistics_company   VARCHAR(100),
    tracking_number     VARCHAR(100),
    shipped_at          TIMESTAMP,
    estimated_delivery  DATE,
    order_amount        DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    discount            DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    actual_amount       DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    item_count          INT             NOT NULL DEFAULT 0,
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    remark              TEXT,
    load_time           TIMESTAMP       NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE  dwd.dwd_order IS 'DWD层-订单主表（清洗后的订单数据）';
COMMENT ON COLUMN dwd.dwd_order.order_id           IS '订单ID，主键（格式: ORD + 年月日时分秒毫秒）';
COMMENT ON COLUMN dwd.dwd_order.buyer_name         IS '买家姓名';
COMMENT ON COLUMN dwd.dwd_order.buyer_phone        IS '买家手机号';
COMMENT ON COLUMN dwd.dwd_order.buyer_email        IS '买家邮箱';
COMMENT ON COLUMN dwd.dwd_order.province           IS '省份';
COMMENT ON COLUMN dwd.dwd_order.city               IS '城市';
COMMENT ON COLUMN dwd.dwd_order.district           IS '区/县';
COMMENT ON COLUMN dwd.dwd_order.address_detail     IS '详细地址';
COMMENT ON COLUMN dwd.dwd_order.postal_code        IS '邮政编码';
COMMENT ON COLUMN dwd.dwd_order.recipient_name     IS '收件人姓名';
COMMENT ON COLUMN dwd.dwd_order.recipient_phone    IS '收件人手机号';
COMMENT ON COLUMN dwd.dwd_order.order_status       IS '订单状态（待付款/已付款/已发货/已完成/已取消）';
COMMENT ON COLUMN dwd.dwd_order.payment_method     IS '支付方式（微信支付/支付宝/银行卡/货到付款）';
COMMENT ON COLUMN dwd.dwd_order.payment_amount     IS '实付金额（元）';
COMMENT ON COLUMN dwd.dwd_order.payment_time       IS '支付时间，待付款/已取消时为NULL';
COMMENT ON COLUMN dwd.dwd_order.logistics_company  IS '物流公司（顺丰速运/中通快递/圆通速递等）';
COMMENT ON COLUMN dwd.dwd_order.tracking_number    IS '物流单号，待付款/已付款时为NULL';
COMMENT ON COLUMN dwd.dwd_order.shipped_at         IS '发货时间，待付款/已付款时为NULL';
COMMENT ON COLUMN dwd.dwd_order.estimated_delivery IS '预计送达日期';
COMMENT ON COLUMN dwd.dwd_order.order_amount       IS '订单金额（元），折扣前';
COMMENT ON COLUMN dwd.dwd_order.discount           IS '优惠金额（元）';
COMMENT ON COLUMN dwd.dwd_order.actual_amount      IS '实际支付金额（元），已取消订单为0';
COMMENT ON COLUMN dwd.dwd_order.item_count         IS '商品件数';
COMMENT ON COLUMN dwd.dwd_order.created_at         IS '订单创建时间';
COMMENT ON COLUMN dwd.dwd_order.remark             IS '订单备注';
COMMENT ON COLUMN dwd.dwd_order.load_time          IS '数据加载时间';

-- 2.3 订单商品明细表（items 数组展开）
CREATE TABLE IF NOT EXISTS dwd.dwd_order_item (
    id              BIGSERIAL       PRIMARY KEY,
    order_id        VARCHAR(32)     NOT NULL,
    product_id      VARCHAR(64)     NOT NULL,
    product_name    VARCHAR(500)    NOT NULL,
    brand           VARCHAR(100),
    category        VARCHAR(50),
    sub_category    VARCHAR(100),
    original_price  DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    price           DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    quantity        INT             NOT NULL DEFAULT 1,
    sales_volume    INT             NOT NULL DEFAULT 1,
    subtotal        DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_order_item_order_id   ON dwd.dwd_order_item(order_id);
CREATE INDEX IF NOT EXISTS idx_order_item_product_id ON dwd.dwd_order_item(product_id);
COMMENT ON TABLE  dwd.dwd_order_item IS 'DWD层-订单商品明细表（items数组展开后的逐行数据）';
COMMENT ON COLUMN dwd.dwd_order_item.id             IS '自增主键';
COMMENT ON COLUMN dwd.dwd_order_item.order_id       IS '订单ID，关联 dwd.dwd_order';
COMMENT ON COLUMN dwd.dwd_order_item.product_id     IS '商品ID，关联 dwd.dwd_product';
COMMENT ON COLUMN dwd.dwd_order_item.product_name   IS '商品名称（订单快照）';
COMMENT ON COLUMN dwd.dwd_order_item.brand          IS '品牌（订单快照）';
COMMENT ON COLUMN dwd.dwd_order_item.category       IS '品类（订单快照）';
COMMENT ON COLUMN dwd.dwd_order_item.sub_category   IS '子类（订单快照）';
COMMENT ON COLUMN dwd.dwd_order_item.original_price IS '商品原价（元）';
COMMENT ON COLUMN dwd.dwd_order_item.price          IS '实际售价（元）';
COMMENT ON COLUMN dwd.dwd_order_item.quantity       IS '购买数量';
COMMENT ON COLUMN dwd.dwd_order_item.sales_volume   IS '销售数量（通常与quantity一致）';
COMMENT ON COLUMN dwd.dwd_order_item.subtotal       IS '小计金额（元），price * quantity';
COMMENT ON COLUMN dwd.dwd_order_item.load_time      IS '数据加载时间';

-- =============================================================================
-- 3. DWS 层 - 汇总数据（Data Warehouse Summary）
-- 说明: 对 DWD 明细数据进行轻中度汇总，构建宽表与日汇总表，便于 ADS 层快速计算指标
-- =============================================================================

-- 3.1 订单商品宽表（核心宽表，关联订单+商品+用户+物流全部维度）
CREATE TABLE IF NOT EXISTS dws.dws_order_wide (
    order_id            VARCHAR(32)     NOT NULL,
    product_id          VARCHAR(64)     NOT NULL,
    product_name        VARCHAR(500),
    brand               VARCHAR(100),
    category            VARCHAR(50),
    sub_category        VARCHAR(100),
    price               DECIMAL(15, 2),
    quantity            INT,
    subtotal            DECIMAL(15, 2),
    order_status        VARCHAR(10),
    buyer_name          VARCHAR(100),
    buyer_phone         VARCHAR(20),
    buyer_email         VARCHAR(200),
    province            VARCHAR(50),
    city                VARCHAR(50),
    district            VARCHAR(50),
    payment_method      VARCHAR(20),
    payment_amount      DECIMAL(15, 2),
    logistics_company   VARCHAR(100),
    tracking_number     VARCHAR(100),
    order_amount        DECIMAL(15, 2),
    discount            DECIMAL(15, 2),
    actual_amount       DECIMAL(15, 2),
    order_created_at    TIMESTAMP,
    dt                  DATE            NOT NULL DEFAULT CURRENT_DATE,
    load_time           TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, product_id)
);
CREATE INDEX IF NOT EXISTS idx_wide_dt         ON dws.dws_order_wide(dt);
CREATE INDEX IF NOT EXISTS idx_wide_category   ON dws.dws_order_wide(category);
CREATE INDEX IF NOT EXISTS idx_wide_province   ON dws.dws_order_wide(province);
CREATE INDEX IF NOT EXISTS idx_wide_product_id ON dws.dws_order_wide(product_id);
COMMENT ON TABLE  dws.dws_order_wide IS 'DWS层-订单商品宽表（订单+商品+用户+物流全维度关联）';
COMMENT ON COLUMN dws.dws_order_wide.order_id          IS '订单ID';
COMMENT ON COLUMN dws.dws_order_wide.product_id        IS '商品ID';
COMMENT ON COLUMN dws.dws_order_wide.product_name      IS '商品名称';
COMMENT ON COLUMN dws.dws_order_wide.brand             IS '品牌';
COMMENT ON COLUMN dws.dws_order_wide.category          IS '品类';
COMMENT ON COLUMN dws.dws_order_wide.sub_category      IS '子类';
COMMENT ON COLUMN dws.dws_order_wide.price             IS '商品售价（元）';
COMMENT ON COLUMN dws.dws_order_wide.quantity          IS '购买数量';
COMMENT ON COLUMN dws.dws_order_wide.subtotal          IS '小计金额（元）';
COMMENT ON COLUMN dws.dws_order_wide.order_status      IS '订单状态';
COMMENT ON COLUMN dws.dws_order_wide.buyer_name        IS '买家姓名';
COMMENT ON COLUMN dws.dws_order_wide.buyer_phone       IS '买家手机号（用户唯一标识）';
COMMENT ON COLUMN dws.dws_order_wide.buyer_email       IS '买家邮箱';
COMMENT ON COLUMN dws.dws_order_wide.province          IS '省份';
COMMENT ON COLUMN dws.dws_order_wide.city              IS '城市';
COMMENT ON COLUMN dws.dws_order_wide.district          IS '区/县';
COMMENT ON COLUMN dws.dws_order_wide.payment_method    IS '支付方式';
COMMENT ON COLUMN dws.dws_order_wide.payment_amount    IS '实付金额（元）';
COMMENT ON COLUMN dws.dws_order_wide.logistics_company IS '物流公司';
COMMENT ON COLUMN dws.dws_order_wide.tracking_number   IS '物流单号';
COMMENT ON COLUMN dws.dws_order_wide.order_amount      IS '订单金额（元）';
COMMENT ON COLUMN dws.dws_order_wide.discount          IS '优惠金额（元）';
COMMENT ON COLUMN dws.dws_order_wide.actual_amount     IS '实际支付金额（元）';
COMMENT ON COLUMN dws.dws_order_wide.order_created_at  IS '订单创建时间';
COMMENT ON COLUMN dws.dws_order_wide.dt                IS '分区日期（订单日期，按天分区）';
COMMENT ON COLUMN dws.dws_order_wide.load_time         IS '数据加载时间';

-- 3.2 商品日销售汇总
CREATE TABLE IF NOT EXISTS dws.dws_product_sales_1d (
    product_id      VARCHAR(64)     NOT NULL,
    dt              DATE            NOT NULL,
    product_name    VARCHAR(500),
    category        VARCHAR(50),
    brand           VARCHAR(100),
    total_quantity  BIGINT          NOT NULL DEFAULT 0,
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    order_count     BIGINT          NOT NULL DEFAULT 0,
    avg_price       DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, dt)
);
CREATE INDEX IF NOT EXISTS idx_product_sales_dt ON dws.dws_product_sales_1d(dt);
COMMENT ON TABLE  dws.dws_product_sales_1d IS 'DWS层-商品日销售汇总（按商品+日期聚合）';
COMMENT ON COLUMN dws.dws_product_sales_1d.product_id     IS '商品ID';
COMMENT ON COLUMN dws.dws_product_sales_1d.dt             IS '统计日期';
COMMENT ON COLUMN dws.dws_product_sales_1d.product_name   IS '商品名称';
COMMENT ON COLUMN dws.dws_product_sales_1d.category       IS '品类';
COMMENT ON COLUMN dws.dws_product_sales_1d.brand          IS '品牌';
COMMENT ON COLUMN dws.dws_product_sales_1d.total_quantity IS '当日总销量（件）';
COMMENT ON COLUMN dws.dws_product_sales_1d.total_amount   IS '当日总销售额（元）';
COMMENT ON COLUMN dws.dws_product_sales_1d.order_count    IS '当日包含该商品的订单数';
COMMENT ON COLUMN dws.dws_product_sales_1d.avg_price      IS '当日平均售价（元），total_amount / total_quantity';
COMMENT ON COLUMN dws.dws_product_sales_1d.load_time      IS '数据加载时间';

-- 3.3 品类日销售汇总
CREATE TABLE IF NOT EXISTS dws.dws_category_sales_1d (
    category        VARCHAR(50)     NOT NULL,
    dt              DATE            NOT NULL,
    total_quantity  BIGINT          NOT NULL DEFAULT 0,
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    order_count     BIGINT          NOT NULL DEFAULT 0,
    sku_count       BIGINT          NOT NULL DEFAULT 0,
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (category, dt)
);
CREATE INDEX IF NOT EXISTS idx_category_sales_dt ON dws.dws_category_sales_1d(dt);
COMMENT ON TABLE  dws.dws_category_sales_1d IS 'DWS层-品类日销售汇总（按品类+日期聚合）';
COMMENT ON COLUMN dws.dws_category_sales_1d.category       IS '品类名称';
COMMENT ON COLUMN dws.dws_category_sales_1d.dt             IS '统计日期';
COMMENT ON COLUMN dws.dws_category_sales_1d.total_quantity IS '当日该品类总销量（件）';
COMMENT ON COLUMN dws.dws_category_sales_1d.total_amount   IS '当日该品类总销售额（元）';
COMMENT ON COLUMN dws.dws_category_sales_1d.order_count    IS '当日该品类订单数';
COMMENT ON COLUMN dws.dws_category_sales_1d.sku_count      IS '当日该品类动销SKU数（去重product_id数）';
COMMENT ON COLUMN dws.dws_category_sales_1d.load_time      IS '数据加载时间';

-- 3.4 用户日行为汇总
CREATE TABLE IF NOT EXISTS dws.dws_user_order_1d (
    buyer_phone         VARCHAR(20)     NOT NULL,
    dt                  DATE            NOT NULL,
    buyer_name          VARCHAR(100),
    order_count         BIGINT          NOT NULL DEFAULT 0,
    total_amount        DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    total_quantity      BIGINT          NOT NULL DEFAULT 0,
    completed_count     BIGINT          NOT NULL DEFAULT 0,
    cancelled_count     BIGINT          NOT NULL DEFAULT 0,
    pending_count       BIGINT          NOT NULL DEFAULT 0,
    load_time           TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (buyer_phone, dt)
);
CREATE INDEX IF NOT EXISTS idx_user_order_dt ON dws.dws_user_order_1d(dt);
COMMENT ON TABLE  dws.dws_user_order_1d IS 'DWS层-用户日行为汇总（按用户手机号+日期聚合）';
COMMENT ON COLUMN dws.dws_user_order_1d.buyer_phone     IS '买家手机号（用户唯一标识）';
COMMENT ON COLUMN dws.dws_user_order_1d.dt              IS '统计日期';
COMMENT ON COLUMN dws.dws_user_order_1d.buyer_name      IS '买家姓名';
COMMENT ON COLUMN dws.dws_user_order_1d.order_count     IS '当日下单次数';
COMMENT ON COLUMN dws.dws_user_order_1d.total_amount    IS '当日消费总金额（元）';
COMMENT ON COLUMN dws.dws_user_order_1d.total_quantity  IS '当日购买商品总件数';
COMMENT ON COLUMN dws.dws_user_order_1d.completed_count IS '当日已完成订单数';
COMMENT ON COLUMN dws.dws_user_order_1d.cancelled_count IS '当日已取消订单数';
COMMENT ON COLUMN dws.dws_user_order_1d.pending_count   IS '当日待付款/已付款/已发货订单数';
COMMENT ON COLUMN dws.dws_user_order_1d.load_time       IS '数据加载时间';

-- =============================================================================
-- 4. ADS 层 - 应用指标（Application Data Service）
-- 说明: 面向可视化的业务指标表，供 Spring Boot 直接查询展示
-- =============================================================================

-- 4.1 品类营收排行
CREATE TABLE IF NOT EXISTS ads.ads_category_revenue (
    category        VARCHAR(50)     PRIMARY KEY,
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    total_quantity  BIGINT          NOT NULL DEFAULT 0,
    order_count     BIGINT          NOT NULL DEFAULT 0,
    sku_count       BIGINT          NOT NULL DEFAULT 0,
    percentage      DECIMAL(5, 2)   NOT NULL DEFAULT 0.00,
    rank            INT             NOT NULL DEFAULT 0,
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE  ads.ads_category_revenue IS 'ADS层-品类营收排行（按累计销售额降序排名）';
COMMENT ON COLUMN ads.ads_category_revenue.category       IS '品类名称，主键';
COMMENT ON COLUMN ads.ads_category_revenue.total_amount   IS '累计销售额（元）';
COMMENT ON COLUMN ads.ads_category_revenue.total_quantity IS '累计销量（件）';
COMMENT ON COLUMN ads.ads_category_revenue.order_count    IS '累计订单数';
COMMENT ON COLUMN ads.ads_category_revenue.sku_count      IS '累计动销SKU数';
COMMENT ON COLUMN ads.ads_category_revenue.percentage     IS '销售额占比（%），所有品类总和=100%';
COMMENT ON COLUMN ads.ads_category_revenue.rank           IS '排名（按销售额降序，从1开始）';
COMMENT ON COLUMN ads.ads_category_revenue.load_time      IS '数据加载时间';

-- 4.2 每日销售趋势
CREATE TABLE IF NOT EXISTS ads.ads_daily_sales_trend (
    dt                  DATE            PRIMARY KEY,
    total_sales_amount  DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    total_order_count   BIGINT          NOT NULL DEFAULT 0,
    avg_order_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    load_time           TIMESTAMP       NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE  ads.ads_daily_sales_trend IS 'ADS层-每日销售趋势（全天汇总）';
COMMENT ON COLUMN ads.ads_daily_sales_trend.dt                 IS '日期，主键';
COMMENT ON COLUMN ads.ads_daily_sales_trend.total_sales_amount IS '当日总销售额（元）';
COMMENT ON COLUMN ads.ads_daily_sales_trend.total_order_count  IS '当日总订单数';
COMMENT ON COLUMN ads.ads_daily_sales_trend.avg_order_amount   IS '当日平均客单价（元），total_sales_amount / total_order_count';
COMMENT ON COLUMN ads.ads_daily_sales_trend.load_time          IS '数据加载时间';

-- 4.3 用户价值分析 (RFM)
CREATE TABLE IF NOT EXISTS ads.ads_user_value (
    buyer_phone      VARCHAR(20)     PRIMARY KEY,
    buyer_name       VARCHAR(100),
    last_order_date  DATE,
    order_frequency  BIGINT          NOT NULL DEFAULT 0,
    total_monetary   DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    avg_order_amount DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    value_tier       VARCHAR(10)     NOT NULL DEFAULT '低价值',
    load_time        TIMESTAMP       NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_user_value_tier ON ads.ads_user_value(value_tier);
COMMENT ON TABLE  ads.ads_user_value IS 'ADS层-用户价值分析（RFM模型，Recency/Frequency/Monetary）';
COMMENT ON COLUMN ads.ads_user_value.buyer_phone      IS '买家手机号，主键';
COMMENT ON COLUMN ads.ads_user_value.buyer_name       IS '买家姓名';
COMMENT ON COLUMN ads.ads_user_value.last_order_date  IS '最近购买日期（Recency）';
COMMENT ON COLUMN ads.ads_user_value.order_frequency  IS '累计下单次数（Frequency）';
COMMENT ON COLUMN ads.ads_user_value.total_monetary   IS '累计消费金额（Monetary），元';
COMMENT ON COLUMN ads.ads_user_value.avg_order_amount IS '平均客单价（元）';
COMMENT ON COLUMN ads.ads_user_value.value_tier       IS '价值分层: 高价值(>=10000) / 中价值(1000-10000) / 低价值(<1000)';
COMMENT ON COLUMN ads.ads_user_value.load_time        IS '数据加载时间';

-- 4.4 商品热销排行
CREATE TABLE IF NOT EXISTS ads.ads_product_ranking (
    product_id      VARCHAR(64)     PRIMARY KEY,
    product_name    VARCHAR(500),
    category        VARCHAR(50),
    brand           VARCHAR(100),
    total_quantity  BIGINT          NOT NULL DEFAULT 0,
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    rank            INT             NOT NULL DEFAULT 0,
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_product_ranking_rank ON ads.ads_product_ranking(rank);
CREATE INDEX IF NOT EXISTS idx_product_ranking_cat  ON ads.ads_product_ranking(category);
COMMENT ON TABLE  ads.ads_product_ranking IS 'ADS层-商品热销排行（按累计销量降序排名）';
COMMENT ON COLUMN ads.ads_product_ranking.product_id     IS '商品ID，主键';
COMMENT ON COLUMN ads.ads_product_ranking.product_name   IS '商品名称';
COMMENT ON COLUMN ads.ads_product_ranking.category       IS '品类';
COMMENT ON COLUMN ads.ads_product_ranking.brand          IS '品牌';
COMMENT ON COLUMN ads.ads_product_ranking.total_quantity IS '累计销量（件）';
COMMENT ON COLUMN ads.ads_product_ranking.total_amount   IS '累计销售额（元）';
COMMENT ON COLUMN ads.ads_product_ranking.rank           IS '排名（按累计销量降序，从1开始）';
COMMENT ON COLUMN ads.ads_product_ranking.load_time      IS '数据加载时间';

-- 4.5 区域销售分析（省市区三级）
CREATE TABLE IF NOT EXISTS ads.ads_regional_sales (
    province         VARCHAR(50)     NOT NULL,
    city             VARCHAR(50)     NOT NULL DEFAULT '合计',
    district         VARCHAR(50)     NOT NULL DEFAULT '合计',
    total_orders     BIGINT          NOT NULL DEFAULT 0,
    total_amount     DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    avg_order_amount DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    buyer_count      BIGINT          NOT NULL DEFAULT 0,
    load_time        TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (province, city, district)
);
CREATE INDEX IF NOT EXISTS idx_regional_province ON ads.ads_regional_sales(province);
COMMENT ON TABLE  ads.ads_regional_sales IS 'ADS层-区域销售分析（省市区三级维度的销售数据）';
COMMENT ON COLUMN ads.ads_regional_sales.province         IS '省份';
COMMENT ON COLUMN ads.ads_regional_sales.city             IS '城市，省级汇总时为''合计''';
COMMENT ON COLUMN ads.ads_regional_sales.district         IS '区/县，省级/市级汇总时为''合计''';
COMMENT ON COLUMN ads.ads_regional_sales.total_orders     IS '订单总数';
COMMENT ON COLUMN ads.ads_regional_sales.total_amount     IS '销售总额（元）';
COMMENT ON COLUMN ads.ads_regional_sales.avg_order_amount IS '平均客单价（元）';
COMMENT ON COLUMN ads.ads_regional_sales.buyer_count      IS '去重买家数';
COMMENT ON COLUMN ads.ads_regional_sales.load_time        IS '数据加载时间';
