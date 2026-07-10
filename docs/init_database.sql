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
-- 2. DWD 层 - 明细数据
-- =============================================================================

-- dwd.dwd_product: 清洗后的商品数据
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
COMMENT ON TABLE  dwd.dwd_product             IS '清洗后的商品数据';
COMMENT ON COLUMN dwd.dwd_product.product_id  IS '商品ID（UUID）';
COMMENT ON COLUMN dwd.dwd_product.is_abnormal IS '异常数据标记（price<0 等）';

-- dwd.dwd_order: 清洗后的订单主表
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
COMMENT ON TABLE  dwd.dwd_order               IS '清洗后的订单主表';
COMMENT ON COLUMN dwd.dwd_order.order_status  IS '订单状态：待付款/已付款/已发货/已完成/已取消';

-- dwd.dwd_order_item: 订单商品明细（items 展开）
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
COMMENT ON TABLE  dwd.dwd_order_item                IS '订单商品明细（items 数组展开）';

-- =============================================================================
-- 3. DWS 层 - 汇总数据
-- =============================================================================

-- dws.dws_order_wide: 订单商品宽表
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
CREATE INDEX IF NOT EXISTS idx_wide_dt            ON dws.dws_order_wide(dt);
CREATE INDEX IF NOT EXISTS idx_wide_category      ON dws.dws_order_wide(category);
CREATE INDEX IF NOT EXISTS idx_wide_province      ON dws.dws_order_wide(province);
CREATE INDEX IF NOT EXISTS idx_wide_product_id    ON dws.dws_order_wide(product_id);
COMMENT ON TABLE  dws.dws_order_wide              IS '订单商品宽表（订单+商品+用户+物流）';

-- dws.dws_product_sales_1d: 商品日销售汇总
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
COMMENT ON TABLE  dws.dws_product_sales_1d       IS '商品日销售汇总';

-- dws.dws_category_sales_1d: 品类日销售汇总
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
COMMENT ON TABLE  dws.dws_category_sales_1d       IS '品类日销售汇总';

-- dws.dws_user_order_1d: 用户日行为汇总
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
COMMENT ON TABLE  dws.dws_user_order_1d       IS '用户日行为汇总';

-- =============================================================================
-- 4. ADS 层 - 应用指标
-- =============================================================================

-- ads.ads_category_revenue: 品类营收排行
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
COMMENT ON TABLE  ads.ads_category_revenue IS '品类营收排行指标';

-- ads.ads_daily_sales_trend: 每日销售趋势
CREATE TABLE IF NOT EXISTS ads.ads_daily_sales_trend (
    dt                  DATE            PRIMARY KEY,
    total_sales_amount  DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    total_order_count   BIGINT          NOT NULL DEFAULT 0,
    avg_order_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    load_time           TIMESTAMP       NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE  ads.ads_daily_sales_trend IS '每日销售趋势指标';

-- ads.ads_user_value: 用户价值分析 (RFM)
CREATE TABLE IF NOT EXISTS ads.ads_user_value (
    buyer_phone     VARCHAR(20)     PRIMARY KEY,
    buyer_name      VARCHAR(100),
    last_order_date DATE,
    order_frequency BIGINT          NOT NULL DEFAULT 0,
    total_monetary  DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    avg_order_amount DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    value_tier      VARCHAR(10)     NOT NULL DEFAULT '低价值',
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE  ads.ads_user_value             IS '用户价值分析 (RFM)';
COMMENT ON COLUMN ads.ads_user_value.value_tier  IS '价值分层：高价值/中价值/低价值';

-- ads.ads_product_ranking: 商品热销排行
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
COMMENT ON TABLE  ads.ads_product_ranking IS '商品热销排行指标';

-- ads.ads_regional_sales: 区域销售分析
CREATE TABLE IF NOT EXISTS ads.ads_regional_sales (
    province        VARCHAR(50)     NOT NULL,
    city            VARCHAR(50)     NOT NULL DEFAULT '合计',
    district        VARCHAR(50)     NOT NULL DEFAULT '合计',
    total_orders    BIGINT          NOT NULL DEFAULT 0,
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    avg_order_amount DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    buyer_count     BIGINT          NOT NULL DEFAULT 0,
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (province, city, district)
);
CREATE INDEX IF NOT EXISTS idx_regional_province ON ads.ads_regional_sales(province);
COMMENT ON TABLE  ads.ads_regional_sales IS '区域销售分析（省市区三级）';
