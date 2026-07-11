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
    product_id      VARCHAR(64)     PRIMARY KEY,                              -- 商品ID（UUID），主键
    name            VARCHAR(500)    NOT NULL,                                 -- 商品名称
    brand           VARCHAR(100),                                            -- 品牌
    category        VARCHAR(50)     NOT NULL,                                 -- 品类（电子产品/服装/食品饮料/家居生活/美妆个护/母婴/运动户外/图书文娱）
    sub_category    VARCHAR(100),                                            -- 子类（手机/笔记本电脑/男装/护肤等）
    price           DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 商品价格（元），精度2位小数
    description     TEXT,                                                     -- 商品描述
    image_url       VARCHAR(500),                                            -- 商品图片URL
    keywords        VARCHAR(500),                                            -- 关键词（逗号分隔）
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),                   -- 商品创建时间
    is_variant      BOOLEAN         NOT NULL DEFAULT FALSE,                   -- 是否为变体商品（同款不同规格/颜色）
    is_abnormal     BOOLEAN         NOT NULL DEFAULT FALSE,                   -- 异常数据标记（price<0或必填字段缺失时为TRUE）
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()                    -- 数据加载时间
);
COMMENT ON TABLE  dwd.dwd_product             IS 'DWD层-商品明细表（清洗后的商品数据）';

-- 2.2 订单主表
CREATE TABLE IF NOT EXISTS dwd.dwd_order (
    order_id            VARCHAR(32)     PRIMARY KEY,                          -- 订单ID，主键（格式: ORD + 年月日时分秒毫秒）
    buyer_name          VARCHAR(100),                                        -- 买家姓名
    buyer_phone         VARCHAR(20),                                         -- 买家手机号
    buyer_email         VARCHAR(200),                                        -- 买家邮箱
    province            VARCHAR(50),                                         -- 省份
    city                VARCHAR(50),                                         -- 城市
    district            VARCHAR(50),                                         -- 区/县
    address_detail      VARCHAR(500),                                        -- 详细地址
    postal_code         VARCHAR(10),                                         -- 邮政编码
    recipient_name      VARCHAR(100),                                        -- 收件人姓名
    recipient_phone     VARCHAR(20),                                         -- 收件人手机号
    order_status        VARCHAR(10)     NOT NULL DEFAULT '待付款',            -- 订单状态（待付款/已付款/已发货/已完成/已取消）
    payment_method      VARCHAR(20),                                         -- 支付方式（微信支付/支付宝/银行卡/货到付款）
    payment_amount      DECIMAL(15, 2),                                      -- 实付金额（元）
    payment_time        TIMESTAMP,                                           -- 支付时间，待付款/已取消时为NULL
    logistics_company   VARCHAR(100),                                        -- 物流公司（顺丰速运/中通快递/圆通速递等）
    tracking_number     VARCHAR(100),                                        -- 物流单号，待付款/已付款时为NULL
    shipped_at          TIMESTAMP,                                           -- 发货时间，待付款/已付款时为NULL
    estimated_delivery  DATE,                                                -- 预计送达日期
    order_amount        DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,               -- 订单金额（元），折扣前
    discount            DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,               -- 优惠金额（元）
    actual_amount       DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,               -- 实际支付金额（元），已取消订单为0
    item_count          INT             NOT NULL DEFAULT 0,                  -- 商品件数
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),               -- 订单创建时间
    remark              TEXT,                                                 -- 订单备注
    load_time           TIMESTAMP       NOT NULL DEFAULT NOW()               -- 数据加载时间
);
COMMENT ON TABLE  dwd.dwd_order IS 'DWD层-订单主表（清洗后的订单数据）';

-- 2.3 订单商品明细表（items 数组展开）
CREATE TABLE IF NOT EXISTS dwd.dwd_order_item (
    id              BIGSERIAL       PRIMARY KEY,                              -- 自增主键
    order_id        VARCHAR(32)     NOT NULL,                                 -- 订单ID，关联 dwd.dwd_order
    product_id      VARCHAR(64)     NOT NULL,                                 -- 商品ID，关联 dwd.dwd_product
    product_name    VARCHAR(500)    NOT NULL,                                 -- 商品名称（订单快照）
    brand           VARCHAR(100),                                            -- 品牌（订单快照）
    category        VARCHAR(50),                                             -- 品类（订单快照）
    sub_category    VARCHAR(100),                                            -- 子类（订单快照）
    original_price  DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 商品原价（元）
    price           DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 实际售价（元）
    quantity        INT             NOT NULL DEFAULT 1,                      -- 购买数量
    sales_volume    INT             NOT NULL DEFAULT 1,                      -- 销售数量（通常与quantity一致）
    subtotal        DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 小计金额（元），price * quantity
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()                   -- 数据加载时间
);
CREATE INDEX IF NOT EXISTS idx_order_item_order_id   ON dwd.dwd_order_item(order_id);
CREATE INDEX IF NOT EXISTS idx_order_item_product_id ON dwd.dwd_order_item(product_id);
COMMENT ON TABLE  dwd.dwd_order_item IS 'DWD层-订单商品明细表（items数组展开后的逐行数据）';

-- =============================================================================
-- 3. DWS 层 - 汇总数据（Data Warehouse Summary）
-- 说明: 对 DWD 明细数据进行轻中度汇总，构建宽表与日汇总表，便于 ADS 层快速计算指标
-- =============================================================================

-- 3.1 订单商品宽表（核心宽表，关联订单+商品+用户+物流全部维度）
CREATE TABLE IF NOT EXISTS dws.dws_order_wide (
    order_id            VARCHAR(32)     NOT NULL,                             -- 订单ID
    product_id          VARCHAR(64)     NOT NULL,                             -- 商品ID
    product_name        VARCHAR(500),                                        -- 商品名称
    brand               VARCHAR(100),                                        -- 品牌
    category            VARCHAR(50),                                         -- 品类
    sub_category        VARCHAR(100),                                        -- 子类
    price               DECIMAL(15, 2),                                      -- 商品售价（元）
    quantity            INT,                                                  -- 购买数量
    subtotal            DECIMAL(15, 2),                                      -- 小计金额（元）
    order_status        VARCHAR(10),                                         -- 订单状态
    buyer_name          VARCHAR(100),                                        -- 买家姓名
    buyer_phone         VARCHAR(20),                                         -- 买家手机号（用户唯一标识）
    buyer_email         VARCHAR(200),                                        -- 买家邮箱
    province            VARCHAR(50),                                         -- 省份
    city                VARCHAR(50),                                         -- 城市
    district            VARCHAR(50),                                         -- 区/县
    payment_method      VARCHAR(20),                                         -- 支付方式
    payment_amount      DECIMAL(15, 2),                                      -- 实付金额（元）
    logistics_company   VARCHAR(100),                                        -- 物流公司
    tracking_number     VARCHAR(100),                                        -- 物流单号
    order_amount        DECIMAL(15, 2),                                      -- 订单金额（元）
    discount            DECIMAL(15, 2),                                      -- 优惠金额（元）
    actual_amount       DECIMAL(15, 2),                                      -- 实际支付金额（元）
    order_created_at    TIMESTAMP,                                           -- 订单创建时间
    dt                  DATE            NOT NULL DEFAULT CURRENT_DATE,        -- 分区日期（订单日期，按天分区）
    load_time           TIMESTAMP       NOT NULL DEFAULT NOW(),               -- 数据加载时间
    PRIMARY KEY (order_id, product_id)
);
CREATE INDEX IF NOT EXISTS idx_wide_dt            ON dws.dws_order_wide(dt);            -- 日期筛选
CREATE INDEX IF NOT EXISTS idx_wide_category      ON dws.dws_order_wide(category);      -- 品类筛选
CREATE INDEX IF NOT EXISTS idx_wide_province      ON dws.dws_order_wide(province);      -- 省份筛选
CREATE INDEX IF NOT EXISTS idx_wide_product_id    ON dws.dws_order_wide(product_id);    -- 商品关联
COMMENT ON TABLE  dws.dws_order_wide IS 'DWS层-订单商品宽表（订单+商品+用户+物流全维度关联）';

-- 3.2 商品日销售汇总
CREATE TABLE IF NOT EXISTS dws.dws_product_sales_1d (
    product_id      VARCHAR(64)     NOT NULL,                                 -- 商品ID
    dt              DATE            NOT NULL,                                 -- 统计日期
    product_name    VARCHAR(500),                                            -- 商品名称
    category        VARCHAR(50),                                             -- 品类
    brand           VARCHAR(100),                                            -- 品牌
    total_quantity  BIGINT          NOT NULL DEFAULT 0,                      -- 当日总销量（件）
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 当日总销售额（元）
    order_count     BIGINT          NOT NULL DEFAULT 0,                      -- 当日包含该商品的订单数
    avg_price       DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 当日平均售价（元），total_amount / total_quantity
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW(),                   -- 数据加载时间
    PRIMARY KEY (product_id, dt)
);
CREATE INDEX IF NOT EXISTS idx_product_sales_dt ON dws.dws_product_sales_1d(dt);
COMMENT ON TABLE  dws.dws_product_sales_1d IS 'DWS层-商品日销售汇总（按商品+日期聚合）';

-- 3.3 品类日销售汇总
CREATE TABLE IF NOT EXISTS dws.dws_category_sales_1d (
    category        VARCHAR(50)     NOT NULL,                                 -- 品类名称
    dt              DATE            NOT NULL,                                 -- 统计日期
    total_quantity  BIGINT          NOT NULL DEFAULT 0,                      -- 当日该品类总销量（件）
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 当日该品类总销售额（元）
    order_count     BIGINT          NOT NULL DEFAULT 0,                      -- 当日该品类订单数
    sku_count       BIGINT          NOT NULL DEFAULT 0,                      -- 当日该品类动销SKU数（去重product_id数）
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW(),                   -- 数据加载时间
    PRIMARY KEY (category, dt)
);
CREATE INDEX IF NOT EXISTS idx_category_sales_dt ON dws.dws_category_sales_1d(dt);
COMMENT ON TABLE  dws.dws_category_sales_1d IS 'DWS层-品类日销售汇总（按品类+日期聚合）';

-- 3.4 用户日行为汇总
CREATE TABLE IF NOT EXISTS dws.dws_user_order_1d (
    buyer_phone         VARCHAR(20)     NOT NULL,                             -- 买家手机号（用户唯一标识）
    dt                  DATE            NOT NULL,                             -- 统计日期
    buyer_name          VARCHAR(100),                                        -- 买家姓名
    order_count         BIGINT          NOT NULL DEFAULT 0,                  -- 当日下单次数
    total_amount        DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,               -- 当日消费总金额（元）
    total_quantity      BIGINT          NOT NULL DEFAULT 0,                  -- 当日购买商品总件数
    completed_count     BIGINT          NOT NULL DEFAULT 0,                  -- 当日已完成订单数
    cancelled_count     BIGINT          NOT NULL DEFAULT 0,                  -- 当日已取消订单数
    pending_count       BIGINT          NOT NULL DEFAULT 0,                  -- 当日待付款/已付款/已发货订单数
    load_time           TIMESTAMP       NOT NULL DEFAULT NOW(),               -- 数据加载时间
    PRIMARY KEY (buyer_phone, dt)
);
CREATE INDEX IF NOT EXISTS idx_user_order_dt ON dws.dws_user_order_1d(dt);
COMMENT ON TABLE  dws.dws_user_order_1d IS 'DWS层-用户日行为汇总（按用户手机号+日期聚合）';

-- =============================================================================
-- 4. ADS 层 - 应用指标（Application Data Service）
-- 说明: 面向可视化的业务指标表，供 Spring Boot 直接查询展示
-- =============================================================================

-- 4.1 品类营收排行
CREATE TABLE IF NOT EXISTS ads.ads_category_revenue (
    category        VARCHAR(50)     PRIMARY KEY,                              -- 品类名称，主键
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 累计销售额（元）
    total_quantity  BIGINT          NOT NULL DEFAULT 0,                      -- 累计销量（件）
    order_count     BIGINT          NOT NULL DEFAULT 0,                      -- 累计订单数
    sku_count       BIGINT          NOT NULL DEFAULT 0,                      -- 累计动销SKU数
    percentage      DECIMAL(5, 2)   NOT NULL DEFAULT 0.00,                   -- 销售额占比（%），所有品类总和=100%
    rank            INT             NOT NULL DEFAULT 0,                      -- 排名（按销售额降序，从1开始）
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()                   -- 数据加载时间
);
COMMENT ON TABLE  ads.ads_category_revenue IS 'ADS层-品类营收排行（按累计销售额降序排名）';

-- 4.2 每日销售趋势
CREATE TABLE IF NOT EXISTS ads.ads_daily_sales_trend (
    dt                  DATE            PRIMARY KEY,                          -- 日期，主键
    total_sales_amount  DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,               -- 当日总销售额（元）
    total_order_count   BIGINT          NOT NULL DEFAULT 0,                  -- 当日总订单数
    avg_order_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,               -- 当日平均客单价（元），total_sales_amount / total_order_count
    load_time           TIMESTAMP       NOT NULL DEFAULT NOW()               -- 数据加载时间
);
COMMENT ON TABLE  ads.ads_daily_sales_trend IS 'ADS层-每日销售趋势（全天汇总）';

-- 4.3 用户价值分析 (RFM)
CREATE TABLE IF NOT EXISTS ads.ads_user_value (
    buyer_phone     VARCHAR(20)     PRIMARY KEY,                              -- 买家手机号，主键
    buyer_name      VARCHAR(100),                                            -- 买家姓名
    last_order_date DATE,                                                    -- 最近购买日期（Recency）
    order_frequency BIGINT          NOT NULL DEFAULT 0,                      -- 累计下单次数（Frequency）
    total_monetary  DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 累计消费金额（Monetary），元
    avg_order_amount DECIMAL(15, 2) NOT NULL DEFAULT 0.00,                   -- 平均客单价（元）
    value_tier      VARCHAR(10)     NOT NULL DEFAULT '低价值',                -- 价值分层: 高价值(>=10000) / 中价值(1000-10000) / 低价值(<1000)
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()                   -- 数据加载时间
);
CREATE INDEX IF NOT EXISTS idx_user_value_tier ON ads.ads_user_value(value_tier);
COMMENT ON TABLE  ads.ads_user_value IS 'ADS层-用户价值分析（RFM模型，Recency/Frequency/Monetary）';

-- 4.4 商品热销排行
CREATE TABLE IF NOT EXISTS ads.ads_product_ranking (
    product_id      VARCHAR(64)     PRIMARY KEY,                              -- 商品ID，主键
    product_name    VARCHAR(500),                                            -- 商品名称
    category        VARCHAR(50),                                             -- 品类
    brand           VARCHAR(100),                                            -- 品牌
    total_quantity  BIGINT          NOT NULL DEFAULT 0,                      -- 累计销量（件）
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 累计销售额（元）
    rank            INT             NOT NULL DEFAULT 0,                      -- 排名（按累计销量降序，从1开始）
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW()                   -- 数据加载时间
);
CREATE INDEX IF NOT EXISTS idx_product_ranking_rank ON ads.ads_product_ranking(rank);
CREATE INDEX IF NOT EXISTS idx_product_ranking_cat  ON ads.ads_product_ranking(category);
COMMENT ON TABLE  ads.ads_product_ranking IS 'ADS层-商品热销排行（按累计销量降序排名）';

-- 4.5 区域销售分析（省市区三级）
CREATE TABLE IF NOT EXISTS ads.ads_regional_sales (
    province        VARCHAR(50)     NOT NULL,                                 -- 省份
    city            VARCHAR(50)     NOT NULL DEFAULT '合计',                  -- 城市，省级汇总时为'合计'
    district        VARCHAR(50)     NOT NULL DEFAULT '合计',                  -- 区/县，省级/市级汇总时为'合计'
    total_orders    BIGINT          NOT NULL DEFAULT 0,                      -- 订单总数
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,                   -- 销售总额（元）
    avg_order_amount DECIMAL(15, 2) NOT NULL DEFAULT 0.00,                   -- 平均客单价（元）
    buyer_count     BIGINT          NOT NULL DEFAULT 0,                      -- 去重买家数
    load_time       TIMESTAMP       NOT NULL DEFAULT NOW(),                   -- 数据加载时间
    PRIMARY KEY (province, city, district)
);
CREATE INDEX IF NOT EXISTS idx_regional_province ON ads.ads_regional_sales(province);
COMMENT ON TABLE  ads.ads_regional_sales IS 'ADS层-区域销售分析（省市区三级维度的销售数据）';
