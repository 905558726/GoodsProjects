#!/bin/bash
# =============================================================================
# clean_orders.sh — 随机清理订单数据，保留 50 万条
#
# 策略:
#   1. 以基准库为基准，随机保留 50 万条订单（ORDER BY RANDOM()）
#   2. 其他库清理掉基准库中已删除的对应订单（按 order_id 匹配）
#   3. 清理后重建 DWS 层和 ADS 层（与 Flink Job 聚合逻辑一致）
#   4. 不处理 dim 维度层
#
# 定时执行 (crontab): 每周日 00:00
#   0 0 * * 0 /opt/scripts/clean_orders.sh >> /var/log/clean_orders_cron.log 2>&1
# =============================================================================

set -e

# ---- 配置 ----
PSQL="/opt/postgresql-16.0/bin/psql"
PGUSER="postgres"
PGHOST="localhost"
KEEP_COUNT=500000

# 基准库（以此库为基准进行随机清理）
BASE_DB="DataWarehouse"

# 其他库（清理掉基准库中已删除的对应订单，空格分隔）
# 示例: OTHER_DBS="DataWarehouse_test DataWarehouse_dev"
OTHER_DBS=""

# 日志
LOG_DIR="/var/log"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/clean_orders_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "========================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 订单清理开始"
echo "========================================="

# =============================================================================
# 工具函数
# =============================================================================

# 执行 SQL 并打印描述
run_sql() {
    local db=$1 sql=$2 desc=$3
    echo "[$(date '+%H:%M:%S')] $desc..."
    $PSQL -U "$PGUSER" -h "$PGHOST" -d "$db" -c "$sql" 2>&1
    local rc=${PIPESTATUS[0]}
    if [ "$rc" -ne 0 ]; then
        echo "[$(date '+%H:%M:%S')] 错误: $desc 失败 (exit=$rc)"
        exit $rc
    fi
    echo "[$(date '+%H:%M:%S')] $desc 完成"
}

# 静默执行 SQL，返回结果
run_sql_quiet() {
    local db=$1 sql=$2
    $PSQL -U "$PGUSER" -h "$PGHOST" -d "$db" -q -t -c "$sql" 2>/dev/null
}

# =============================================================================
# rebuild_dws: 从 DWD 全量重建 DWS 四张表
# =============================================================================
rebuild_dws() {
    local db=$1
    echo "[$(date '+%H:%M:%S')] --- 重建 $db DWS 层 ---"

    # 清空
    run_sql "$db" "TRUNCATE TABLE dws.dws_order_wide"          "  清空 dws_order_wide"
    run_sql "$db" "TRUNCATE TABLE dws.dws_product_sales_1d"    "  清空 dws_product_sales_1d"
    run_sql "$db" "TRUNCATE TABLE dws.dws_category_sales_1d"   "  清空 dws_category_sales_1d"
    run_sql "$db" "TRUNCATE TABLE dws.dws_user_order_1d"       "  清空 dws_user_order_1d"

    # 1) 订单商品宽表
    run_sql "$db" \
"INSERT INTO dws.dws_order_wide (
    order_id,product_id,product_name,brand,category,sub_category,
    price,quantity,subtotal,order_status,buyer_name,buyer_phone,
    buyer_email,province,city,district,payment_method,payment_amount,
    logistics_company,tracking_number,order_amount,discount,actual_amount,
    order_created_at,dt,load_time
)
SELECT
    oi.order_id, oi.product_id, oi.product_name, oi.brand,
    oi.category, oi.sub_category, oi.price, oi.quantity, oi.subtotal,
    COALESCE(o.order_status,''), COALESCE(o.buyer_name,''),
    COALESCE(o.buyer_phone,''), COALESCE(o.buyer_email,''),
    COALESCE(o.province,''), COALESCE(o.city,''), COALESCE(o.district,''),
    COALESCE(o.payment_method,''), COALESCE(o.payment_amount,0),
    COALESCE(o.logistics_company,''), COALESCE(o.tracking_number,''),
    COALESCE(o.order_amount,0), COALESCE(o.discount,0),
    COALESCE(o.actual_amount,0), COALESCE(o.created_at,NOW()),
    COALESCE(o.created_at,NOW())::date, NOW()
FROM dwd.dwd_order_item oi
LEFT JOIN dwd.dwd_order o ON oi.order_id = o.order_id" \
    "  重建 dws_order_wide"

    # 2) 商品日销售汇总
    run_sql "$db" \
"INSERT INTO dws.dws_product_sales_1d (
    product_id,dt,product_name,category,brand,
    total_quantity,total_amount,order_count,avg_price,load_time
)
SELECT
    oi.product_id,
    COALESCE(o.created_at,NOW())::date,
    MAX(oi.product_name), MAX(oi.category), MAX(oi.brand),
    SUM(oi.quantity), SUM(oi.subtotal),
    COUNT(DISTINCT oi.order_id),
    CASE WHEN SUM(oi.quantity)>0
         THEN SUM(oi.subtotal)/SUM(oi.quantity) ELSE 0 END,
    NOW()
FROM dwd.dwd_order_item oi
LEFT JOIN dwd.dwd_order o ON oi.order_id = o.order_id
GROUP BY oi.product_id, COALESCE(o.created_at,NOW())::date" \
    "  重建 dws_product_sales_1d"

    # 3) 品类日销售汇总
    run_sql "$db" \
"INSERT INTO dws.dws_category_sales_1d (
    category,dt,total_quantity,total_amount,order_count,sku_count,load_time
)
SELECT
    oi.category,
    COALESCE(o.created_at,NOW())::date,
    SUM(oi.quantity), SUM(oi.subtotal),
    COUNT(DISTINCT oi.order_id), COUNT(DISTINCT oi.product_id),
    NOW()
FROM dwd.dwd_order_item oi
LEFT JOIN dwd.dwd_order o ON oi.order_id = o.order_id
GROUP BY oi.category, COALESCE(o.created_at,NOW())::date" \
    "  重建 dws_category_sales_1d"

    # 4) 用户日行为汇总
    run_sql "$db" \
"INSERT INTO dws.dws_user_order_1d (
    buyer_phone,dt,buyer_name,order_count,total_amount,total_quantity,
    completed_count,cancelled_count,pending_count,load_time
)
SELECT
    COALESCE(o.buyer_phone,'unknown'),
    COALESCE(o.created_at,NOW())::date,
    MAX(COALESCE(o.buyer_name,'')),
    COUNT(DISTINCT oi.order_id),
    SUM(COALESCE(o.actual_amount,oi.subtotal,0)),
    SUM(oi.quantity),
    SUM(CASE WHEN o.order_status='已完成' THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.order_status='已取消' THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.order_status IN ('待付款','已付款','已发货') THEN 1 ELSE 0 END),
    NOW()
FROM dwd.dwd_order_item oi
LEFT JOIN dwd.dwd_order o ON oi.order_id = o.order_id
WHERE COALESCE(o.buyer_phone,'') != ''
GROUP BY COALESCE(o.buyer_phone,'unknown'), COALESCE(o.created_at,NOW())::date" \
    "  重建 dws_user_order_1d"
}

# =============================================================================
# rebuild_ads: 从 DWS 全量重建 ADS 五张表
# =============================================================================
rebuild_ads() {
    local db=$1
    echo "[$(date '+%H:%M:%S')] --- 重建 $db ADS 层 ---"

    # 清空
    run_sql "$db" "TRUNCATE TABLE ads.ads_category_revenue"  "  清空 ads_category_revenue"
    run_sql "$db" "TRUNCATE TABLE ads.ads_daily_sales_trend" "  清空 ads_daily_sales_trend"
    run_sql "$db" "TRUNCATE TABLE ads.ads_user_value"        "  清空 ads_user_value"
    run_sql "$db" "TRUNCATE TABLE ads.ads_product_ranking"   "  清空 ads_product_ranking"
    run_sql "$db" "TRUNCATE TABLE ads.ads_regional_sales"    "  清空 ads_regional_sales"

    # 1) 品类营收排行
    run_sql "$db" \
"INSERT INTO ads.ads_category_revenue (
    category,total_amount,total_quantity,order_count,sku_count,
    percentage,rank,load_time
)
SELECT
    category,
    SUM(total_amount), SUM(total_quantity), SUM(order_count), SUM(sku_count),
    ROUND(SUM(total_amount)*100.0/NULLIF(SUM(SUM(total_amount)) OVER(),0),2),
    RANK() OVER (ORDER BY SUM(total_amount) DESC),
    NOW()
FROM dws.dws_category_sales_1d
GROUP BY category" \
    "  重建 ads_category_revenue"

    # 2) 每日销售趋势
    run_sql "$db" \
"INSERT INTO ads.ads_daily_sales_trend (
    dt,total_sales_amount,total_order_count,avg_order_amount,load_time
)
SELECT
    dt, SUM(total_amount), SUM(order_count),
    ROUND(SUM(total_amount)*1.0/NULLIF(SUM(order_count),0),2),
    NOW()
FROM dws.dws_category_sales_1d
GROUP BY dt" \
    "  重建 ads_daily_sales_trend"

    # 3) 用户价值分析 (RFM)
    run_sql "$db" \
"INSERT INTO ads.ads_user_value (
    buyer_phone,buyer_name,last_order_date,order_frequency,
    total_monetary,avg_order_amount,value_tier,load_time
)
SELECT
    buyer_phone, MAX(buyer_name), MAX(dt),
    SUM(order_count), SUM(total_amount),
    ROUND(SUM(total_amount)*1.0/NULLIF(SUM(order_count),0),2),
    CASE WHEN SUM(total_amount)>=10000 THEN '高价值'
         WHEN SUM(total_amount)>=1000  THEN '中价值'
         ELSE '低价值' END,
    NOW()
FROM dws.dws_user_order_1d
GROUP BY buyer_phone" \
    "  重建 ads_user_value"

    # 4) 商品热销排行
    run_sql "$db" \
"INSERT INTO ads.ads_product_ranking (
    product_id,product_name,category,brand,
    total_quantity,total_amount,rank,load_time
)
SELECT
    product_id, MAX(product_name), MAX(category), MAX(brand),
    SUM(total_quantity), SUM(total_amount),
    RANK() OVER (ORDER BY SUM(total_quantity) DESC),
    NOW()
FROM dws.dws_product_sales_1d
GROUP BY product_id" \
    "  重建 ads_product_ranking"

    # 5) 区域销售分析 — 省级
    run_sql "$db" \
"INSERT INTO ads.ads_regional_sales (
    province,city,district,total_orders,total_amount,
    avg_order_amount,buyer_count,load_time
)
SELECT
    province,'合计','合计',
    COUNT(DISTINCT order_id), SUM(actual_amount),
    ROUND(SUM(actual_amount)*1.0/NULLIF(COUNT(DISTINCT order_id),0),2),
    COUNT(DISTINCT buyer_phone),
    NOW()
FROM dws.dws_order_wide
WHERE province != ''
GROUP BY province" \
    "  重建 ads_regional_sales (省级)"

    # 6) 区域销售分析 — 市级
    run_sql "$db" \
"INSERT INTO ads.ads_regional_sales (
    province,city,district,total_orders,total_amount,
    avg_order_amount,buyer_count,load_time
)
SELECT
    province,city,'合计',
    COUNT(DISTINCT order_id)::bigint, SUM(actual_amount),
    ROUND(SUM(actual_amount)*1.0/NULLIF(COUNT(DISTINCT order_id),0),2),
    COUNT(DISTINCT buyer_phone)::bigint,
    NOW()
FROM dws.dws_order_wide
WHERE city != ''
GROUP BY province,city" \
    "  重建 ads_regional_sales (市级)"
}

# =============================================================================
# clean_one_db: 清理单个数据库的 DWD 订单，然后重建 DWS+ADS
# =============================================================================
clean_one_db() {
    local db=$1
    local del_ids_table=$2   # 已有删除列表的表名（如 public._cleanup_deleted_orders）
    local is_base=$3         # "true" 表示基准库（需要随机选择），"false" 表示其他库

    echo ""
    echo "========== 处理数据库: $db (基准库=$is_base) =========="

    # 当前订单总数
    local total=$(run_sql_quiet "$db" "SELECT COUNT(*) FROM dwd.dwd_order;")
    echo "[$(date '+%H:%M:%S')] $db 当前订单总数: $total"

    if [ "$is_base" = "true" ]; then
        # ---- 基准库: 随机选择要保留的订单 ----
        if [ "$total" -le "$KEEP_COUNT" ]; then
            echo "[$(date '+%H:%M:%S')] $db 订单数 $total <= $KEEP_COUNT，无需清理"
            return
        fi

        echo "[$(date '+%H:%M:%S')] $db 随机保留 $KEEP_COUNT 条订单..."

        $PSQL -U "$PGUSER" -h "$PGHOST" -d "$db" <<EOSQL
BEGIN;

-- 要保留的订单
CREATE TEMP TABLE _keep_orders AS
SELECT order_id FROM dwd.dwd_order
ORDER BY RANDOM()
LIMIT $KEEP_COUNT;

-- 要删除的订单
CREATE TEMP TABLE _del_orders AS
SELECT order_id FROM dwd.dwd_order
WHERE order_id NOT IN (SELECT order_id FROM _keep_orders);

-- 持久化删除列表（供其他库使用）
DROP TABLE IF EXISTS public._cleanup_deleted_orders;
CREATE TABLE public._cleanup_deleted_orders AS
SELECT order_id FROM _del_orders;
CREATE INDEX ON public._cleanup_deleted_orders(order_id);

COMMIT;
EOSQL

        local del_count=$(run_sql_quiet "$db" "SELECT COUNT(*) FROM public._cleanup_deleted_orders;")
        echo "[$(date '+%H:%M:%S')] $db 将删除 $del_count 条订单 (保留 $KEEP_COUNT 条)"
        DEL_IDS_TABLE="public._cleanup_deleted_orders"
    else
        # ---- 其他库: 使用已有的删除列表 ----
        local del_count=$(run_sql_quiet "$db" "SELECT COUNT(*) FROM $del_ids_table;")
        echo "[$(date '+%H:%M:%S')] $db 将删除 $del_count 条订单（与基准库保持一致）"
        DEL_IDS_TABLE="$del_ids_table"
    fi

    # ---- 删除 DWD 订单数据 ----
    run_sql "$db" \
"DELETE FROM dwd.dwd_order_item
WHERE order_id IN (SELECT order_id FROM $DEL_IDS_TABLE)" \
    "  删除 dwd_order_item"

    run_sql "$db" \
"DELETE FROM dwd.dwd_order
WHERE order_id IN (SELECT order_id FROM $DEL_IDS_TABLE)" \
    "  删除 dwd_order"

    local remain=$(run_sql_quiet "$db" "SELECT COUNT(*) FROM dwd.dwd_order;")
    echo "[$(date '+%H:%M:%S')] $db 剩余订单: $remain"

    # ---- 重建 DWS + ADS ----
    rebuild_dws "$db"
    rebuild_ads "$db"

    echo "[$(date '+%H:%M:%S')] $db 处理完成"
}

# =============================================================================
# 主流程
# =============================================================================

# 阶段1: 处理基准库（随机选择 + 删除 + 重建）
clean_one_db "$BASE_DB" "" "true"

# 阶段2: 处理其他库（用基准库的删除列表）
if [ -n "$OTHER_DBS" ]; then
    # 导出删除列表到文件（跨库传递）
    DEL_IDS_FILE="/tmp/_cleanup_order_ids_$$.txt"
    run_sql_quiet "$BASE_DB" "\COPY (SELECT order_id FROM public._cleanup_deleted_orders) TO '$DEL_IDS_FILE'"
    echo ""
    echo "[$(date '+%H:%M:%S')] 导出 $(wc -l < "$DEL_IDS_FILE") 个待删除订单ID"

    for OTHER_DB in $OTHER_DBS; do
        # 将删除列表导入到其他库
        run_sql "$OTHER_DB" \
"DROP TABLE IF EXISTS public._cleanup_deleted_orders;
CREATE TABLE public._cleanup_deleted_orders (order_id VARCHAR(32) PRIMARY KEY);" \
        "  创建删除列表表"

        run_sql "$OTHER_DB" \
"\COPY public._cleanup_deleted_orders FROM '$DEL_IDS_FILE'" \
        "  导入删除列表"

        clean_one_db "$OTHER_DB" "public._cleanup_deleted_orders" "false"

        # 清理其他库的临时表
        run_sql "$OTHER_DB" "DROP TABLE IF EXISTS public._cleanup_deleted_orders" "  清理临时表"
    done

    rm -f "$DEL_IDS_FILE"
else
    echo ""
    echo "[$(date '+%H:%M:%S')] 无其他库需要处理"
fi

# 清理基准库临时表
run_sql "$BASE_DB" "DROP TABLE IF EXISTS public._cleanup_deleted_orders" "清理基准库临时表"

# =============================================================================
# 完成
# =============================================================================
echo ""
echo "========================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 订单清理完成!"
echo "  基准库 [$BASE_DB]: $(run_sql_quiet "$BASE_DB" "SELECT COUNT(*) FROM dwd.dwd_order;") 条订单"
for OTHER_DB in $OTHER_DBS; do
    echo "  其他库 [$OTHER_DB]: $(run_sql_quiet "$OTHER_DB" "SELECT COUNT(*) FROM dwd.dwd_order;") 条订单"
done
echo "  日志: $LOG_FILE"
echo "========================================="
