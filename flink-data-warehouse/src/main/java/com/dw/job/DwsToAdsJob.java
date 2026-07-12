package com.dw.job;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * DWS → ADS 应用指标作业
 *
 * 从 DWS 汇总表读取数据，计算：
 * 1. ads_category_revenue   - 品类营收排行（含占比、排名）
 * 2. ads_daily_sales_trend   - 每日销售趋势
 * 3. ads_user_value          - 用户价值分析 (RFM)
 * 4. ads_product_ranking     - 商品热销排行
 * 5. ads_regional_sales      - 区域销售分析
 */
public class DwsToAdsJob {

    private static Properties cfg = new Properties();

    public static void main(String[] args) throws Exception {
        try (InputStream is = DwsToAdsJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) { System.err.println("config.properties not found"); System.exit(1); return; }
            cfg.load(is);
        }
        String dbUrl  = cfg.getProperty("db.jdbc.url");
        String dbUser = cfg.getProperty("db.user");
        String dbPass = cfg.getProperty("db.password");

        try (Connection c = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
            c.setAutoCommit(false);
            System.out.println("DwsToAdsJob: connected to " + dbUrl);

            buildCategoryRevenue(c);
            buildDailySalesTrend(c);
            buildUserValue(c);
            buildProductRanking(c);
            buildRegionalSales(c);

            c.commit();
            System.out.println("DwsToAdsJob done.");
        }
    }

    // ==================== 1. 品类营收排行 ====================
    private static void buildCategoryRevenue(Connection c) throws Exception {
        String sql =
            "INSERT INTO ads.ads_category_revenue (category, total_amount, total_quantity, order_count, sku_count, percentage, rank, load_time) " +
            "SELECT category, " +
            "  SUM(total_amount) AS total_amount, SUM(total_quantity) AS total_quantity, " +
            "  SUM(order_count) AS order_count, SUM(sku_count) AS sku_count, " +
            "  ROUND(SUM(total_amount) * 100.0 / NULLIF(SUM(SUM(total_amount)) OVER(), 0), 2) AS percentage, " +
            "  RANK() OVER (ORDER BY SUM(total_amount) DESC) AS rank, " +
            "  NOW() " +
            "FROM dws.dws_category_sales_1d " +
            "GROUP BY category " +
            "ON CONFLICT (category) DO UPDATE SET " +
            "  total_amount=EXCLUDED.total_amount, total_quantity=EXCLUDED.total_quantity, " +
            "  order_count=EXCLUDED.order_count, sku_count=EXCLUDED.sku_count, " +
            "  percentage=EXCLUDED.percentage, rank=EXCLUDED.rank, load_time=NOW()";
        Statement st = c.createStatement();
        int n = st.executeUpdate(sql);
        st.close();
        c.commit();
        System.out.println("ads_category_revenue: " + n + " rows");
    }

    // ==================== 2. 每日销售趋势 ====================
    private static void buildDailySalesTrend(Connection c) throws Exception {
        String sql =
            "INSERT INTO ads.ads_daily_sales_trend (dt, total_sales_amount, total_order_count, avg_order_amount, load_time) " +
            "SELECT dt, " +
            "  SUM(total_amount) AS total_sales_amount, " +
            "  SUM(order_count) AS total_order_count, " +
            "  ROUND(SUM(total_amount) * 1.0 / NULLIF(SUM(order_count), 0), 2) AS avg_order_amount, " +
            "  NOW() " +
            "FROM dws.dws_category_sales_1d " +
            "GROUP BY dt " +
            "ON CONFLICT (dt) DO UPDATE SET " +
            "  total_sales_amount=EXCLUDED.total_sales_amount, total_order_count=EXCLUDED.total_order_count, " +
            "  avg_order_amount=EXCLUDED.avg_order_amount, load_time=NOW()";
        Statement st = c.createStatement();
        int n = st.executeUpdate(sql);
        st.close();
        c.commit();
        System.out.println("ads_daily_sales_trend: " + n + " rows");
    }

    // ==================== 3. 用户价值分析 (RFM) ====================
    private static void buildUserValue(Connection c) throws Exception {
        String sql =
            "INSERT INTO ads.ads_user_value (buyer_phone, buyer_name, last_order_date, order_frequency, total_monetary, avg_order_amount, value_tier, load_time) " +
            "SELECT buyer_phone, " +
            "  MAX(buyer_name) AS buyer_name, " +
            "  MAX(dt) AS last_order_date, " +
            "  SUM(order_count) AS order_frequency, " +
            "  SUM(total_amount) AS total_monetary, " +
            "  ROUND(SUM(total_amount) * 1.0 / NULLIF(SUM(order_count), 0), 2) AS avg_order_amount, " +
            "  CASE " +
            "    WHEN SUM(total_amount) >= 10000 THEN '高价值' " +
            "    WHEN SUM(total_amount) >= 1000  THEN '中价值' " +
            "    ELSE '低价值' " +
            "  END AS value_tier, " +
            "  NOW() " +
            "FROM dws.dws_user_order_1d " +
            "GROUP BY buyer_phone " +
            "ON CONFLICT (buyer_phone) DO UPDATE SET " +
            "  last_order_date=EXCLUDED.last_order_date, order_frequency=EXCLUDED.order_frequency, " +
            "  total_monetary=EXCLUDED.total_monetary, avg_order_amount=EXCLUDED.avg_order_amount, " +
            "  value_tier=EXCLUDED.value_tier, load_time=NOW()";
        Statement st = c.createStatement();
        int n = st.executeUpdate(sql);
        st.close();
        c.commit();
        System.out.println("ads_user_value: " + n + " rows");
    }

    // ==================== 4. 商品热销排行 ====================
    private static void buildProductRanking(Connection c) throws Exception {
        String sql =
            "INSERT INTO ads.ads_product_ranking (product_id, product_name, category, brand, total_quantity, total_amount, rank, load_time) " +
            "SELECT product_id, " +
            "  MAX(product_name) AS product_name, " +
            "  MAX(category) AS category, " +
            "  MAX(brand) AS brand, " +
            "  SUM(total_quantity) AS total_quantity, " +
            "  SUM(total_amount) AS total_amount, " +
            "  RANK() OVER (ORDER BY SUM(total_quantity) DESC) AS rank, " +
            "  NOW() " +
            "FROM dws.dws_product_sales_1d " +
            "GROUP BY product_id " +
            "ON CONFLICT (product_id) DO UPDATE SET " +
            "  total_quantity=EXCLUDED.total_quantity, total_amount=EXCLUDED.total_amount, " +
            "  rank=EXCLUDED.rank, load_time=NOW()";
        Statement st = c.createStatement();
        int n = st.executeUpdate(sql);
        st.close();
        c.commit();
        System.out.println("ads_product_ranking: " + n + " rows");
    }

    // ==================== 5. 区域销售分析 ====================
    private static void buildRegionalSales(Connection c) throws Exception {
        // 省份级汇总
        String sql =
            "INSERT INTO ads.ads_regional_sales (province, city, district, total_orders, total_amount, avg_order_amount, buyer_count, load_time) " +
            "SELECT province, '合计', '合计', " +
            "  COUNT(DISTINCT order_id) AS total_orders, " +
            "  SUM(actual_amount) AS total_amount, " +
            "  ROUND(SUM(actual_amount) * 1.0 / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_order_amount, " +
            "  COUNT(DISTINCT buyer_phone) AS buyer_count, " +
            "  NOW() " +
            "FROM dws.dws_order_wide " +
            "WHERE province != '' " +
            "GROUP BY province " +
            "ON CONFLICT (province, city, district) DO UPDATE SET " +
            "  total_orders=EXCLUDED.total_orders, total_amount=EXCLUDED.total_amount, " +
            "  avg_order_amount=EXCLUDED.avg_order_amount, buyer_count=EXCLUDED.buyer_count, load_time=NOW()";
        Statement st = c.createStatement();
        int n = st.executeUpdate(sql);
        st.close();
        c.commit();
        System.out.println("ads_regional_sales: " + n + " rows");

        // 城市级汇总
        String sqlCity =
            "INSERT INTO ads.ads_regional_sales (province, city, district, total_orders, total_amount, avg_order_amount, buyer_count, load_time) " +
            "SELECT province, city, '合计', " +
            "  COUNT(DISTINCT order_id), SUM(actual_amount), " +
            "  ROUND(SUM(actual_amount) * 1.0 / NULLIF(COUNT(DISTINCT order_id), 0), 2), " +
            "  COUNT(DISTINCT buyer_phone), NOW() " +
            "FROM dws.dws_order_wide WHERE city != '' " +
            "GROUP BY province, city " +
            "ON CONFLICT (province, city, district) DO UPDATE SET " +
            "  total_orders=EXCLUDED.total_orders, total_amount=EXCLUDED.total_amount, " +
            "  avg_order_amount=EXCLUDED.avg_order_amount, buyer_count=EXCLUDED.buyer_count, load_time=NOW()";
        st = c.createStatement();
        n = st.executeUpdate(sqlCity);
        st.close();
        c.commit();
        System.out.println("ads_regional_sales (city): " + n + " rows");
    }
}
