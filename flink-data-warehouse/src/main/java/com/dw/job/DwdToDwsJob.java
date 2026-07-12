package com.dw.job;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * DWD → DWS 汇总作业
 *
 * 从 DWD 表读取数据，构建：
 * 1. dws_order_wide   - 订单商品宽表
 * 2. dws_product_sales_1d  - 商品日销售汇总
 * 3. dws_category_sales_1d - 品类日销售汇总
 * 4. dws_user_order_1d     - 用户日行为汇总
 */
public class DwdToDwsJob {

    // 从 classpath config.properties 加载
    private static Properties cfg = new Properties();

    public static void main(String[] args) throws Exception {
        try (InputStream is = DwdToDwsJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) { System.err.println("config.properties not found"); System.exit(1); return; }
            cfg.load(is);
        }
        String dbUrl  = cfg.getProperty("db.jdbc.url");
        String dbUser = cfg.getProperty("db.user");
        String dbPass = cfg.getProperty("db.password");

        try (Connection c = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
            c.setAutoCommit(false);
            System.out.println("DwdToDwsJob: connected to " + dbUrl);

            buildOrderWide(c);
            buildProductSales(c);
            buildCategorySales(c);
            buildUserOrder(c);

            c.commit();
            System.out.println("DwdToDwsJob done.");
        }
    }

    // ============ 1. 订单商品宽表 ============
    private static void buildOrderWide(Connection c) throws Exception {
        // 从 dwd_order_item 获取已有数据（订单快照不需要额外 JOIN dwd_order）
        String sql = "INSERT INTO dws.dws_order_wide "
                + "(order_id,product_id,product_name,brand,category,sub_category,price,quantity,subtotal,order_status,buyer_name,buyer_phone,buyer_email,province,city,district,payment_method,payment_amount,logistics_company,tracking_number,order_amount,discount,actual_amount,order_created_at,dt,load_time) "
                + "SELECT oi.order_id, oi.product_id, oi.product_name, oi.brand, oi.category, oi.sub_category, "
                + "oi.price, oi.quantity, oi.subtotal, "
                + "COALESCE(o.order_status,''), COALESCE(o.buyer_name,''), COALESCE(o.buyer_phone,''), COALESCE(o.buyer_email,''), "
                + "COALESCE(o.province,''), COALESCE(o.city,''), COALESCE(o.district,''), "
                + "COALESCE(o.payment_method,''), COALESCE(o.payment_amount,0), "
                + "COALESCE(o.logistics_company,''), COALESCE(o.tracking_number,''), "
                + "COALESCE(o.order_amount,0), COALESCE(o.discount,0), COALESCE(o.actual_amount,0), "
                + "COALESCE(o.created_at,NOW()), COALESCE(o.created_at,NOW())::date, NOW() "
                + "FROM dwd.dwd_order_item oi "
                + "LEFT JOIN dwd.dwd_order o ON oi.order_id = o.order_id "
                + "ON CONFLICT (order_id, product_id) DO NOTHING";
        Statement st = c.createStatement();
        int rows = st.executeUpdate(sql);
        st.close();
        c.commit();
        System.out.println("dws_order_wide: " + rows + " rows");
    }

    // ============ 2. 商品日销售汇总 ============
    private static void buildProductSales(Connection c) throws Exception {
        String sql = "INSERT INTO dws.dws_product_sales_1d (product_id, dt, product_name, category, brand, total_quantity, total_amount, order_count, avg_price, load_time) "
                + "SELECT oi.product_id, COALESCE(o.created_at, NOW())::date AS dt, "
                + "MAX(oi.product_name) AS product_name, MAX(oi.category) AS category, MAX(oi.brand) AS brand, "
                + "SUM(oi.quantity) AS total_quantity, SUM(oi.subtotal) AS total_amount, "
                + "COUNT(DISTINCT oi.order_id) AS order_count, "
                + "CASE WHEN SUM(oi.quantity)>0 THEN SUM(oi.subtotal)/SUM(oi.quantity) ELSE 0 END AS avg_price, "
                + "NOW() "
                + "FROM dwd.dwd_order_item oi "
                + "LEFT JOIN dwd.dwd_order o ON oi.order_id = o.order_id "
                + "GROUP BY oi.product_id, COALESCE(o.created_at, NOW())::date "
                + "ON CONFLICT (product_id, dt) DO UPDATE SET "
                + "total_quantity=EXCLUDED.total_quantity, total_amount=EXCLUDED.total_amount, "
                + "order_count=EXCLUDED.order_count, avg_price=EXCLUDED.avg_price, load_time=NOW()";
        Statement st = c.createStatement();
        int rows = st.executeUpdate(sql);
        st.close();
        c.commit();
        System.out.println("dws_product_sales_1d: " + rows + " rows");
    }

    // ============ 3. 品类日销售汇总 ============
    private static void buildCategorySales(Connection c) throws Exception {
        String sql = "INSERT INTO dws.dws_category_sales_1d (category, dt, total_quantity, total_amount, order_count, sku_count, load_time) "
                + "SELECT oi.category, COALESCE(o.created_at, NOW())::date AS dt, "
                + "SUM(oi.quantity) AS total_quantity, SUM(oi.subtotal) AS total_amount, "
                + "COUNT(DISTINCT oi.order_id) AS order_count, "
                + "COUNT(DISTINCT oi.product_id) AS sku_count, "
                + "NOW() "
                + "FROM dwd.dwd_order_item oi "
                + "LEFT JOIN dwd.dwd_order o ON oi.order_id = o.order_id "
                + "GROUP BY oi.category, COALESCE(o.created_at, NOW())::date "
                + "ON CONFLICT (category, dt) DO UPDATE SET "
                + "total_quantity=EXCLUDED.total_quantity, total_amount=EXCLUDED.total_amount, "
                + "order_count=EXCLUDED.order_count, sku_count=EXCLUDED.sku_count, load_time=NOW()";
        Statement st = c.createStatement();
        int rows = st.executeUpdate(sql);
        st.close();
        c.commit();
        System.out.println("dws_category_sales_1d: " + rows + " rows");
    }

    // ============ 4. 用户日行为汇总 ============
    private static void buildUserOrder(Connection c) throws Exception {
        // 需要 dwd_order 数据（用户信息在订单主表）。如果 dwd_order 为空则从 order_item 推断
        String sql = "INSERT INTO dws.dws_user_order_1d (buyer_phone, dt, buyer_name, order_count, total_amount, total_quantity, completed_count, cancelled_count, pending_count, load_time) "
                + "SELECT COALESCE(o.buyer_phone, 'unknown') AS buyer_phone, "
                + "COALESCE(o.created_at, NOW())::date AS dt, "
                + "MAX(COALESCE(o.buyer_name, '')) AS buyer_name, "
                + "COUNT(DISTINCT oi.order_id) AS order_count, "
                + "SUM(COALESCE(o.actual_amount, oi.subtotal, 0)) AS total_amount, "
                + "SUM(oi.quantity) AS total_quantity, "
                + "SUM(CASE WHEN o.order_status='已完成' THEN 1 ELSE 0 END) AS completed_count, "
                + "SUM(CASE WHEN o.order_status='已取消' THEN 1 ELSE 0 END) AS cancelled_count, "
                + "SUM(CASE WHEN o.order_status IN ('待付款','已付款','已发货') THEN 1 ELSE 0 END) AS pending_count, "
                + "NOW() "
                + "FROM dwd.dwd_order_item oi "
                + "LEFT JOIN dwd.dwd_order o ON oi.order_id = o.order_id "
                + "WHERE COALESCE(o.buyer_phone,'') != '' "
                + "GROUP BY COALESCE(o.buyer_phone, 'unknown'), COALESCE(o.created_at, NOW())::date "
                + "ON CONFLICT (buyer_phone, dt) DO UPDATE SET "
                + "order_count=EXCLUDED.order_count, total_amount=EXCLUDED.total_amount, "
                + "total_quantity=EXCLUDED.total_quantity, completed_count=EXCLUDED.completed_count, "
                + "cancelled_count=EXCLUDED.cancelled_count, pending_count=EXCLUDED.pending_count, load_time=NOW()";
        Statement st = c.createStatement();
        int rows = st.executeUpdate(sql);
        st.close();
        c.commit();
        System.out.println("dws_user_order_1d: " + rows + " rows");
    }
}
