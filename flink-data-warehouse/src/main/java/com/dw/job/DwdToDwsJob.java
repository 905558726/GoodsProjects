package com.dw.job;

import java.io.*;
import java.sql.*;
import java.util.Properties;

/**
 * DWD → DWS 定时轮询汇总 — Flink 常驻 Job
 *
 * 每 30 秒查询 DWD 增量数据，全量刷新 DWS 四张汇总表。
 * UPSERT 语义保证幂等。
 *
 * flink run -d -c com.dw.job.DwdToDwsJob flink-data-warehouse.jar
 */
public class DwdToDwsJob {

    private static Properties cfg = new Properties();

    public static void main(String[] args) {
        try (InputStream is = DwdToDwsJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) { System.err.println("config.properties not found"); System.exit(1); return; }
            cfg.load(is);
        } catch (IOException e) { e.printStackTrace(); System.exit(1); }

        String dbUrl  = cfg.getProperty("db.jdbc.url");
        String dbUser = cfg.getProperty("db.user");
        String dbPass = cfg.getProperty("db.password");

        System.out.println("[DwdToDwsJob] STARTED db=" + dbUrl);

        while (true) {
            try (Connection c = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
                c.setAutoCommit(false);
                exec(c, "INSERT INTO dws.dws_order_wide (order_id,product_id,product_name,brand,category,sub_category,price,quantity,subtotal,order_status,buyer_name,buyer_phone,buyer_email,province,city,district,payment_method,payment_amount,logistics_company,tracking_number,order_amount,discount,actual_amount,order_created_at,dt,load_time) SELECT oi.order_id,oi.product_id,oi.product_name,oi.brand,oi.category,oi.sub_category,oi.price,oi.quantity,oi.subtotal,COALESCE(o.order_status,''),COALESCE(o.buyer_name,''),COALESCE(o.buyer_phone,''),COALESCE(o.buyer_email,''),COALESCE(o.province,''),COALESCE(o.city,''),COALESCE(o.district,''),COALESCE(o.payment_method,''),COALESCE(o.payment_amount,0),COALESCE(o.logistics_company,''),COALESCE(o.tracking_number,''),COALESCE(o.order_amount,0),COALESCE(o.discount,0),COALESCE(o.actual_amount,0),COALESCE(o.created_at,NOW()),COALESCE(o.created_at,NOW())::date,NOW() FROM dwd.dwd_order_item oi LEFT JOIN dwd.dwd_order o ON oi.order_id=o.order_id ON CONFLICT (order_id,product_id) DO NOTHING");
                exec(c, "INSERT INTO dws.dws_product_sales_1d (product_id,dt,product_name,category,brand,total_quantity,total_amount,order_count,avg_price,load_time) SELECT oi.product_id,COALESCE(o.created_at,NOW())::date,MAX(oi.product_name),MAX(oi.category),MAX(oi.brand),SUM(oi.quantity),SUM(oi.subtotal),COUNT(DISTINCT oi.order_id),CASE WHEN SUM(oi.quantity)>0 THEN SUM(oi.subtotal)/SUM(oi.quantity) ELSE 0 END,NOW() FROM dwd.dwd_order_item oi LEFT JOIN dwd.dwd_order o ON oi.order_id=o.order_id GROUP BY oi.product_id,COALESCE(o.created_at,NOW())::date ON CONFLICT (product_id,dt) DO UPDATE SET total_quantity=EXCLUDED.total_quantity,total_amount=EXCLUDED.total_amount,order_count=EXCLUDED.order_count,avg_price=EXCLUDED.avg_price,load_time=NOW()");
                exec(c, "INSERT INTO dws.dws_category_sales_1d (category,dt,total_quantity,total_amount,order_count,sku_count,load_time) SELECT oi.category,COALESCE(o.created_at,NOW())::date,SUM(oi.quantity),SUM(oi.subtotal),COUNT(DISTINCT oi.order_id),COUNT(DISTINCT oi.product_id),NOW() FROM dwd.dwd_order_item oi LEFT JOIN dwd.dwd_order o ON oi.order_id=o.order_id GROUP BY oi.category,COALESCE(o.created_at,NOW())::date ON CONFLICT (category,dt) DO UPDATE SET total_quantity=EXCLUDED.total_quantity,total_amount=EXCLUDED.total_amount,order_count=EXCLUDED.order_count,sku_count=EXCLUDED.sku_count,load_time=NOW()");
                exec(c, "INSERT INTO dws.dws_user_order_1d (buyer_phone,dt,buyer_name,order_count,total_amount,total_quantity,completed_count,cancelled_count,pending_count,load_time) SELECT COALESCE(o.buyer_phone,'unknown'),COALESCE(o.created_at,NOW())::date,MAX(COALESCE(o.buyer_name,'')),COUNT(DISTINCT oi.order_id),SUM(COALESCE(o.actual_amount,oi.subtotal,0)),SUM(oi.quantity),SUM(CASE WHEN o.order_status='已完成' THEN 1 ELSE 0 END),SUM(CASE WHEN o.order_status='已取消' THEN 1 ELSE 0 END),SUM(CASE WHEN o.order_status IN ('待付款','已付款','已发货') THEN 1 ELSE 0 END),NOW() FROM dwd.dwd_order_item oi LEFT JOIN dwd.dwd_order o ON oi.order_id=o.order_id WHERE COALESCE(o.buyer_phone,'')!='' GROUP BY COALESCE(o.buyer_phone,'unknown'),COALESCE(o.created_at,NOW())::date ON CONFLICT (buyer_phone,dt) DO UPDATE SET order_count=EXCLUDED.order_count,total_amount=EXCLUDED.total_amount,total_quantity=EXCLUDED.total_quantity,completed_count=EXCLUDED.completed_count,cancelled_count=EXCLUDED.cancelled_count,pending_count=EXCLUDED.pending_count,load_time=NOW()");
                c.commit();
                System.out.println("[DwdToDwsJob] cycle done");
            } catch (Exception e) { e.printStackTrace(); }
            try { Thread.sleep(30000); } catch (InterruptedException e) { break; }
        }
    }

    private static void exec(Connection c, String sql) throws SQLException {
        Statement st = c.createStatement(); st.executeUpdate(sql); st.close();
    }
}
