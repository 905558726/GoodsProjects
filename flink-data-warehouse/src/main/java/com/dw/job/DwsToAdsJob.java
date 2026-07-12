package com.dw.job;

import java.io.*;
import java.sql.*;
import java.util.Properties;

/**
 * DWS → ADS 定时轮询指标计算 — Flink 常驻 Job
 *
 * 每 30 秒从 DWS 全量刷新 ADS 五张指标表。
 * UPSERT 语义保证幂等。
 *
 * flink run -d -c com.dw.job.DwsToAdsJob flink-data-warehouse.jar
 */
public class DwsToAdsJob {

    private static Properties cfg = new Properties();

    public static void main(String[] args) {
        try (InputStream is = DwsToAdsJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) { System.err.println("config.properties not found"); System.exit(1); return; }
            cfg.load(is);
        } catch (IOException e) { e.printStackTrace(); System.exit(1); }

        String dbUrl  = cfg.getProperty("db.jdbc.url");
        String dbUser = cfg.getProperty("db.user");
        String dbPass = cfg.getProperty("db.password");

        System.out.println("[DwsToAdsJob] STARTED db=" + dbUrl);

        while (true) {
            try (Connection c = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
                c.setAutoCommit(false);
                exec(c, "INSERT INTO ads.ads_category_revenue (category,total_amount,total_quantity,order_count,sku_count,percentage,rank,load_time) SELECT category,SUM(total_amount),SUM(total_quantity),SUM(order_count),SUM(sku_count),ROUND(SUM(total_amount)*100.0/NULLIF(SUM(SUM(total_amount)) OVER(),0),2),RANK() OVER (ORDER BY SUM(total_amount) DESC),NOW() FROM dws.dws_category_sales_1d GROUP BY category ON CONFLICT (category) DO UPDATE SET total_amount=EXCLUDED.total_amount,total_quantity=EXCLUDED.total_quantity,order_count=EXCLUDED.order_count,sku_count=EXCLUDED.sku_count,percentage=EXCLUDED.percentage,rank=EXCLUDED.rank,load_time=NOW()");
                exec(c, "INSERT INTO ads.ads_daily_sales_trend (dt,total_sales_amount,total_order_count,avg_order_amount,load_time) SELECT dt,SUM(total_amount),SUM(order_count),ROUND(SUM(total_amount)*1.0/NULLIF(SUM(order_count),0),2),NOW() FROM dws.dws_category_sales_1d GROUP BY dt ON CONFLICT (dt) DO UPDATE SET total_sales_amount=EXCLUDED.total_sales_amount,total_order_count=EXCLUDED.total_order_count,avg_order_amount=EXCLUDED.avg_order_amount,load_time=NOW()");
                exec(c, "INSERT INTO ads.ads_user_value (buyer_phone,buyer_name,last_order_date,order_frequency,total_monetary,avg_order_amount,value_tier,load_time) SELECT buyer_phone,MAX(buyer_name),MAX(dt),SUM(order_count),SUM(total_amount),ROUND(SUM(total_amount)*1.0/NULLIF(SUM(order_count),0),2),CASE WHEN SUM(total_amount)>=10000 THEN '高价值' WHEN SUM(total_amount)>=1000 THEN '中价值' ELSE '低价值' END,NOW() FROM dws.dws_user_order_1d GROUP BY buyer_phone ON CONFLICT (buyer_phone) DO UPDATE SET last_order_date=EXCLUDED.last_order_date,order_frequency=EXCLUDED.order_frequency,total_monetary=EXCLUDED.total_monetary,avg_order_amount=EXCLUDED.avg_order_amount,value_tier=EXCLUDED.value_tier,load_time=NOW()");
                exec(c, "INSERT INTO ads.ads_product_ranking (product_id,product_name,category,brand,total_quantity,total_amount,rank,load_time) SELECT product_id,MAX(product_name),MAX(category),MAX(brand),SUM(total_quantity),SUM(total_amount),RANK() OVER (ORDER BY SUM(total_quantity) DESC),NOW() FROM dws.dws_product_sales_1d GROUP BY product_id ON CONFLICT (product_id) DO UPDATE SET total_quantity=EXCLUDED.total_quantity,total_amount=EXCLUDED.total_amount,rank=EXCLUDED.rank,load_time=NOW()");
                exec(c, "INSERT INTO ads.ads_regional_sales (province,city,district,total_orders,total_amount,avg_order_amount,buyer_count,load_time) SELECT province,'合计','合计',COUNT(DISTINCT order_id),SUM(actual_amount),ROUND(SUM(actual_amount)*1.0/NULLIF(COUNT(DISTINCT order_id),0),2),COUNT(DISTINCT buyer_phone),NOW() FROM dws.dws_order_wide WHERE province!='' GROUP BY province ON CONFLICT (province,city,district) DO UPDATE SET total_orders=EXCLUDED.total_orders,total_amount=EXCLUDED.total_amount,avg_order_amount=EXCLUDED.avg_order_amount,buyer_count=EXCLUDED.buyer_count,load_time=NOW()");
                c.commit();
                System.out.println("[DwsToAdsJob] cycle done");
            } catch (Exception e) { e.printStackTrace(); }
            try { Thread.sleep(30000); } catch (InterruptedException e) { break; }
        }
    }

    private static void exec(Connection c, String sql) throws SQLException {
        Statement st = c.createStatement(); st.executeUpdate(sql); st.close();
    }
}
