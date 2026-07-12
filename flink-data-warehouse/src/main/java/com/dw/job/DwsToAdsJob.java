package com.dw.job;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

/**
 * DWS → ADS 定时轮询指标计算 — Flink DataStream Job
 *
 * 每 30s 从 DWS 全量刷新 ADS 五表，可部署到 Flink 集群。
 *
 * flink run -d -c com.dw.job.DwsToAdsJob flink-data-warehouse.jar
 */
public class DwsToAdsJob {

    public static void main(String[] args) throws Exception {
        Properties cfg = loadConfig();
        String dbUrl  = cfg.getProperty("db.jdbc.url");
        String dbUser = cfg.getProperty("db.user");
        String dbPass = cfg.getProperty("db.password");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getCheckpointConfig().setCheckpointInterval(60_000); // 60s

        DataStream<Integer> ticks = env.addSource(new DwdToDwsJob.TickingSource(30_000)).name("tick-source").uid("tick-source");
        ticks.addSink(new AdsRefreshSink(dbUrl, dbUser, dbPass)).name("ads-sink").uid("ads-sink");

        System.out.println("[DwsToAdsJob] FLINK JOB STARTED");
        env.execute("DWS-to-ADS Refresh");
    }

    private static Properties loadConfig() throws IOException {
        Properties cfg = new Properties();
        try (InputStream is = DwsToAdsJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) throw new RuntimeException("config.properties not found");
            cfg.load(is);
        }
        return cfg;
    }

    // ---- ADS Refresh Sink ----
    public static class AdsRefreshSink extends RichSinkFunction<Integer> {
        private final String url, user, pass;
        private transient Connection conn;
        public AdsRefreshSink(String url, String user, String pass) { this.url = url; this.user = user; this.pass = pass; }

        @Override public void open(Configuration p) throws Exception {
            conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);
        }

        @Override public void invoke(Integer tick, Context ctx) throws Exception {
            exec("INSERT INTO ads.ads_category_revenue (category,total_amount,total_quantity,order_count,sku_count,percentage,rank,load_time) SELECT category,SUM(total_amount),SUM(total_quantity),SUM(order_count),SUM(sku_count),ROUND(SUM(total_amount)*100.0/NULLIF(SUM(SUM(total_amount)) OVER(),0),2),RANK() OVER (ORDER BY SUM(total_amount) DESC),NOW() FROM dws.dws_category_sales_1d GROUP BY category ON CONFLICT (category) DO UPDATE SET total_amount=EXCLUDED.total_amount,total_quantity=EXCLUDED.total_quantity,order_count=EXCLUDED.order_count,sku_count=EXCLUDED.sku_count,percentage=EXCLUDED.percentage,rank=EXCLUDED.rank,load_time=NOW()");
            exec("INSERT INTO ads.ads_daily_sales_trend (dt,total_sales_amount,total_order_count,avg_order_amount,load_time) SELECT dt,SUM(total_amount),SUM(order_count),ROUND(SUM(total_amount)*1.0/NULLIF(SUM(order_count),0),2),NOW() FROM dws.dws_category_sales_1d GROUP BY dt ON CONFLICT (dt) DO UPDATE SET total_sales_amount=EXCLUDED.total_sales_amount,total_order_count=EXCLUDED.total_order_count,avg_order_amount=EXCLUDED.avg_order_amount,load_time=NOW()");
            exec("INSERT INTO ads.ads_user_value (buyer_phone,buyer_name,last_order_date,order_frequency,total_monetary,avg_order_amount,value_tier,load_time) SELECT buyer_phone,MAX(buyer_name),MAX(dt),SUM(order_count),SUM(total_amount),ROUND(SUM(total_amount)*1.0/NULLIF(SUM(order_count),0),2),CASE WHEN SUM(total_amount)>=10000 THEN '高价值' WHEN SUM(total_amount)>=1000 THEN '中价值' ELSE '低价值' END,NOW() FROM dws.dws_user_order_1d GROUP BY buyer_phone ON CONFLICT (buyer_phone) DO UPDATE SET last_order_date=EXCLUDED.last_order_date,order_frequency=EXCLUDED.order_frequency,total_monetary=EXCLUDED.total_monetary,avg_order_amount=EXCLUDED.avg_order_amount,value_tier=EXCLUDED.value_tier,load_time=NOW()");
            exec("INSERT INTO ads.ads_product_ranking (product_id,product_name,category,brand,total_quantity,total_amount,rank,load_time) SELECT product_id,MAX(product_name),MAX(category),MAX(brand),SUM(total_quantity),SUM(total_amount),RANK() OVER (ORDER BY SUM(total_quantity) DESC),NOW() FROM dws.dws_product_sales_1d GROUP BY product_id ON CONFLICT (product_id) DO UPDATE SET total_quantity=EXCLUDED.total_quantity,total_amount=EXCLUDED.total_amount,rank=EXCLUDED.rank,load_time=NOW()");
            exec("INSERT INTO ads.ads_regional_sales (province,city,district,total_orders,total_amount,avg_order_amount,buyer_count,load_time) SELECT province,'合计','合计',COUNT(DISTINCT order_id),SUM(actual_amount),ROUND(SUM(actual_amount)*1.0/NULLIF(COUNT(DISTINCT order_id),0),2),COUNT(DISTINCT buyer_phone),NOW() FROM dws.dws_order_wide WHERE province!='' GROUP BY province ON CONFLICT (province,city,district) DO UPDATE SET total_orders=EXCLUDED.total_orders,total_amount=EXCLUDED.total_amount,avg_order_amount=EXCLUDED.avg_order_amount,buyer_count=EXCLUDED.buyer_count,load_time=NOW()");
            // 城市级汇总: COUNT(DISTINCT order_id) 返回 bigint, 需要强制转换
            exec("INSERT INTO ads.ads_regional_sales (province,city,district,total_orders,total_amount,avg_order_amount,buyer_count,load_time) SELECT province,city,'合计',COUNT(DISTINCT order_id)::bigint,SUM(actual_amount),ROUND(SUM(actual_amount)*1.0/NULLIF(COUNT(DISTINCT order_id),0),2),COUNT(DISTINCT buyer_phone)::bigint,NOW() FROM dws.dws_order_wide WHERE city!='' GROUP BY province,city ON CONFLICT (province,city,district) DO UPDATE SET total_orders=EXCLUDED.total_orders,total_amount=EXCLUDED.total_amount,avg_order_amount=EXCLUDED.avg_order_amount,buyer_count=EXCLUDED.buyer_count,load_time=NOW()");
            conn.commit();
            System.out.println("[DwsToAdsJob] cycle done");
        }

        @Override public void close() throws Exception { if (conn != null) conn.close(); }

        private void exec(String sql) throws Exception {
            Statement st = conn.createStatement(); st.executeUpdate(sql); st.close();
        }
    }
}
