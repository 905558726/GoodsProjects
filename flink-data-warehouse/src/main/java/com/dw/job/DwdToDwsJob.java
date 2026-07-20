package com.dw.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * DWD → DWS 定时轮询汇总 — Flink DataStream Job
 *
 * 每 30s 从 DWD 全量刷新 DWS 四表，可部署到 Flink 集群。
 *
 * flink run -d -c com.dw.job.DwdToDwsJob flink-data-warehouse.jar
 */
public class DwdToDwsJob {

    public static void main(String[] args) throws Exception {
        Properties cfg = loadConfig();
        String dbUrl  = cfg.getProperty("db.jdbc.url");
        String dbUser = cfg.getProperty("db.user");
        String dbPass = cfg.getProperty("db.password");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(60_000); // 60s

        DataStream<Integer> ticks = env.addSource(new TickingSource(30_000)).name("tick-source").uid("tick-source");
        ticks.addSink(new DwsRefreshSink(dbUrl, dbUser, dbPass)).name("dws-sink").uid("dws-sink");

        System.out.println("[DwdToDwsJob] FLINK JOB STARTED");
        env.execute("DWD-to-DWS Refresh");
    }

    private static Properties loadConfig() throws IOException {
        Properties cfg = new Properties();
        try (InputStream is = DwdToDwsJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) throw new RuntimeException("config.properties not found");
            cfg.load(is);
        }
        return cfg;
    }

    // ---- 30s Ticking Source ----
    public static class TickingSource extends RichSourceFunction<Integer> {
        private volatile boolean running = true;
        private final long intervalMs;
        public TickingSource(long intervalMs) { this.intervalMs = intervalMs; }
        @Override public void run(SourceContext<Integer> ctx) throws InterruptedException {
            while (running) { ctx.collect(1); Thread.sleep(intervalMs); }
        }
        @Override public void cancel() { running = false; }
    }

    // ---- DWS Refresh Sink ----
    public static class DwsRefreshSink extends RichSinkFunction<Integer> {
        private final String url, user, pass;
        private transient Connection conn;
        public DwsRefreshSink(String url, String user, String pass) { this.url = url; this.user = user; this.pass = pass; }

        @Override public void open(Configuration p) throws Exception {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);
        }

        @Override public void invoke(Integer tick, Context ctx) throws Exception {
            exec("INSERT INTO dws.dws_order_wide (order_id,product_id,product_name,brand,category,sub_category,price,quantity,subtotal,order_status,buyer_name,buyer_phone,buyer_email,province,city,district,payment_method,payment_amount,logistics_company,tracking_number,order_amount,discount,actual_amount,order_created_at,dt,load_time) SELECT oi.order_id,oi.product_id,oi.product_name,oi.brand,oi.category,oi.sub_category,oi.price,oi.quantity,oi.subtotal,COALESCE(o.order_status,''),COALESCE(o.buyer_name,''),COALESCE(o.buyer_phone,''),COALESCE(o.buyer_email,''),COALESCE(o.province,''),COALESCE(o.city,''),COALESCE(o.district,''),COALESCE(o.payment_method,''),COALESCE(o.payment_amount,0),COALESCE(o.logistics_company,''),COALESCE(o.tracking_number,''),COALESCE(o.order_amount,0),COALESCE(o.discount,0),COALESCE(o.actual_amount,0),COALESCE(o.created_at,NOW()),COALESCE(o.created_at,NOW())::date,NOW() FROM dwd.dwd_order_item oi LEFT JOIN dwd.dwd_order o ON oi.order_id=o.order_id ON CONFLICT (order_id,product_id) DO NOTHING");
            exec("INSERT INTO dws.dws_product_sales_1d (product_id,dt,product_name,category,brand,total_quantity,total_amount,order_count,avg_price,load_time) SELECT oi.product_id,COALESCE(o.created_at,NOW())::date,MAX(oi.product_name),MAX(oi.category),MAX(oi.brand),SUM(oi.quantity),SUM(oi.subtotal),COUNT(DISTINCT oi.order_id),CASE WHEN SUM(oi.quantity)>0 THEN SUM(oi.subtotal)/SUM(oi.quantity) ELSE 0 END,NOW() FROM dwd.dwd_order_item oi LEFT JOIN dwd.dwd_order o ON oi.order_id=o.order_id GROUP BY oi.product_id,COALESCE(o.created_at,NOW())::date ON CONFLICT (product_id,dt) DO UPDATE SET total_quantity=EXCLUDED.total_quantity,total_amount=EXCLUDED.total_amount,order_count=EXCLUDED.order_count,avg_price=EXCLUDED.avg_price,load_time=NOW()");
            exec("INSERT INTO dws.dws_category_sales_1d (category,dt,total_quantity,total_amount,order_count,sku_count,load_time) SELECT oi.category,COALESCE(o.created_at,NOW())::date,SUM(oi.quantity),SUM(oi.subtotal),COUNT(DISTINCT oi.order_id),COUNT(DISTINCT oi.product_id),NOW() FROM dwd.dwd_order_item oi LEFT JOIN dwd.dwd_order o ON oi.order_id=o.order_id GROUP BY oi.category,COALESCE(o.created_at,NOW())::date ON CONFLICT (category,dt) DO UPDATE SET total_quantity=EXCLUDED.total_quantity,total_amount=EXCLUDED.total_amount,order_count=EXCLUDED.order_count,sku_count=EXCLUDED.sku_count,load_time=NOW()");
            exec("INSERT INTO dws.dws_user_order_1d (buyer_phone,dt,buyer_name,order_count,total_amount,total_quantity,completed_count,cancelled_count,pending_count,load_time) SELECT COALESCE(o.buyer_phone,'unknown'),COALESCE(o.created_at,NOW())::date,MAX(COALESCE(o.buyer_name,'')),COUNT(DISTINCT oi.order_id),SUM(COALESCE(o.actual_amount,oi.subtotal,0)),SUM(oi.quantity),SUM(CASE WHEN o.order_status='已完成' THEN 1 ELSE 0 END),SUM(CASE WHEN o.order_status='已取消' THEN 1 ELSE 0 END),SUM(CASE WHEN o.order_status IN ('待付款','已付款','已发货') THEN 1 ELSE 0 END),NOW() FROM dwd.dwd_order_item oi LEFT JOIN dwd.dwd_order o ON oi.order_id=o.order_id WHERE COALESCE(o.buyer_phone,'')!='' GROUP BY COALESCE(o.buyer_phone,'unknown'),COALESCE(o.created_at,NOW())::date ON CONFLICT (buyer_phone,dt) DO UPDATE SET order_count=EXCLUDED.order_count,total_amount=EXCLUDED.total_amount,total_quantity=EXCLUDED.total_quantity,completed_count=EXCLUDED.completed_count,cancelled_count=EXCLUDED.cancelled_count,pending_count=EXCLUDED.pending_count,load_time=NOW()");
            conn.commit();
            System.out.println("[DwdToDwsJob] cycle done");
        }

        @Override public void close() throws Exception { if (conn != null) conn.close(); }

        private void exec(String sql) throws Exception {
            Statement st = conn.createStatement(); st.executeUpdate(sql); st.close();
        }
    }
}
