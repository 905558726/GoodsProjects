package com.dw.job;

import com.dw.util.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

/**
 * ODS → DWD 实时 Flink DataStream Job
 *
 * 使用 Flink KafkaSource + DataStream + JDBC Sink 架构。
 * 可部署到 Flink Standalone 集群作为常驻 Streaming Job。
 *
 * flink run -d -c com.dw.job.OdsToDwdJob flink-data-warehouse.jar
 */
public class OdsToDwdJob {

    public static void main(String[] args) throws Exception {
        // 加载数据库配置（不从 args 传）
        Properties cfg = loadConfig();
        String dbUrl  = cfg.getProperty("db.jdbc.url");
        String dbUser = cfg.getProperty("db.user");
        String dbPass = cfg.getProperty("db.password");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(60_000); // 60s

        // ---- Product Stream ----
        KafkaSource<String> productSource = stringKafkaSource("ods_products_data", "dwd-product-flink");
        DataStream<String> productStream = env.fromSource(productSource, WatermarkStrategy.noWatermarks(), "kafka-product");
        productStream.addSink(new DwdProductSink(dbUrl, dbUser, dbPass)).name("sink-product").uid("sink-product");

        // ---- Order Stream ----
        KafkaSource<String> orderSource = stringKafkaSource("ods_orders_data", "dwd-order-flink");
        DataStream<String> orderStream = env.fromSource(orderSource, WatermarkStrategy.noWatermarks(), "kafka-order");
        orderStream.addSink(new DwdOrderSink(dbUrl, dbUser, dbPass)).name("sink-order").uid("sink-order");

        System.out.println("[OdsToDwdJob] FLINK STREAMING JOB STARTED");
        env.execute("ODS-to-DWD Streaming");
    }

    private static Properties loadConfig() throws IOException {
        Properties cfg = new Properties();
        try (InputStream is = OdsToDwdJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) throw new RuntimeException("config.properties not found");
            cfg.load(is);
        }
        return cfg;
    }

    private static KafkaSource<String> stringKafkaSource(String topic, String group) {
        return KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<String>() {
                    @Override public String deserialize(byte[] message) {
                        return message != null ? new String(message) : null;
                    }
                })
                .build();
    }

    // ============ Product JDBC Sink ============
    public static class DwdProductSink extends RichSinkFunction<String> {
        private final String url, user, pass;
        private transient Connection conn;
        private transient PreparedStatement ps;

        public DwdProductSink(String url, String user, String pass) { this.url = url; this.user = user; this.pass = pass; }

        @Override public void open(Configuration p) throws Exception {
            conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);
            ps = conn.prepareStatement("INSERT INTO dwd.dwd_product (product_id,name,brand,category,sub_category,price,description,image_url,keywords,created_at,is_variant,is_abnormal,load_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NOW()) ON CONFLICT (product_id) DO UPDATE SET name=EXCLUDED.name,price=EXCLUDED.price,load_time=NOW()");
        }

        @Override public void invoke(String json, Context ctx) throws Exception {
            if (json == null) return;
            Map<String,Object> m = JsonUtils.getMapper().readValue(json, new TypeReference<Map<String,Object>>(){});
            ps.setString(1, s(m,"product_id","-")); ps.setString(2, s(m,"name","?"));
            ps.setString(3, s(m,"brand","")); ps.setString(4, s(m,"category",""));
            ps.setString(5, s(m,"sub_category","")); ps.setBigDecimal(6, d(m,"price"));
            ps.setString(7, s(m,"description","")); ps.setString(8, s(m,"image_url",""));
            ps.setString(9, kw(m)); ps.setTimestamp(10, ts(m,"created_at"));
            ps.setBoolean(11, b(m,"is_variant")); ps.setBoolean(12, false);
            ps.executeUpdate(); conn.commit();
        }

        @Override public void close() throws Exception { if (ps != null) ps.close(); if (conn != null) conn.close(); }
    }

    // ============ Order JDBC Sink ============
    public static class DwdOrderSink extends RichSinkFunction<String> {
        private final String url, user, pass;
        private transient Connection conn;
        private transient PreparedStatement psItem, psOrder;

        public DwdOrderSink(String url, String user, String pass) { this.url = url; this.user = user; this.pass = pass; }

        @Override public void open(Configuration p) throws Exception {
            conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);
            psItem = conn.prepareStatement("INSERT INTO dwd.dwd_order_item (order_id,product_id,product_name,brand,category,sub_category,original_price,price,quantity,sales_volume,subtotal,load_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,NOW()) ON CONFLICT DO NOTHING");
            psOrder = conn.prepareStatement("INSERT INTO dwd.dwd_order (order_id,buyer_name,buyer_phone,buyer_email,province,city,district,address_detail,postal_code,recipient_name,recipient_phone,order_status,payment_method,payment_amount,payment_time,logistics_company,tracking_number,shipped_at,estimated_delivery,order_amount,discount,actual_amount,item_count,created_at,remark,load_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW()) ON CONFLICT (order_id) DO UPDATE SET order_status=EXCLUDED.order_status,actual_amount=EXCLUDED.actual_amount,load_time=NOW()");
        }

        @Override public void invoke(String json, Context ctx) throws Exception {
            if (json == null) return;
            Map<String,Object> m = JsonUtils.getMapper().readValue(json, new TypeReference<Map<String,Object>>(){});
            String oid = s(m,"order_id","-");
            List<Map<String,Object>> items = (List<Map<String,Object>>) m.get("items");
            if (items != null) for (Map<String,Object> it : items) {
                psItem.setString(1, oid); psItem.setString(2, s(it,"product_id",""));
                psItem.setString(3, s(it,"product_name","")); psItem.setString(4, s(it,"brand",""));
                psItem.setString(5, s(it,"category","")); psItem.setString(6, s(it,"sub_category",""));
                psItem.setBigDecimal(7, d(it,"original_price")); psItem.setBigDecimal(8, d(it,"price"));
                psItem.setInt(9, i(it,"quantity",1)); psItem.setInt(10, i(it,"sales_volume",1));
                psItem.setBigDecimal(11, d(it,"subtotal")); psItem.addBatch();
            }
            psItem.executeBatch();
            Map<String,Object> buyer=(Map<String,Object>)m.get("buyer"),addr=(Map<String,Object>)m.get("shipping_address"),pay=(Map<String,Object>)m.get("payment"),logi=(Map<String,Object>)m.get("logistics");
            String status=s(m,"order_status","待付款"); boolean cancelled="已取消".equals(status);
            psOrder.setString(1,oid); psOrder.setString(2,buyer!=null?s(buyer,"name",""):""); psOrder.setString(3,buyer!=null?s(buyer,"phone",""):""); psOrder.setString(4,buyer!=null?s(buyer,"email",""):"");
            psOrder.setString(5,addr!=null?s(addr,"province",""):""); psOrder.setString(6,addr!=null?s(addr,"city",""):""); psOrder.setString(7,addr!=null?s(addr,"district",""):"");
            psOrder.setString(8,addr!=null?s(addr,"detail",""):""); psOrder.setString(9,addr!=null?s(addr,"postal_code",""):""); psOrder.setString(10,addr!=null?s(addr,"recipient_name",""):""); psOrder.setString(11,addr!=null?s(addr,"recipient_phone",""):"");
            psOrder.setString(12,status); psOrder.setString(13,!cancelled&&pay!=null?s(pay,"method",null):null); psOrder.setBigDecimal(14,!cancelled&&pay!=null?d(pay,"amount"):null); psOrder.setTimestamp(15,!cancelled&&pay!=null?ts(pay,"paid_at"):null);
            boolean shipped="已发货".equals(status)||"已完成".equals(status);
            psOrder.setString(16,shipped&&logi!=null?s(logi,"company",null):null); psOrder.setString(17,shipped&&logi!=null?s(logi,"tracking_number",null):null); psOrder.setTimestamp(18,shipped&&logi!=null?ts(logi,"shipped_at"):null); psOrder.setDate(19,null);
            psOrder.setBigDecimal(20,d(m,"order_amount")); psOrder.setBigDecimal(21,d(m,"discount")); psOrder.setBigDecimal(22,cancelled?BigDecimal.ZERO:d(m,"actual_amount")); psOrder.setInt(23,items!=null?items.size():0); psOrder.setTimestamp(24,ts(m,"created_at")); psOrder.setString(25,s(m,"remark",null));
            psOrder.executeUpdate(); conn.commit();
        }

        @Override public void close() throws Exception { if (psItem != null) psItem.close(); if (psOrder != null) psOrder.close(); if (conn != null) conn.close(); }
    }

    // ---- helpers ----
    private static String s(Map<String,Object> m, String k, String def) { Object v=m.get(k); return v!=null&&!v.toString().isEmpty()?v.toString():def; }
    private static BigDecimal d(Map<String,Object> m, String k) { Object v=m.get(k); return v instanceof Number?new BigDecimal(v.toString()).setScale(2,java.math.RoundingMode.HALF_UP):BigDecimal.ZERO; }
    private static int i(Map<String,Object> m, String k, int def) { Object v=m.get(k); return v instanceof Number?((Number)v).intValue():def; }
    private static String kw(Map<String,Object> m) { Object v=m.get("keywords"); return v instanceof List?String.join(",",(List<String>)v):""; }
    private static Timestamp ts(Map<String,Object> m, String k) { String ss=s(m,k,""); if(ss.isEmpty()) return new Timestamp(System.currentTimeMillis()); try{return Timestamp.valueOf(ss.replace("T"," ").substring(0,19));}catch(Exception e){return new Timestamp(System.currentTimeMillis());} }
    private static boolean b(Map<String,Object> m, String k) { Object v=m.get(k); return v instanceof Boolean&&(Boolean)v; }
}
