package com.dw.job;

import com.dw.util.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * ODS → DWD 清洗作业
 *
 * 从 classpath config.properties 读取连接配置，
 * 原生 Kafka Consumer + JDBC 批量写入 PostgreSQL。
 */
public class OdsToDwdJob {

    private static Properties cfg = new Properties();
    private static Connection conn;

    public static void main(String[] args) throws Exception {
        loadConfig();
        String kafka  = cfg.getProperty("kafka.bootstrap.servers");
        String dbUrl  = cfg.getProperty("db.jdbc.url");
        String dbUser = cfg.getProperty("db.user");
        String dbPass = cfg.getProperty("db.password");
        System.out.println("OdsToDwdJob: kafka=" + kafka + " db=" + dbUrl);

        conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
        conn.setAutoCommit(false);

        int productCount = loadProducts(kafka);
        int orderCount   = loadOrders(kafka);

        conn.close();
        System.out.println("Done. products=" + productCount + " orders=" + orderCount);
    }

    private static void loadConfig() throws IOException {
        try (InputStream is = OdsToDwdJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) throw new RuntimeException("config.properties not found");
            cfg.load(is);
        }
    }

    // ---- Products ----
    private static int loadProducts(String kafka) throws Exception {
        String gid = "dwd-products-" + System.currentTimeMillis();
        Properties p = kafkaProps(kafka, gid);
        int n = 0;
        try (var c = new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(p)) {
            c.subscribe(List.of("ods_products_data"));
            long deadline = System.currentTimeMillis() + 10000;
            while (c.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
                c.poll(java.time.Duration.ofMillis(200));
            }
            if (c.assignment().isEmpty()) { System.out.println("Products: no partitions assigned"); return 0; }
            c.seekToBeginning(c.assignment());

            String sql = "INSERT INTO dwd.dwd_product(product_id,name,brand,category,sub_category,price,description,image_url,keywords,created_at,is_variant,is_abnormal,load_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NOW()) ON CONFLICT(product_id) DO UPDATE SET name=EXCLUDED.name,price=EXCLUDED.price,load_time=NOW()";
            PreparedStatement ps = conn.prepareStatement(sql);
            int batch = 0;
            deadline = System.currentTimeMillis() + 30000;
            while (System.currentTimeMillis() < deadline) {
                var recs = c.poll(java.time.Duration.ofMillis(1000));
                if (recs.isEmpty()) { if (batch == 0) continue; else break; }
                for (var r : recs) {
                    if (r.value() == null) continue;
                    Map<String,Object> m = toMap(r.value());
                    ps.setString(1, str(m,"product_id","-"));
                    ps.setString(2, str(m,"name","?"));
                    ps.setString(3, str(m,"brand",""));
                    ps.setString(4, str(m,"category",""));
                    ps.setString(5, str(m,"sub_category",""));
                    ps.setBigDecimal(6, dec(m,"price"));
                    ps.setString(7, str(m,"description",""));
                    ps.setString(8, str(m,"image_url",""));
                    ps.setString(9, keywords(m));
                    ps.setTimestamp(10, ts(m,"created_at"));
                    ps.setBoolean(11, bool(m,"is_variant"));
                    ps.setBoolean(12, false);
                    ps.addBatch();
                    if (++batch % 50 == 0) { ps.executeBatch(); conn.commit(); }
                    n++;
                }
            }
            if (batch % 50 != 0) { ps.executeBatch(); conn.commit(); }
            ps.close();
        }
        System.out.println("Products: " + n);
        return n;
    }

    // ---- Orders ----
    private static int loadOrders(String kafka) throws Exception {
        String gid = "dwd-orders-" + System.currentTimeMillis();
        Properties p = kafkaProps(kafka, gid);
        int cnt = 0, itemCnt = 0;
        try (var c = new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(p)) {
            c.subscribe(List.of("ods_orders_data"));
            long deadline = System.currentTimeMillis() + 10000;
            while (c.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
                c.poll(java.time.Duration.ofMillis(200));
            }
            if (c.assignment().isEmpty()) { System.out.println("Orders: no partitions assigned"); return 0; }
            c.seekToBeginning(c.assignment());

            String sql = "INSERT INTO dwd.dwd_order_item(order_id,product_id,product_name,brand,category,sub_category,original_price,price,quantity,sales_volume,subtotal,load_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,NOW()) ON CONFLICT DO NOTHING";
            PreparedStatement ps = conn.prepareStatement(sql);
            deadline = System.currentTimeMillis() + 30000;
            while (System.currentTimeMillis() < deadline) {
                var recs = c.poll(java.time.Duration.ofMillis(1000));
                if (recs.isEmpty()) { if (cnt == 0) continue; else break; }
                for (var r : recs) {
                    if (r.value() == null) continue;
                    Map<String,Object> m = toMap(r.value());
                    String oid = str(m,"order_id","-");
                    List<Map<String,Object>> items = (List<Map<String,Object>>) m.get("items");
                    if (items == null) continue;
                    for (Map<String,Object> it : items) {
                        ps.setString(1, oid);
                        ps.setString(2, str(it,"product_id",""));
                        ps.setString(3, str(it,"product_name",""));
                        ps.setString(4, str(it,"brand",""));
                        ps.setString(5, str(it,"category",""));
                        ps.setString(6, str(it,"sub_category",""));
                        ps.setBigDecimal(7, dec(it,"original_price"));
                        ps.setBigDecimal(8, dec(it,"price"));
                        ps.setInt(9, ((Number)it.getOrDefault("quantity",1)).intValue());
                        ps.setInt(10, ((Number)it.getOrDefault("sales_volume",1)).intValue());
                        ps.setBigDecimal(11, dec(it,"subtotal"));
                        ps.addBatch();
                        itemCnt++;
                    }
                    ps.executeBatch();
                    cnt++;
                }
                conn.commit();
            }
            ps.close();
        }
        System.out.println("Orders: " + cnt + " items: " + itemCnt);
        return cnt;
    }

    // ---- helpers ----
    private static Properties kafkaProps(String kafka, String gid) {
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka);
        p.put("group.id", gid);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        p.put("max.poll.records", "200");
        return p;
    }

    private static Map<String,Object> toMap(String json) throws Exception {
        return JsonUtils.getMapper().readValue(json, new TypeReference<Map<String,Object>>(){});
    }

    private static String str(Map<String,Object> m, String k, String def) {
        Object v = m.get(k); return v != null && !v.toString().isEmpty() ? v.toString() : def;
    }

    private static java.math.BigDecimal dec(Map<String,Object> m, String k) {
        Object v = m.get(k);
        if (v instanceof Number) return new java.math.BigDecimal(v.toString()).setScale(2, java.math.RoundingMode.HALF_UP);
        return java.math.BigDecimal.ZERO;
    }

    private static String keywords(Map<String,Object> m) {
        Object v = m.get("keywords");
        if (v instanceof List) return String.join(",", (List<String>)v);
        return "";
    }

    private static Timestamp ts(Map<String,Object> m, String k) {
        String s = str(m,k,""); if (s.isEmpty()) return new Timestamp(System.currentTimeMillis());
        try { return Timestamp.valueOf(s.replace("T"," ").substring(0,19)); } catch (Exception e) { return new Timestamp(System.currentTimeMillis()); }
    }

    private static boolean bool(Map<String,Object> m, String k) {
        Object v = m.get(k); return v instanceof Boolean && (Boolean)v;
    }
}
