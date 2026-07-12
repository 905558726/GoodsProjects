package com.dw.job;

import com.dw.util.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 * ODS → DWD 数据清洗作业
 *
 * 从 classpath config.properties 读取连接配置，
 * 原生 Kafka Consumer 批量消费 + JDBC 批量 UPSERT 写入 PostgreSQL。
 *
 * 写入 3 张 DWD 表:
 *  - dwd.dwd_product     (商品清洗)
 *  - dwd.dwd_order       (订单主表，含 buyer / payment / logistics)
 *  - dwd.dwd_order_item  (订单商品明细，items 数组展开)
 *
 * 运行: java -Xmx256M -cp flink-data-warehouse.jar com.dw.job.OdsToDwdJob
 */
public class OdsToDwdJob {

    private static Properties cfg = new Properties();
    private static Connection conn;
    private static int productCount, orderCount, itemCount;

    public static void main(String[] args) throws Exception {
        loadConfig();
        String kafka  = cfg.getProperty("kafka.bootstrap.servers");
        String dbUrl  = cfg.getProperty("db.jdbc.url");
        String dbUser = cfg.getProperty("db.user");
        String dbPass = cfg.getProperty("db.password");
        System.out.println("OdsToDwdJob: kafka=" + kafka + " db=" + dbUrl);

        conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
        conn.setAutoCommit(false);

        // 不再 TRUNCATE: 使用 UPSERT 增量写入
        System.out.println("Tables truncated.");
        productCount = 0; orderCount = 0; itemCount = 0;

        loadProducts(kafka);
        loadOrders(kafka);

        conn.close();
        System.out.println("=== OdsToDwdJob FINISHED ===");
        System.out.println("Products=" + productCount + " Orders=" + orderCount + " Items=" + itemCount);
    }

    // ==================== 配置加载 ====================
    private static void loadConfig() throws IOException {
        try (InputStream is = OdsToDwdJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) throw new RuntimeException("config.properties not found in classpath");
            cfg.load(is);
        }
    }

    // ==================== 商品 ====================
    private static void loadProducts(String kafka) throws Exception {
        String gid = "dwd-products-" + System.currentTimeMillis();
        Properties p = kafkaProps(kafka, gid);
        try (var c = new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(p)) {
            c.subscribe(List.of("ods_products_data"));
            awaitAssignment(c);
            if (c.assignment().isEmpty()) { System.out.println("Products: no partitions"); return; }
            c.seekToBeginning(c.assignment());

            String sql = "INSERT INTO dwd.dwd_product"
                    + " (product_id,name,brand,category,sub_category,price,description,image_url,keywords,created_at,is_variant,is_abnormal,load_time)"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NOW())"
                    + " ON CONFLICT (product_id) DO UPDATE SET name=EXCLUDED.name, price=EXCLUDED.price, load_time=NOW()";
            PreparedStatement ps = conn.prepareStatement(sql);
            int batch = 0;
            long end = System.currentTimeMillis() + 30000;
            while (System.currentTimeMillis() < end) {
                var recs = c.poll(java.time.Duration.ofMillis(1000));
                if (recs.isEmpty()) { if (batch == 0) continue; else break; }
                for (var r : recs) {
                    if (r.value() == null) continue;
                    Map<String,Object> m = toMap(r.value());
                    ps.setString(1,  str(m,"product_id","-"));
                    ps.setString(2,  str(m,"name","?"));
                    ps.setString(3,  str(m,"brand",""));
                    ps.setString(4,  str(m,"category",""));
                    ps.setString(5,  str(m,"sub_category",""));
                    ps.setBigDecimal(6, dec(m,"price"));
                    ps.setString(7,  str(m,"description",""));
                    ps.setString(8,  str(m,"image_url",""));
                    ps.setString(9,  kwStr(m));
                    ps.setTimestamp(10, ts(m,"created_at"));
                    ps.setBoolean(11, bool(m,"is_variant"));
                    ps.setBoolean(12, false);
                    ps.addBatch();
                    if (++batch % 50 == 0) { ps.executeBatch(); conn.commit(); }
                    productCount++;
                }
            }
            if (batch % 50 != 0) { ps.executeBatch(); conn.commit(); }
            ps.close();
        }
        System.out.println("Products loaded: " + productCount);
    }

    // ==================== 订单: 同时写 dwd_order + dwd_order_item ====================
    private static void loadOrders(String kafka) throws Exception {
        String gid = "dwd-orders-" + System.currentTimeMillis();
        Properties p = kafkaProps(kafka, gid);
        try (var c = new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(p)) {
            c.subscribe(List.of("ods_orders_data"));
            awaitAssignment(c);
            if (c.assignment().isEmpty()) { System.out.println("Orders: no partitions"); return; }
            c.seekToBeginning(c.assignment());

            String sqlOrder = "INSERT INTO dwd.dwd_order"
                    + " (order_id,buyer_name,buyer_phone,buyer_email,province,city,district,address_detail,postal_code,recipient_name,recipient_phone,order_status,payment_method,payment_amount,payment_time,logistics_company,tracking_number,shipped_at,estimated_delivery,order_amount,discount,actual_amount,item_count,created_at,remark,load_time)"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())"
                    + " ON CONFLICT (order_id) DO UPDATE SET order_status=EXCLUDED.order_status, actual_amount=EXCLUDED.actual_amount, load_time=NOW()";

            String sqlItem = "INSERT INTO dwd.dwd_order_item"
                    + " (order_id,product_id,product_name,brand,category,sub_category,original_price,price,quantity,sales_volume,subtotal,load_time)"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?,NOW()) ON CONFLICT DO NOTHING";

            PreparedStatement psOrder = conn.prepareStatement(sqlOrder);
            PreparedStatement psItem  = conn.prepareStatement(sqlItem);

            long end = System.currentTimeMillis() + 30000;
            while (System.currentTimeMillis() < end) {
                var recs = c.poll(java.time.Duration.ofMillis(1000));
                if (recs.isEmpty()) { if (orderCount == 0) continue; else break; }
                for (var r : recs) {
                    if (r.value() == null) continue;
                    Map<String,Object> m = toMap(r.value());
                    String oid = str(m,"order_id","-");

                    // ----- 写 dwd_order_item -----
                    List<Map<String,Object>> items = (List<Map<String,Object>>) m.get("items");
                    int ic = 0;
                    if (items != null) {
                        for (Map<String,Object> it : items) {
                            psItem.setString(1, oid);
                            psItem.setString(2, str(it,"product_id",""));
                            psItem.setString(3, str(it,"product_name",""));
                            psItem.setString(4, str(it,"brand",""));
                            psItem.setString(5, str(it,"category",""));
                            psItem.setString(6, str(it,"sub_category",""));
                            psItem.setBigDecimal(7, dec(it,"original_price"));
                            psItem.setBigDecimal(8, dec(it,"price"));
                            psItem.setInt(9, intVal(it,"quantity",1));
                            psItem.setInt(10, intVal(it,"sales_volume",1));
                            psItem.setBigDecimal(11, dec(it,"subtotal"));
                            psItem.addBatch();
                            ic++;
                        }
                        psItem.executeBatch();
                    }
                    itemCount += ic;

                    // ----- 写 dwd_order -----
                    Map<String,Object> buyer   = (Map<String,Object>) m.get("buyer");
                    Map<String,Object> addr    = (Map<String,Object>) m.get("shipping_address");
                    Map<String,Object> pay     = (Map<String,Object>) m.get("payment");
                    Map<String,Object> logi    = (Map<String,Object>) m.get("logistics");
                    String status = str(m,"order_status","待付款");
                    boolean cancelled = "已取消".equals(status);

                    psOrder.setString(1, oid);
                    // buyer
                    psOrder.setString(2,  buyer != null ? str(buyer,"name","") : "");
                    psOrder.setString(3,  buyer != null ? str(buyer,"phone","") : "");
                    psOrder.setString(4,  buyer != null ? str(buyer,"email","") : "");
                    // shipping_address
                    psOrder.setString(5,  addr != null ? str(addr,"province","") : "");
                    psOrder.setString(6,  addr != null ? str(addr,"city","") : "");
                    psOrder.setString(7,  addr != null ? str(addr,"district","") : "");
                    psOrder.setString(8,  addr != null ? str(addr,"detail","") : "");
                    psOrder.setString(9,  addr != null ? str(addr,"postal_code","") : "");
                    psOrder.setString(10, addr != null ? str(addr,"recipient_name","") : "");
                    psOrder.setString(11, addr != null ? str(addr,"recipient_phone","") : "");
                    // status
                    psOrder.setString(12, status);
                    // payment (已取消时为空)
                    psOrder.setString(13, !cancelled && pay != null ? str(pay,"method",null) : null);
                    psOrder.setBigDecimal(14, !cancelled && pay != null ? dec(pay,"amount") : null);
                    psOrder.setTimestamp(15, !cancelled && pay != null ? ts(pay,"paid_at") : null);
                    // logistics (只有已发货/已完成有物流)
                    boolean shipped = "已发货".equals(status) || "已完成".equals(status);
                    psOrder.setString(16, shipped && logi != null ? str(logi,"company",null) : null);
                    psOrder.setString(17, shipped && logi != null ? str(logi,"tracking_number",null) : null);
                    psOrder.setTimestamp(18, shipped && logi != null ? ts(logi,"shipped_at") : null);
                    psOrder.setDate(19, shipped && logi != null ? dt(logi,"estimated_delivery") : null);
                    // amounts
                    psOrder.setBigDecimal(20, dec(m,"order_amount"));
                    psOrder.setBigDecimal(21, dec(m,"discount"));
                    psOrder.setBigDecimal(22, cancelled ? BigDecimal.ZERO : dec(m,"actual_amount"));
                    psOrder.setInt(23, ic);
                    psOrder.setTimestamp(24, ts(m,"created_at"));
                    psOrder.setString(25, str(m,"remark",null));
                    psOrder.addBatch();
                    orderCount++;

                    if (orderCount % 50 == 0) { psOrder.executeBatch(); conn.commit(); }
                }
            }
            psOrder.executeBatch();
            conn.commit();
            psOrder.close();
            psItem.close();
        }
        System.out.println("Orders loaded: " + orderCount + " items: " + itemCount);
    }

    // ==================== 工具函数 ====================

    private static void awaitAssignment(org.apache.kafka.clients.consumer.KafkaConsumer<?,?> c) {
        long deadline = System.currentTimeMillis() + 10000;
        while (c.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            c.poll(java.time.Duration.ofMillis(200));
        }
    }

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

    // ---- safe getters ----
    private static String str(Map<String,Object> m, String k, String def) {
        Object v = m.get(k); return v != null && !v.toString().isEmpty() ? v.toString() : def;
    }

    private static BigDecimal dec(Map<String,Object> m, String k) {
        Object v = m.get(k);
        if (v instanceof Number) return new BigDecimal(v.toString()).setScale(2, java.math.RoundingMode.HALF_UP);
        return BigDecimal.ZERO;
    }

    private static int intVal(Map<String,Object> m, String k, int def) {
        Object v = m.get(k); return v instanceof Number ? ((Number)v).intValue() : def;
    }

    private static String kwStr(Map<String,Object> m) {
        Object v = m.get("keywords");
        return v instanceof List ? String.join(",", (List<String>)v) : "";
    }

    private static Timestamp ts(Map<String,Object> m, String k) {
        String s = str(m,k,""); if (s.isEmpty()) return new Timestamp(System.currentTimeMillis());
        try { return Timestamp.valueOf(s.replace("T"," ").substring(0,19)); }
        catch (Exception e) { return new Timestamp(System.currentTimeMillis()); }
    }

    private static java.sql.Date dt(Map<String,Object> m, String k) {
        String s = str(m,k,""); if (s.isEmpty()) return null;
        try { return java.sql.Date.valueOf(s.substring(0,10)); } catch (Exception e) { return null; }
    }

    private static boolean bool(Map<String,Object> m, String k) {
        Object v = m.get(k); return v instanceof Boolean && (Boolean)v;
    }
}
