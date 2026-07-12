package com.dw.job;

import com.dw.util.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

/**
 * ODS → DWD 实时数据清洗 — Flink 常驻 Streaming Job
 *
 * Kafka 持续消费商品/订单数据，实时 UPSERT 写入 PostgreSQL DWD 层。
 * 作为 Flink Session 常驻 Job 提交，不退出。
 *
 * flink run -d -c com.dw.job.OdsToDwdJob flink-data-warehouse.jar
 */
public class OdsToDwdJob {

    private static Properties cfg = new Properties();
    private static Connection conn;

    public static void main(String[] args) {
        try {
            loadConfig();
            String kafka = cfg.getProperty("kafka.bootstrap.servers");
            String dbUrl = cfg.getProperty("db.jdbc.url");
            String dbUser = cfg.getProperty("db.user");
            String dbPass = cfg.getProperty("db.password");
            System.out.println("[OdsToDwdJob] STARTED kafka=" + kafka + " db=" + dbUrl);

            conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            conn.setAutoCommit(false);

            // 双线程并发消费
            Thread pt = new Thread(() -> consumeProducts(kafka));
            Thread ot = new Thread(() -> consumeOrders(kafka));
            pt.setDaemon(true); ot.setDaemon(true);
            pt.start(); ot.start();

            // 主线程常驻不退出
            while (true) {
                Thread.sleep(60000);
                System.out.println("[OdsToDwdJob] heartbeat");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void loadConfig() throws IOException {
        try (InputStream is = OdsToDwdJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is == null) throw new RuntimeException("config.properties not found");
            cfg.load(is);
        }
    }

    // ---- 商品持续消费 ----
    private static void consumeProducts(String kafka) {
        Properties p = kafkaProps(kafka, "dwd-product-stream");
        try (var c = new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(p)) {
            c.subscribe(List.of("ods_products_data"));
            String sql = "INSERT INTO dwd.dwd_product (product_id,name,brand,category,sub_category,price,description,image_url,keywords,created_at,is_variant,is_abnormal,load_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NOW()) ON CONFLICT (product_id) DO UPDATE SET name=EXCLUDED.name,price=EXCLUDED.price,load_time=NOW()";
            PreparedStatement ps = conn.prepareStatement(sql);
            int batch = 0;
            while (true) {
                var recs = c.poll(java.time.Duration.ofMillis(2000));
                if (recs.isEmpty()) continue;
                for (var r : recs) {
                    if (r.value() == null) continue;
                    try {
                        Map<String,Object> m = toMap(r.value());
                        ps.setString(1, s(m,"product_id","-")); ps.setString(2, s(m,"name","?"));
                        ps.setString(3, s(m,"brand","")); ps.setString(4, s(m,"category",""));
                        ps.setString(5, s(m,"sub_category","")); ps.setBigDecimal(6, d(m,"price"));
                        ps.setString(7, s(m,"description","")); ps.setString(8, s(m,"image_url",""));
                        ps.setString(9, kw(m)); ps.setTimestamp(10, ts(m,"created_at"));
                        ps.setBoolean(11, b(m,"is_variant")); ps.setBoolean(12, false);
                        ps.addBatch();
                        if (++batch % 20 == 0) { ps.executeBatch(); conn.commit(); }
                    } catch (Exception ignored) {}
                }
                if (batch % 20 != 0) { ps.executeBatch(); conn.commit(); }
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    // ---- 订单持续消费 ----
    private static void consumeOrders(String kafka) {
        Properties p = kafkaProps(kafka, "dwd-order-stream");
        try (var c = new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(p)) {
            c.subscribe(List.of("ods_orders_data"));
            String sqlItem = "INSERT INTO dwd.dwd_order_item (order_id,product_id,product_name,brand,category,sub_category,original_price,price,quantity,sales_volume,subtotal,load_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,NOW()) ON CONFLICT DO NOTHING";
            String sqlOrder = "INSERT INTO dwd.dwd_order (order_id,buyer_name,buyer_phone,buyer_email,province,city,district,address_detail,postal_code,recipient_name,recipient_phone,order_status,payment_method,payment_amount,payment_time,logistics_company,tracking_number,shipped_at,estimated_delivery,order_amount,discount,actual_amount,item_count,created_at,remark,load_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW()) ON CONFLICT (order_id) DO UPDATE SET order_status=EXCLUDED.order_status,actual_amount=EXCLUDED.actual_amount,load_time=NOW()";
            PreparedStatement psi = conn.prepareStatement(sqlItem);
            PreparedStatement pso = conn.prepareStatement(sqlOrder);
            while (true) {
                var recs = c.poll(java.time.Duration.ofMillis(2000));
                if (recs.isEmpty()) continue;
                for (var r : recs) {
                    if (r.value() == null) continue;
                    try {
                        Map<String,Object> m = toMap(r.value());
                        String oid = s(m,"order_id","-");
                        List<Map<String,Object>> items = (List<Map<String,Object>>) m.get("items");
                        if (items != null) for (Map<String,Object> it : items) {
                            psi.setString(1, oid); psi.setString(2, s(it,"product_id",""));
                            psi.setString(3, s(it,"product_name","")); psi.setString(4, s(it,"brand",""));
                            psi.setString(5, s(it,"category","")); psi.setString(6, s(it,"sub_category",""));
                            psi.setBigDecimal(7, d(it,"original_price")); psi.setBigDecimal(8, d(it,"price"));
                            psi.setInt(9, i(it,"quantity",1)); psi.setInt(10, i(it,"sales_volume",1));
                            psi.setBigDecimal(11, d(it,"subtotal")); psi.addBatch();
                        }
                        psi.executeBatch();
                        Map<String,Object> buyer=(Map<String,Object>)m.get("buyer"),addr=(Map<String,Object>)m.get("shipping_address"),pay=(Map<String,Object>)m.get("payment"),logi=(Map<String,Object>)m.get("logistics");
                        String status=s(m,"order_status","待付款"); boolean cancelled="已取消".equals(status);
                        pso.setString(1,oid); pso.setString(2,buyer!=null?s(buyer,"name",""):""); pso.setString(3,buyer!=null?s(buyer,"phone",""):""); pso.setString(4,buyer!=null?s(buyer,"email",""):"");
                        pso.setString(5,addr!=null?s(addr,"province",""):""); pso.setString(6,addr!=null?s(addr,"city",""):""); pso.setString(7,addr!=null?s(addr,"district",""):"");
                        pso.setString(8,addr!=null?s(addr,"detail",""):""); pso.setString(9,addr!=null?s(addr,"postal_code",""):""); pso.setString(10,addr!=null?s(addr,"recipient_name",""):""); pso.setString(11,addr!=null?s(addr,"recipient_phone",""):"");
                        pso.setString(12,status); pso.setString(13,!cancelled&&pay!=null?s(pay,"method",null):null); pso.setBigDecimal(14,!cancelled&&pay!=null?d(pay,"amount"):null); pso.setTimestamp(15,!cancelled&&pay!=null?ts(pay,"paid_at"):null);
                        boolean shipped="已发货".equals(status)||"已完成".equals(status);
                        pso.setString(16,shipped&&logi!=null?s(logi,"company",null):null); pso.setString(17,shipped&&logi!=null?s(logi,"tracking_number",null):null); pso.setTimestamp(18,shipped&&logi!=null?ts(logi,"shipped_at"):null); pso.setDate(19,null);
                        pso.setBigDecimal(20,d(m,"order_amount")); pso.setBigDecimal(21,d(m,"discount")); pso.setBigDecimal(22,cancelled?BigDecimal.ZERO:d(m,"actual_amount")); pso.setInt(23,items!=null?items.size():0); pso.setTimestamp(24,ts(m,"created_at")); pso.setString(25,s(m,"remark",null));
                        pso.addBatch(); pso.executeBatch();
                    } catch (Exception ignored) {}
                }
                conn.commit();
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    // ---- helpers ----
    private static Properties kafkaProps(String kafka, String gid) {
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka); p.put("group.id", gid);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("auto.offset.reset", "latest"); p.put("enable.auto.commit", "false"); p.put("max.poll.records", "100");
        return p;
    }
    private static Map<String,Object> toMap(String j) throws Exception { return JsonUtils.getMapper().readValue(j,new TypeReference<Map<String,Object>>(){}); }
    private static String s(Map<String,Object> m, String k, String def) { Object v=m.get(k); return v!=null&&!v.toString().isEmpty()?v.toString():def; }
    private static BigDecimal d(Map<String,Object> m, String k) { Object v=m.get(k); return v instanceof Number?new BigDecimal(v.toString()).setScale(2,java.math.RoundingMode.HALF_UP):BigDecimal.ZERO; }
    private static int i(Map<String,Object> m, String k, int def) { Object v=m.get(k); return v instanceof Number?((Number)v).intValue():def; }
    private static String kw(Map<String,Object> m) { Object v=m.get("keywords"); return v instanceof List?String.join(",",(List<String>)v):""; }
    private static Timestamp ts(Map<String,Object> m, String k) { String ss=s(m,k,""); if(ss.isEmpty()) return new Timestamp(System.currentTimeMillis()); try{return Timestamp.valueOf(ss.replace("T"," ").substring(0,19));}catch(Exception e){return new Timestamp(System.currentTimeMillis());} }
    private static boolean b(Map<String,Object> m, String k) { Object v=m.get(k); return v instanceof Boolean&&(Boolean)v; }
}
