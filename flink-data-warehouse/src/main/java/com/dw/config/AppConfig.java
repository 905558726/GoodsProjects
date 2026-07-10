package com.dw.config;

/**
 * 应用配置常量（由 config.properties 或环境变量注入）
 * 部署时替换为实际配置，模板文件参考 config-template.properties
 */
public class AppConfig {

    // ---- Kafka 配置 ----
    public static final String KAFKA_BOOTSTRAP_SERVERS = "<KAFKA_SERVERS>";
    public static final String KAFKA_TOPIC_PRODUCTS   = "ods_products_data";
    public static final String KAFKA_TOPIC_ORDERS     = "ods_orders_data";
    public static final String KAFKA_GROUP_ID         = "flink-dw-consumer";

    // ---- PostgreSQL 配置 ----
    public static final String DB_JDBC_URL  = "jdbc:postgresql://<DB_HOST>:5432/DataWarehouse";
    public static final String DB_USER      = "<DB_USER>";
    public static final String DB_PASSWORD  = "<DB_PASSWORD>";

    // ---- Flink Checkpoint 配置 ----
    public static final long   CHECKPOINT_INTERVAL_MS = 60_000;
    public static final String CHECKPOINT_DIR         = "file:///tmp/flink-checkpoints";

    // ---- 业务常量 ----
    public static final long   KAFKA_POLL_TIMEOUT_MS = 100;

    private AppConfig() {}
}
