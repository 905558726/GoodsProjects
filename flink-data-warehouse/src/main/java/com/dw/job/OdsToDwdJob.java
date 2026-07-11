package com.dw.job;

import com.dw.function.OrderSplitFunction;
import com.dw.function.ProductCleanFunction;
import com.dw.model.DwdOrderItem;
import com.dw.model.DwdProduct;
import com.dw.model.OrderEvent;
import com.dw.model.ProductEvent;
import com.dw.sink.PostgresJdbcSink;
import com.dw.source.OrderDeserializer;
import com.dw.source.ProductDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink 作业1: ODS层 → DWD层
 *
 * 从 Kafka 消费商品/订单两条流的 JSON 原始数据，
 * 经过清洗（字段标准化、异常过滤、items 展开）后，
 * 批量 UPSERT 写入 PostgreSQL dwd schema。
 *
 * 提交命令示例:
 *   java -Xmx256M -cp flink-data-warehouse.jar com.dw.job.OdsToDwdJob
 *   --kafka <KAFKA_SERVERS> --db <JDBC_URL> --user <USER> --pass <PASSWORD>
 */
public class OdsToDwdJob {

    private static final Logger LOG = LoggerFactory.getLogger(OdsToDwdJob.class);

    private static String kafkaServers  = System.getProperty("kafka.servers", "120.79.232.83:9092");
    private static String dbJdbcUrl     = System.getProperty("db.jdbc.url",
            "jdbc:postgresql://120.79.232.83:5432/DataWarehouse");
    private static String dbUser        = System.getProperty("db.user", "root");
    private static String dbPassword    = System.getProperty("db.password", "");
    private static int    batchSize     = Integer.getInteger("batch.size", 200);

    public static void main(String[] args) throws Exception {

        // ---- 命令行参数覆盖 ----
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--kafka": kafkaServers = args[++i]; break;
                case "--db":    dbJdbcUrl    = args[++i]; break;
                case "--user":  dbUser       = args[++i]; break;
                case "--pass":  dbPassword   = args[++i]; break;
                case "--batch": batchSize    = Integer.parseInt(args[++i]); break;
                default: break;
            }
        }
        LOG.info("Starting OdsToDwdJob | kafka={} | db={} | batch={}", kafkaServers, dbJdbcUrl, batchSize);

        // ---- Flink 环境 ----
        Configuration flinkConf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, flinkConf);
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);

        // ---- 1. Kafka Source: 商品流 ----
        KafkaSource<ProductEvent> productSource = KafkaSource.<ProductEvent>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics("ods_products_data")
                .setGroupId("dwd-product-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new ProductDeserializer())
                .build();

        DataStream<ProductEvent> productStream = env
                .fromSource(productSource, WatermarkStrategy.noWatermarks(), "kafka-product-source")
                .uid("kafka-product-source");

        SingleOutputStreamOperator<DwdProduct> cleanProductStream = productStream
                .map(new ProductCleanFunction())
                .name("product-clean")
                .uid("product-clean")
                .filter(p -> p != null)
                .name("product-filter-null")
                .uid("product-filter-null");

        // ---- 2. Kafka Source: 订单流 ----
        KafkaSource<OrderEvent> orderSource = KafkaSource.<OrderEvent>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics("ods_orders_data")
                .setGroupId("dwd-order-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new OrderDeserializer())
                .build();

        DataStream<OrderEvent> orderStream = env
                .fromSource(orderSource, WatermarkStrategy.noWatermarks(), "kafka-order-source")
                .uid("kafka-order-source");

        SingleOutputStreamOperator<DwdOrderItem> orderItemStream = orderStream
                .flatMap(new OrderSplitFunction())
                .name("order-split")
                .uid("order-split")
                .filter(item -> item != null)
                .name("order-item-filter-null")
                .uid("order-item-filter-null");

        // ---- 3. 合并写入 PostgreSQL ----
        PostgresJdbcSink dbSink = new PostgresJdbcSink(dbJdbcUrl, dbUser, dbPassword, batchSize);

        DataStream<Object> mergedStream = cleanProductStream
                .map(p -> (Object) p)
                .union(orderItemStream.map(i -> (Object) i));

        mergedStream
                .addSink(dbSink)
                .name("postgres-dwd-sink")
                .uid("postgres-dwd-sink");

        // ---- 4. 执行 ----
        env.execute("ODS-to-DWD Job");
    }
}
