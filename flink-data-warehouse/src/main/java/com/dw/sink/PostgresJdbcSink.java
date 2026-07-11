package com.dw.sink;

import com.dw.model.DwdOrderItem;
import com.dw.model.DwdProduct;
import com.dw.util.JdbcUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL 批量 Sink — 将清洗后的 DWD 数据批量写入数据库
 */
public class PostgresJdbcSink extends RichSinkFunction<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresJdbcSink.class);

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final int    batchSize;

    private transient Connection conn;
    private final List<Object[]> productBuffer  = new ArrayList<>();
    private final List<Object[]> orderItemBuffer = new ArrayList<>();

    private static final String UPSERT_PRODUCT =
            "INSERT INTO dwd.dwd_product (product_id, name, brand, category, sub_category, price, " +
            "description, image_url, keywords, created_at, is_variant, is_abnormal, load_time) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) " +
            "ON CONFLICT (product_id) DO UPDATE SET " +
            "name=EXCLUDED.name, price=EXCLUDED.price, category=EXCLUDED.category, " +
            "is_abnormal=EXCLUDED.is_abnormal, load_time=EXCLUDED.load_time";

    private static final String UPSERT_ORDER_ITEM =
            "INSERT INTO dwd.dwd_order_item (order_id, product_id, product_name, brand, category, " +
            "sub_category, original_price, price, quantity, sales_volume, subtotal, load_time) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) " +
            "ON CONFLICT DO NOTHING";  // 明细表自增ID，不做update

    public PostgresJdbcSink(String jdbcUrl, String user, String password, int batchSize) {
        this.jdbcUrl   = jdbcUrl;
        this.user      = user;
        this.password  = password;
        this.batchSize = batchSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = JdbcUtils.getConnection(jdbcUrl, user, password);
        LOG.info("PostgresJdbcSink connection established.");
    }

    @Override
    public void invoke(Object record, Context context) throws Exception {
        if (record instanceof DwdProduct) {
            DwdProduct p = (DwdProduct) record;
            productBuffer.add(new Object[]{
                    p.getProductId(), p.getName(), p.getBrand(), p.getCategory(),
                    p.getSubCategory(), p.getPrice(), p.getDescription(), p.getImageUrl(),
                    p.getKeywords(), p.getCreatedAt(), p.getIsVariant(), p.getIsAbnormal(),
                    p.getLoadTime()
            });
            if (productBuffer.size() >= batchSize) {
                flushProducts();
            }
        } else if (record instanceof DwdOrderItem) {
            DwdOrderItem item = (DwdOrderItem) record;
            orderItemBuffer.add(new Object[]{
                    item.getOrderId(), item.getProductId(), item.getProductName(),
                    item.getBrand(), item.getCategory(), item.getSubCategory(),
                    item.getOriginalPrice(), item.getPrice(), item.getQuantity(),
                    item.getSalesVolume(), item.getSubtotal(), item.getLoadTime()
            });
            if (orderItemBuffer.size() >= batchSize) {
                flushOrderItems();
            }
        }
    }

    private void flushProducts() {
        JdbcUtils.executeBatchUpsert(conn, UPSERT_PRODUCT, new ArrayList<>(productBuffer));
        productBuffer.clear();
    }

    private void flushOrderItems() {
        JdbcUtils.executeBatchUpsert(conn, UPSERT_ORDER_ITEM, new ArrayList<>(orderItemBuffer));
        orderItemBuffer.clear();
    }

    @Override
    public void close() throws Exception {
        // 关闭前刷出残留
        flushProducts();
        flushOrderItems();
        JdbcUtils.closeConnection(conn);
        LOG.info("PostgresJdbcSink closed. Final flush done.");
        super.close();
    }
}
