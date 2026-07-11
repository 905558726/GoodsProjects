package com.dw.function;

import com.dw.model.DwdOrderItem;
import com.dw.model.OrderEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * 订单数据拆分：OrderEvent → DwdOrderItem
 *
 * 处理规则：
 * - items 数组展开为独立 DwdOrderItem 行
 * - 无效订单（空 items）过滤跳过
 * - 金额/日期字段安全转换
 */
public class OrderSplitFunction implements FlatMapFunction<OrderEvent, DwdOrderItem> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderSplitFunction.class);

    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    @Override
    public void flatMap(OrderEvent event, Collector<DwdOrderItem> out) throws Exception {

        List<OrderEvent.OrderItem> items = event.getItems();
        if (items == null || items.isEmpty()) {
            LOG.warn("Order {} has no items, skip.", event.getOrderId());
            return;
        }

        Timestamp now = new Timestamp(System.currentTimeMillis());

        for (OrderEvent.OrderItem item : items) {
            DwdOrderItem dwdItem = new DwdOrderItem();
            dwdItem.setOrderId(safeStr(event.getOrderId(), "UNKNOWN"));
            dwdItem.setProductId(safeStr(item.getProductId(), "UNKNOWN"));
            dwdItem.setProductName(safeStr(item.getProductName(), "未知商品"));
            dwdItem.setBrand(safeStr(item.getBrand(), ""));
            dwdItem.setCategory(safeStr(item.getCategory(), "其他"));
            dwdItem.setSubCategory(safeStr(item.getSubCategory(), ""));
            dwdItem.setOriginalPrice(safeDecimal(item.getOriginalPrice()));
            dwdItem.setPrice(safeDecimal(item.getPrice()));
            dwdItem.setQuantity(item.getQuantity() != null ? item.getQuantity() : 1);
            dwdItem.setSalesVolume(item.getSalesVolume() != null ? item.getSalesVolume() : 1);
            dwdItem.setSubtotal(safeDecimal(item.getSubtotal()));
            dwdItem.setLoadTime(now);
            out.collect(dwdItem);
        }

        LOG.debug("Order {} processed: {} items", event.getOrderId(), items.size());
    }

    private String safeStr(String val, String def) {
        return val != null && !val.isEmpty() ? val : def;
    }

    private BigDecimal safeDecimal(BigDecimal val) {
        return val != null ? val : BigDecimal.ZERO;
    }
}
