package com.dw.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * DWD 层订单商品明细实体，对应 dwd.dwd_order_item 表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DwdOrderItem {

    private String     orderId;
    private String     productId;
    private String     productName;
    private String     brand;
    private String     category;
    private String     subCategory;
    private BigDecimal originalPrice;
    private BigDecimal price;
    private Integer    quantity;
    private Integer    salesVolume;
    private BigDecimal subtotal;
    private Timestamp  loadTime;
}
