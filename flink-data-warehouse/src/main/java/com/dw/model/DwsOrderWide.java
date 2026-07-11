package com.dw.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * DWS 层订单商品宽表实体，对应 dws.dws_order_wide
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DwsOrderWide {
    private String     orderId;
    private String     productId;
    private String     productName;
    private String     brand;
    private String     category;
    private String     subCategory;
    private BigDecimal price;
    private Integer    quantity;
    private BigDecimal subtotal;
    private String     orderStatus;
    private String     buyerName;
    private String     buyerPhone;
    private String     buyerEmail;
    private String     province;
    private String     city;
    private String     district;
    private String     paymentMethod;
    private BigDecimal paymentAmount;
    private String     logisticsCompany;
    private String     trackingNumber;
    private BigDecimal orderAmount;
    private BigDecimal discount;
    private BigDecimal actualAmount;
    private Timestamp  orderCreatedAt;
    private Date       dt;
    private Timestamp  loadTime;
}
