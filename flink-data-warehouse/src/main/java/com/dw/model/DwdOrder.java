package com.dw.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * DWD 层订单主表实体，对应 dwd.dwd_order 表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DwdOrder {

    private String     orderId;
    private String     buyerName;
    private String     buyerPhone;
    private String     buyerEmail;
    private String     province;
    private String     city;
    private String     district;
    private String     addressDetail;
    private String     postalCode;
    private String     recipientName;
    private String     recipientPhone;
    private String     orderStatus;
    private String     paymentMethod;
    private BigDecimal paymentAmount;
    private Timestamp  paymentTime;
    private String     logisticsCompany;
    private String     trackingNumber;
    private Timestamp  shippedAt;
    private Date       estimatedDelivery;
    private BigDecimal orderAmount;
    private BigDecimal discount;
    private BigDecimal actualAmount;
    private Integer    itemCount;
    private Timestamp  createdAt;
    private String     remark;
    private Timestamp  loadTime;
}
