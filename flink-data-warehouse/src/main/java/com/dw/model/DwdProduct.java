package com.dw.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * DWD 层商品实体，对应 dwd.dwd_product 表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DwdProduct {

    private String    productId;
    private String    name;
    private String    brand;
    private String    category;
    private String    subCategory;
    private BigDecimal price;
    private String    description;
    private String    imageUrl;
    private String    keywords;       // List<String> JSON序列化后存储
    private Timestamp createdAt;
    private Boolean   isVariant;
    private Boolean   isAbnormal;     // 清洗后标记异常
    private Timestamp loadTime;
}
