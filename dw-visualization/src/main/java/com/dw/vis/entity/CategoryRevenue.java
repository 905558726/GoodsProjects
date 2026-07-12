package com.dw.vis.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;

@Data
@TableName("ads.ads_category_revenue")
public class CategoryRevenue {
    private String     category;
    private BigDecimal totalAmount;
    private Long       totalQuantity;
    private Long       orderCount;
    private Long       skuCount;
    private BigDecimal percentage;
    private Integer    rank;
}
