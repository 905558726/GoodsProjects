package com.dw.vis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;

@Data
@TableName(value = "ads.ads_regional_sales", autoResultMap = true)
public class RegionalSales {
    private String     province;
    private String     city;
    private String     district;
    private Long       totalOrders;
    private BigDecimal totalAmount;
    private BigDecimal avgOrderAmount;
    private Long       buyerCount;
}
