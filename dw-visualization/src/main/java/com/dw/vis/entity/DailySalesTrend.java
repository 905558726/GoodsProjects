package com.dw.vis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;
import java.util.Date;

@Data
@TableName("ads.ads_daily_sales_trend")
public class DailySalesTrend {
    private java.sql.Date dt;
    private BigDecimal totalSalesAmount;
    private Long       totalOrderCount;
    private BigDecimal avgOrderAmount;
}
