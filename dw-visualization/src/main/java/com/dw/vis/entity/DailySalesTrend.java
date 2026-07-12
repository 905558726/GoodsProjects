package com.dw.vis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;

@Data
@TableName(value = "ads.ads_daily_sales_trend", autoResultMap = true)
public class DailySalesTrend {
    private java.sql.Date dt;
    private BigDecimal totalSalesAmount;
    private Long       totalOrderCount;
    private BigDecimal avgOrderAmount;
}
