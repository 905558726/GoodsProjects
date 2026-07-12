package com.dw.vis.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DashboardKpi {
    private BigDecimal totalSalesAmount;
    private Long       totalOrderCount;
    private Long       activeUserCount;
    private Long       activeSkuCount;
}
