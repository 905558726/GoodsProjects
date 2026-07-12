package com.dw.vis.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.dw.vis.entity.*;
import com.dw.vis.mapper.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DashboardService {

    @Autowired private CategoryRevenueMapper  categoryRevenueMapper;
    @Autowired private DailySalesTrendMapper  dailySalesTrendMapper;
    @Autowired private UserValueMapper        userValueMapper;
    @Autowired private ProductRankingMapper   productRankingMapper;
    @Autowired private RegionalSalesMapper    regionalSalesMapper;

    // ---- 品类营收 ----
    public List<CategoryRevenue> getCategoryRevenue() {
        return categoryRevenueMapper.selectList(
                new QueryWrapper<CategoryRevenue>().orderByAsc("rank"));
    }

    // ---- 每日销售趋势 ----
    public List<DailySalesTrend> getDailySalesTrend(String startDate, String endDate) {
        if (startDate == null || startDate.isEmpty()) startDate = "2026-01-01";
        if (endDate == null || endDate.isEmpty()) endDate = "2099-01-01";
        QueryWrapper<DailySalesTrend> qw = new QueryWrapper<>();
        qw.apply("dt >= {0}::date", startDate);
        qw.apply("dt <= {0}::date", endDate);
        qw.orderByAsc("dt");
        return dailySalesTrendMapper.selectList(qw);
    }

    // ---- 用户价值 ----
    public List<UserValue> getUserValue(String tier) {
        QueryWrapper<UserValue> qw = new QueryWrapper<>();
        if (tier != null && !tier.isEmpty()) qw.eq("value_tier", tier);
        qw.orderByDesc("total_monetary");
        return userValueMapper.selectList(qw);
    }

    public Map<String, Long> getUserTierDistribution() {
        List<UserValue> all = userValueMapper.selectList(null);
        return all.stream().collect(Collectors.groupingBy(UserValue::getValueTier, Collectors.counting()));
    }

    // ---- 商品排行 ----
    public List<ProductRanking> getProductRanking(Integer topN, String category) {
        QueryWrapper<ProductRanking> qw = new QueryWrapper<>();
        if (category != null && !category.isEmpty()) qw.eq("category", category);
        qw.orderByAsc("rank");
        if (topN != null && topN > 0) qw.last("LIMIT " + topN);
        return productRankingMapper.selectList(qw);
    }

    // ---- 区域销售 ----
    public List<RegionalSales> getRegionalSales(String level) {
        QueryWrapper<RegionalSales> qw = new QueryWrapper<>();
        if ("province".equals(level)) {
            qw.eq("city", "合计");
        } else if ("city".equals(level)) {
            qw.ne("city", "合计").eq("district", "合计");
        }
        qw.orderByDesc("total_amount");
        return regionalSalesMapper.selectList(qw);
    }

    // ---- 仪表盘 KPI ----
    public DashboardKpi getDashboardKpi() {
        DashboardKpi kpi = new DashboardKpi();
        // 总销售额 — 取所有 daily_sales_trend 的 totalSalesAmount 求和
        List<DailySalesTrend> trends = dailySalesTrendMapper.selectList(null);
        BigDecimal salesSum = BigDecimal.ZERO;
        long orderSum = 0L;
        for (DailySalesTrend t : trends) {
            if (t.getTotalSalesAmount() != null) salesSum = salesSum.add(t.getTotalSalesAmount());
            if (t.getTotalOrderCount() != null) orderSum += t.getTotalOrderCount();
        }
        kpi.setTotalSalesAmount(salesSum);
        kpi.setTotalOrderCount(orderSum);
        // 活跃用户
        kpi.setActiveUserCount(userValueMapper.selectCount(null));
        // 动销 SKU
        kpi.setActiveSkuCount(productRankingMapper.selectCount(null));
        return kpi;
    }
}
