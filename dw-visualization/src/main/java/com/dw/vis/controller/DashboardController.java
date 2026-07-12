package com.dw.vis.controller;

import com.dw.vis.entity.*;
import com.dw.vis.service.DashboardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ads")
public class DashboardController {

    @Autowired private DashboardService dashboardService;

    @GetMapping("/category-revenue")
    public List<CategoryRevenue> getCategoryRevenue() {
        return dashboardService.getCategoryRevenue();
    }

    @GetMapping("/daily-sales-trend")
    public List<DailySalesTrend> getDailySalesTrend(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {
        return dashboardService.getDailySalesTrend(startDate, endDate);
    }

    @GetMapping("/user-value")
    public List<UserValue> getUserValue(@RequestParam(required = false) String valueTier) {
        return dashboardService.getUserValue(valueTier);
    }

    @GetMapping("/user-value-distribution")
    public Map<String, Long> getUserValueDistribution() {
        return dashboardService.getUserTierDistribution();
    }

    @GetMapping("/product-ranking")
    public List<ProductRanking> getProductRanking(
            @RequestParam(defaultValue = "10") Integer topN,
            @RequestParam(required = false) String category) {
        return dashboardService.getProductRanking(topN, category);
    }

    @GetMapping("/regional-sales")
    public List<RegionalSales> getRegionalSales(@RequestParam(defaultValue = "province") String level) {
        return dashboardService.getRegionalSales(level);
    }

    @GetMapping("/dashboard-kpi")
    public DashboardKpi getDashboardKpi() {
        return dashboardService.getDashboardKpi();
    }
}
