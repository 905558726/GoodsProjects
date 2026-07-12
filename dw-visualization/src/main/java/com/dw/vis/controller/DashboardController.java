package com.dw.vis.controller;

import com.dw.vis.entity.*;
import com.dw.vis.service.DashboardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/ads")
public class DashboardController {

    @Autowired private DashboardService dashboardService;

    @GetMapping("/category-revenue")
    public List<CategoryRevenue> getCategoryRevenue() { return dashboardService.getCategoryRevenue(); }

    @GetMapping("/daily-sales-trend")
    public List<DailySalesTrend> getDailySalesTrend(
            @RequestParam(name = "startDate", required = false) String startDate,
            @RequestParam(name = "endDate", required = false) String endDate) {
        return dashboardService.getDailySalesTrend(startDate, endDate);
    }

    @GetMapping("/user-value")
    public List<UserValue> getUserValue(@RequestParam(name = "valueTier", required = false) String valueTier) {
        return dashboardService.getUserValue(valueTier);
    }

    @GetMapping("/user-value-distribution")
    public Map<String, Long> getUserValueDistribution() { return dashboardService.getUserTierDistribution(); }

    @GetMapping("/product-ranking")
    public List<ProductRanking> getProductRanking(
            @RequestParam(name = "topN", defaultValue = "10") Integer topN,
            @RequestParam(name = "category", required = false) String category) {
        return dashboardService.getProductRanking(topN, category);
    }

    @GetMapping("/regional-sales")
    public List<RegionalSales> getRegionalSales(@RequestParam(name = "level", defaultValue = "province") String level) {
        return dashboardService.getRegionalSales(level);
    }

    @GetMapping("/dashboard-kpi")
    public DashboardKpi getDashboardKpi() { return dashboardService.getDashboardKpi(); }

    // ---- 商品库 ----
    @GetMapping("/goods-info")
    public Map<String, Object> getGoodsInfo(
            @RequestParam(name = "category", required = false) String category,
            @RequestParam(name = "brand", required = false) String brand,
            @RequestParam(name = "keyword", required = false) String keyword,
            @RequestParam(name = "page", defaultValue = "1") Integer page,
            @RequestParam(name = "size", defaultValue = "20") Integer size) {
        Map<String, Object> result = new HashMap<>();
        result.put("data", dashboardService.getGoodsInfo(category, brand, keyword, page, size));
        result.put("total", dashboardService.getGoodsInfoCount(category, brand, keyword));
        result.put("categories", dashboardService.getGoodsCategories());
        result.put("brands", dashboardService.getGoodsBrands());
        return result;
    }
}
