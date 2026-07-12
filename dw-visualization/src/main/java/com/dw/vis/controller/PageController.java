package com.dw.vis.controller;

import com.dw.vis.service.DashboardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class PageController {

    @Autowired private DashboardService dashboardService;

    @GetMapping("/")
    public String index(Model model) {
        model.addAttribute("kpi", dashboardService.getDashboardKpi());
        model.addAttribute("categories", dashboardService.getCategoryRevenue());
        return "index";
    }

    @GetMapping("/category-revenue")
    public String categoryRevenue(Model model) {
        model.addAttribute("categories", dashboardService.getCategoryRevenue());
        return "category-revenue";
    }

    @GetMapping("/daily-sales-trend")
    public String dailySalesTrend() {
        return "daily-sales-trend";
    }

    @GetMapping("/user-value")
    public String userValue() {
        return "user-value";
    }

    @GetMapping("/product-ranking")
    public String productRanking() {
        return "product-ranking";
    }

    @GetMapping("/regional-sales")
    public String regionalSales() {
        return "regional-sales";
    }
}
