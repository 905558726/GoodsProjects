package com.dw.vis.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * SPA 路由控制器 —— 所有页面请求统一返回 index.html
 * 客户端 JS 根据 location.pathname 自动匹配对应页面 (纯前端路由)
 */
@Controller
public class PageController {

    @GetMapping("/")
    public String index() { return "index"; }

    @GetMapping("/category-revenue")
    public String categoryRevenue() { return "index"; }

    @GetMapping("/daily-sales-trend")
    public String dailySalesTrend() { return "index"; }

    @GetMapping("/user-value")
    public String userValue() { return "index"; }

    @GetMapping("/product-ranking")
    public String productRanking() { return "index"; }

    @GetMapping("/regional-sales")
    public String regionalSales() { return "index"; }

    @GetMapping("/goods-info")
    public String goodsInfo() { return "index"; }
}
