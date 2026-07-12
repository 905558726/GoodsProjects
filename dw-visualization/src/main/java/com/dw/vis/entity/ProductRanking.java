package com.dw.vis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;

@Data
@TableName("ads.ads_product_ranking")
public class ProductRanking {
    private String     productId;
    private String     productName;
    private String     category;
    private String     brand;
    private Long       totalQuantity;
    private BigDecimal totalAmount;
    private Integer    rank;
}
