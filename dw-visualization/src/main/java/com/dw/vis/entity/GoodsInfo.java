package com.dw.vis.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

@Data
@TableName(value = "dim.dim_goods_info", autoResultMap = true)
public class GoodsInfo {
    @TableId("sku_id")
    private String     skuId;
    private String     productId;
    private String     spuName;
    private String     skuName;
    private String     brandName;
    private String     firstCategoryName;
    private String     secondCategoryName;
    private BigDecimal priceMin;
    private BigDecimal priceMax;
    private BigDecimal skuPrice;
    private BigDecimal priceDelta;
    private String     variantSuffix;
    private String     imageUrl;
    // keywords is ARRAY type, use String for simplicity
    private String     keywords;
    private Date       startDate;
    private Date       endDate;
    private Integer    isActive;
    private Timestamp  dwCreateTime;
    private Timestamp  dwUpdateTime;
}
