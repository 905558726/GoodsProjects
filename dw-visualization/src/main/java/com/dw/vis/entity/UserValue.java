package com.dw.vis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;

@Data
@TableName(value = "ads.ads_user_value", autoResultMap = true)
public class UserValue {
    private String     buyerPhone;
    private String     buyerName;
    private java.sql.Date lastOrderDate;
    private Long       orderFrequency;
    private BigDecimal totalMonetary;
    private BigDecimal avgOrderAmount;
    private String     valueTier;
}
