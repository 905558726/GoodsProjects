package com.dw.vis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;
import java.util.Date;

@Data
@TableName("ads.ads_user_value")
public class UserValue {
    private String     buyerPhone;
    private String     buyerName;
    private java.sql.Date lastOrderDate;
    private Long       orderFrequency;
    private BigDecimal totalMonetary;
    private BigDecimal avgOrderAmount;
    private String     valueTier;
}
