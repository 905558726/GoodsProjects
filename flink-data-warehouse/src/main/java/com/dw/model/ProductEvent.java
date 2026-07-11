package com.dw.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Kafka ods_products_data Topic 商品消息
 * 字段映射 Python product_generator.py 输出 JSON
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductEvent {

    @JsonProperty("product_id")
    private String productId;

    private String name;

    private String brand;

    private String category;

    @JsonProperty("sub_category")
    private String subCategory;

    private BigDecimal price;

    private String description;

    @JsonProperty("image_url")
    private String imageUrl;

    private List<String> keywords;

    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("is_variant")
    private Boolean isVariant;
}
