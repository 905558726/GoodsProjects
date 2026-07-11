package com.dw.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

/**
 * Kafka ods_orders_data Topic 订单消息
 * 字段映射 Python order_generator.py 输出 JSON
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderEvent {

    @JsonProperty("order_id")
    private String orderId;

    private List<OrderItem> items;

    @JsonProperty("item_count")
    private Integer itemCount;

    private Buyer buyer;

    @JsonProperty("shipping_address")
    private ShippingAddress shippingAddress;

    @JsonProperty("order_status")
    private String orderStatus;

    private Payment payment;

    private Logistics logistics;

    @JsonProperty("order_amount")
    private BigDecimal orderAmount;

    private BigDecimal discount;

    @JsonProperty("actual_amount")
    private BigDecimal actualAmount;

    @JsonProperty("created_at")
    private String createdAt;

    private String remark;

    // ---- 嵌套类 ----

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OrderItem {
        @JsonProperty("product_id")
        private String productId;

        @JsonProperty("product_name")
        private String productName;

        private String brand;
        private String category;

        @JsonProperty("sub_category")
        private String subCategory;

        @JsonProperty("original_price")
        private BigDecimal originalPrice;

        private BigDecimal price;
        private Integer quantity;

        @JsonProperty("sales_volume")
        private Integer salesVolume;

        private BigDecimal subtotal;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Buyer {
        private String name;
        private String phone;
        private String email;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShippingAddress {
        private String province;
        private String city;
        private String district;
        private String detail;

        @JsonProperty("postal_code")
        private String postalCode;

        @JsonProperty("recipient_name")
        private String recipientName;

        @JsonProperty("recipient_phone")
        private String recipientPhone;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payment {
        private String method;
        private BigDecimal amount;

        @JsonProperty("paid_at")
        private String paidAt;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Logistics {
        private String company;

        @JsonProperty("tracking_number")
        private String trackingNumber;

        @JsonProperty("shipped_at")
        private String shippedAt;

        @JsonProperty("estimated_delivery")
        private String estimatedDelivery;
    }
}
