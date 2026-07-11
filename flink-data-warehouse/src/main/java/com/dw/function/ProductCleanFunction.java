package com.dw.function;

import com.dw.model.DwdProduct;
import com.dw.model.ProductEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * 商品数据清洗：ProductEvent → DwdProduct
 *
 * 清洗规则：
 * - price 保留 2 位小数，负数修正为 0.00 并标记异常
 * - created_at 字符串标准化为 Timestamp
 * - keywords 数组转为逗号分隔字符串
 * - NULL 字段填充默认值
 */
public class ProductCleanFunction extends RichMapFunction<ProductEvent, DwdProduct> {

    private static final Logger LOG = LoggerFactory.getLogger(ProductCleanFunction.class);

    private static final DateTimeFormatter[] FORMATTERS = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    @Override
    public DwdProduct map(ProductEvent event) throws Exception {
        DwdProduct product = new DwdProduct();

        // 1. productId
        product.setProductId(nullToDefault(event.getProductId(), "UNKNOWN"));

        // 2. name
        product.setName(nullToDefault(event.getName(), "未知商品"));

        // 3. brand
        product.setBrand(nullToDefault(event.getBrand(), ""));

        // 4. category
        product.setCategory(nullToDefault(event.getCategory(), "其他"));

        // 5. subCategory
        product.setSubCategory(nullToDefault(event.getSubCategory(), ""));

        // 6. price — 校验与修正
        BigDecimal price = event.getPrice();
        boolean abnormal = false;
        if (price == null) {
            price = BigDecimal.ZERO;
            abnormal = true;
        } else if (price.compareTo(BigDecimal.ZERO) < 0) {
            LOG.warn("Product {} price is negative ({}), fix to 0.00", event.getProductId(), price);
            price = BigDecimal.ZERO;
            abnormal = true;
        }
        product.setPrice(price.setScale(2, java.math.RoundingMode.HALF_UP));

        // 7. description
        product.setDescription(nullToDefault(event.getDescription(), ""));

        // 8. imageUrl
        product.setImageUrl(nullToDefault(event.getImageUrl(), ""));

        // 9. keywords
        List<String> kw = event.getKeywords();
        product.setKeywords(kw != null && !kw.isEmpty() ? String.join(",", kw) : "");

        // 10. created_at — 多格式解析
        product.setCreatedAt(parseTimestamp(event.getCreatedAt()));

        // 11. isVariant
        Boolean isVariant = event.getIsVariant();
        product.setIsVariant(isVariant != null && isVariant);

        // 12. 异常标记
        product.setIsAbnormal(abnormal);

        // 13. loadTime
        product.setLoadTime(new Timestamp(System.currentTimeMillis()));

        return product;
    }

    private String nullToDefault(String val, String def) {
        return val == null || val.isEmpty() ? def : val;
    }

    private Timestamp parseTimestamp(String dateStr) {
        if (dateStr == null || dateStr.isEmpty()) {
            return Timestamp.valueOf(LocalDateTime.now());
        }
        for (DateTimeFormatter fmt : FORMATTERS) {
            try {
                return Timestamp.valueOf(LocalDateTime.parse(dateStr, fmt));
            } catch (DateTimeParseException ignored) {}
        }
        // fallback
        LOG.warn("Unable to parse date: {}, using current time", dateStr);
        return Timestamp.valueOf(LocalDateTime.now());
    }
}
