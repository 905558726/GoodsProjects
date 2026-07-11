package com.dw.source;

import com.dw.model.ProductEvent;
import com.dw.util.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Kafka JSON message deserializer for ProductEvent
 */
public class ProductDeserializer implements KafkaRecordDeserializationSchema<ProductEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ProductDeserializer.class);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ProductEvent> out) throws IOException {
        byte[] message = record.value();
        if (message == null || message.length == 0) {
            return;
        }
        try {
            ProductEvent event = JsonUtils.getMapper().readValue(message, ProductEvent.class);
            if (event != null) {
                out.collect(event);
            }
        } catch (Exception e) {
            LOG.error("Failed to deserialize ProductEvent: {}", new String(message), e);
        }
    }

    @Override
    public TypeInformation<ProductEvent> getProducedType() {
        return TypeInformation.of(ProductEvent.class);
    }
}
