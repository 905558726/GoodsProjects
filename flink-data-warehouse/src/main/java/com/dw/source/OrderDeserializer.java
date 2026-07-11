package com.dw.source;

import com.dw.model.OrderEvent;
import com.dw.util.JsonUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Kafka JSON message deserializer for OrderEvent
 */
public class OrderDeserializer implements KafkaRecordDeserializationSchema<OrderEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderDeserializer.class);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<OrderEvent> out) throws IOException {
        byte[] message = record.value();
        if (message == null || message.length == 0) {
            return;
        }
        try {
            OrderEvent event = JsonUtils.getMapper().readValue(message, OrderEvent.class);
            if (event != null) {
                out.collect(event);
            }
        } catch (Exception e) {
            LOG.error("Failed to deserialize OrderEvent: {}", new String(message), e);
        }
    }

    @Override
    public TypeInformation<OrderEvent> getProducedType() {
        return TypeInformation.of(OrderEvent.class);
    }
}
