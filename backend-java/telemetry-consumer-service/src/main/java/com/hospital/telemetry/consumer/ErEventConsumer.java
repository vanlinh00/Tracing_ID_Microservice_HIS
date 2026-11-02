package com.hospital.telemetry.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class ErEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ErEventConsumer.class);

    /**
     * Because the listener container has observation enabled:
     * 1. Spring Kafka extracts the W3C traceparent from Kafka RecordHeaders.
     * 2. It creates a child consumer span attached to the original producer's Trace ID.
     * 3. SLF4J MDC automatically reflects the same traceId in the log output!
     */
    @KafkaListener(topics = "his.er.events", groupId = "bedside-telemetry-group")
    public void consumeErAdmission(
            @Payload Map<String, Object> event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(value = "traceparent", required = false) String traceparentHeader) {

        log.info("Received Kafka Event on Topic [{}] Partition [{}]. Inbound header traceparent: {}",
                topic, partition, traceparentHeader);
        log.info("Bedside Telemetry Monitor synced for Encounter: {}. Trauma Bay: {}",
                event.get("encounterId"), event.get("ward"));
    }
}
