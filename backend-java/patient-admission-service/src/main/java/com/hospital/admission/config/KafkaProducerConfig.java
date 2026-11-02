package com.hospital.admission.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Apache Kafka Tracing Configuration (Spring Kafka 3.2+ / Spring Boot 3.4.0)
 *
 * Enabling observation on KafkaTemplate instructs Spring Kafka to automatically
 * serialize the current W3C traceparent into Kafka RecordHeaders before publishing,
 * bridging synchronous HTTP traces into the asynchronous event stream.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
        // CRITICAL: Propagates W3C traceparent into Kafka RecordHeaders
        template.setObservationEnabled(true);
        return template;
    }
}
