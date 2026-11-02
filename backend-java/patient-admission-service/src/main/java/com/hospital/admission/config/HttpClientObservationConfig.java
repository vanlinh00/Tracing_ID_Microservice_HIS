package com.hospital.admission.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

/**
 * Spring Boot 3.4.0 Inter-service HTTP Client Configuration.
 *
 * CRITICAL ARCHITECTURAL PRINCIPLE:
 * In Spring Boot 3.4, never construct RestClient using RestClient.create().
 * Always autowire and use Spring's provided RestClient.Builder!
 *
 * Spring automatically binds the ObservationRegistry into the builder, ensuring
 * that the outbound request automatically carries the active W3C 'traceparent' header
 * with the correct child span ID to downstream internal microservices.
 */
@Configuration(proxyBeanMethods = false)
public class HttpClientObservationConfig {

    @Value("${clinical.chart-service.url:http://localhost:8082}")
    private String chartServiceUrl;

    @Bean
    public RestClient clinicalChartRestClient(RestClient.Builder builder) {
        return builder
                .baseUrl(chartServiceUrl)
                .build();
    }
}
