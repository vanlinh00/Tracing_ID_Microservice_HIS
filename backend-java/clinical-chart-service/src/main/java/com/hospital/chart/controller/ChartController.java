package com.hospital.chart.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/internal/v1/encounters")
public class ChartController {

    private static final Logger log = LoggerFactory.getLogger(ChartController.class);

    @PostMapping("/initiate")
    public ResponseEntity<Map<String, Object>> initiateEncounter(
            @RequestBody Map<String, Object> payload,
            @RequestHeader(value = "traceparent", required = false) String traceparent) {

        // Notice that traceparent is automatically parsed into MDC by Spring Boot 3.4
        // The logger will automatically print [clinical-chart-service, <traceId>, <spanId>]
        log.info("Received internal encounter initiation request. Active traceparent: {}", traceparent);
        log.info("Chart encounter successfully initialized for encounter: {}", payload.get("encounterId"));

        return ResponseEntity.ok(Map.of(
                "status", "CHART_INITIALIZED",
                "encounterId", payload.get("encounterId"),
                "timestamp", System.currentTimeMillis()
        ));
    }
}
