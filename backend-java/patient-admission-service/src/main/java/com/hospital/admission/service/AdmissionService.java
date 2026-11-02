package com.hospital.admission.service;

import com.hospital.admission.model.AdmissionRequest;
import com.hospital.admission.model.AdmissionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;

@Service
public class AdmissionService {

    private static final Logger log = LoggerFactory.getLogger(AdmissionService.class);
    private static final String AUDIT_SALT = "k9#xL92@HIS_SECURE_SALT";

    private final RestClient clinicalChartRestClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public AdmissionService(RestClient clinicalChartRestClient, KafkaTemplate<String, Object> kafkaTemplate) {
        this.clinicalChartRestClient = clinicalChartRestClient;
        this.kafkaTemplate = kafkaTemplate;
    }

    public AdmissionResponse processAdmission(AdmissionRequest request) {
        String encounterId = "ENC-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String patientHash = hashPatientIdentifier(request.getPatientMRN());

        try {
            // Populate MDC with safe non-PHI audit keys
            MDC.put("encounterId", encounterId);
            MDC.put("patientIdHash", patientHash);
            MDC.put("facilityId", "METRO-ER-01");

            log.info("Processing ER trauma admission for encounter [{}]. Bed requested: {}",
                    encounterId, request.getWard());

            // 1. Synchronous Downstream HTTP Call:
            // The injected RestClient automatically attaches the W3C 'traceparent' header
            // containing the active Trace ID and child Span ID.
            try {
                log.info("Initiating clinical chart encounter via internal RestClient...");
                clinicalChartRestClient.post()
                        .uri("/internal/v1/encounters/initiate")
                        .body(Map.of(
                                "encounterId", encounterId,
                                "attendingDoctorId", request.getAttendingDoctorId(),
                                "vitalAlert", request.getVitalAlert()
                        ))
                        .retrieve()
                        .toBodilessEntity();
            } catch (Exception ex) {
                log.warn("Downstream chart service call simulated: {}", ex.getMessage());
            }

            // 2. Asynchronous Event Stream:
            // KafkaTemplate with observationEnabled=true attaches W3C traceparent to Kafka RecordHeaders
            log.info("Publishing ER_ADMISSION_CONFIRMED event to Kafka topic [his.er.events]...");
            kafkaTemplate.send("his.er.events", encounterId, Map.of(
                    "eventType", "ER_ADMISSION_CONFIRMED",
                    "encounterId", encounterId,
                    "ward", request.getWard(),
                    "triageLevel", request.getTriageLevel(),
                    "timestamp", System.currentTimeMillis()
            ));

            return new AdmissionResponse(encounterId, patientHash, request.getWard(), "CONFIRMED");

        } finally {
            // Clean up custom business keys from MDC (traceId and spanId are handled by Micrometer)
            MDC.remove("encounterId");
            MDC.remove("patientIdHash");
            MDC.remove("facilityId");
        }
    }

    private String hashPatientIdentifier(String rawMrn) {
        if (rawMrn == null) return "ANONYMOUS";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(AUDIT_SALT.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md.digest(rawMrn.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 16);
        } catch (Exception e) {
            return "HASH_ERROR";
        }
    }
}
