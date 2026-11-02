package com.hospital.admission.controller;

import com.hospital.admission.model.AdmissionRequest;
import com.hospital.admission.model.AdmissionResponse;
import com.hospital.admission.service.AdmissionService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping
public class AdmissionController {

    private static final Logger log = LoggerFactory.getLogger(AdmissionController.class);
    private final AdmissionService admissionService;

    public AdmissionController(AdmissionService admissionService) {
        this.admissionService = admissionService;
    }

    @PostMapping({
            "/api/v1/admissions",
            "/api/v1/er/triage-admissions",
            "/api/gateway/er/admissions",
            "/api/api/gateway/er/admissions"
    })
    public ResponseEntity<AdmissionResponse> createAdmission(
            @Valid @RequestBody AdmissionRequest request,
            @RequestHeader(value = "traceparent", required = false) String traceparent,
            @RequestHeader(value = "X-Correlation-ID", required = false) String correlationId) {

        log.info("Incoming admission request. Traceparent header: [{}], CorrelationId header: [{}]",
                traceparent, correlationId);

        AdmissionResponse response = admissionService.processAdmission(request);
        return ResponseEntity.ok(response);
    }
}
