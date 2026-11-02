package com.hospital.common;

import org.slf4j.MDC;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Enterprise Healthcare Audit Utility:
 * Implements HIPAA Security Rule § 164.312(b) Audit Controls
 * without logging Protected Health Information (PHI) in plaintext.
 */
public final class AuditCorrelationHelper {

    private static final String DEFAULT_SALT = "k9#xL92@HIS_SECURE_SALT";

    private AuditCorrelationHelper() {}

    /**
     * Set pseudonymized encounter context into MDC for cross-service logging.
     */
    public static void setEncounterAuditContext(String encounterId, String rawMrn, String facilityId) {
        if (encounterId != null) MDC.put("encounterId", encounterId);
        if (facilityId != null) MDC.put("facilityId", facilityId);
        if (rawMrn != null) {
            MDC.put("patientIdHash", hashPatientIdentifier(rawMrn));
        }
    }

    /**
     * Clear custom MDC keys to avoid leaking into pooled threads.
     */
    public static void clearAuditContext() {
        MDC.remove("encounterId");
        MDC.remove("facilityId");
        MDC.remove("patientIdHash");
    }

    /**
     * Generates a deterministic 16-hex salted hash for patient correlation.
     * Compliant with HIPAA Safe Harbor and expert determination methods.
     */
    public static String hashPatientIdentifier(String rawIdentifier) {
        if (rawIdentifier == null || rawIdentifier.isBlank()) return "ANONYMOUS";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(DEFAULT_SALT.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md.digest(rawIdentifier.trim().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 16);
        } catch (NoSuchAlgorithmException e) {
            return "HASH_ERROR";
        }
    }
}
