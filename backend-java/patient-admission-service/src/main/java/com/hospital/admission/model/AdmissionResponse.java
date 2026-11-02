package com.hospital.admission.model;

public class AdmissionResponse {
    private String encounterId;
    private String patientHash;
    private String assignedWard;
    private String status;

    public AdmissionResponse(String encounterId, String patientHash, String assignedWard, String status) {
        this.encounterId = encounterId;
        this.patientHash = patientHash;
        this.assignedWard = assignedWard;
        this.status = status;
    }

    public String getEncounterId() { return encounterId; }
    public String getPatientHash() { return patientHash; }
    public String getAssignedWard() { return assignedWard; }
    public String getStatus() { return status; }
}
