package com.hospital.admission.model;

public class AdmissionRequest {
    private String patientMRN;
    private String triageLevel;
    private String vitalAlert;
    private String ward;
    private String attendingDoctorId;

    public String getPatientMRN() { return patientMRN; }
    public void setPatientMRN(String patientMRN) { this.patientMRN = patientMRN; }

    public String getTriageLevel() { return triageLevel; }
    public void setTriageLevel(String triageLevel) { this.triageLevel = triageLevel; }

    public String getVitalAlert() { return vitalAlert; }
    public void setVitalAlert(String vitalAlert) { this.vitalAlert = vitalAlert; }

    public String getWard() { return ward; }
    public void setWard(String ward) { this.ward = ward; }

    public String getAttendingDoctorId() { return attendingDoctorId; }
    public void setAttendingDoctorId(String attendingDoctorId) { this.attendingDoctorId = attendingDoctorId; }
}
