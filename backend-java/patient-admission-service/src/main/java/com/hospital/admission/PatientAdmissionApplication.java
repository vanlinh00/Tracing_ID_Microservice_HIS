package com.hospital.admission;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class PatientAdmissionApplication {
    public static void main(String[] args) {
        SpringApplication.run(PatientAdmissionApplication.class, args);
    }
}
