package com.adipta.batchkafka.audit;

import org.springframework.stereotype.Service;

@Service
public class AuditService {

    private final ProcessedFileAuditRepository repository;

    public AuditService(ProcessedFileAuditRepository repository) {
        this.repository = repository;
    }

    public void recordJobRun(String jobName, String direction, String fileName,
                             Long rowsProcessed, String status) {
        ProcessedFileAudit audit = new ProcessedFileAudit(
                jobName, direction, fileName, rowsProcessed, status);
        repository.save(audit);
    }
}