package com.adipta.batchkafka.audit;

import org.springframework.stereotype.Service;

import java.time.Clock;

@Service
public class AuditService {

    private final ProcessedFileAuditRepository repository;
    private final Clock clock;

    public AuditService(ProcessedFileAuditRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    public void recordJobRun(String jobName, String direction, String fileName,
                             Long rowsProcessed, String status) {
        ProcessedFileAudit audit = new ProcessedFileAudit(
                jobName, direction, fileName, rowsProcessed, status, clock);
        repository.save(audit);
    }
}