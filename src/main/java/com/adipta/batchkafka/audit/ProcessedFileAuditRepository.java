package com.adipta.batchkafka.audit;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedFileAuditRepository extends JpaRepository<ProcessedFileAudit, Long> {
}