package com.adipta.batchkafka.audit;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_files")
@Getter
@Setter
@NoArgsConstructor
public class ProcessedFileAudit {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_name", nullable = false)
    private String jobName;

    @Column(name = "direction", nullable = false)
    private String direction; // TEXT_TO_CSV | CSV_TO_TEXT | TEXT_TO_DB

    @Column(name = "file_name", nullable = false)
    private String fileName;

    @Column(name = "rows_processed")
    private Long rowsProcessed;

    @Column(name = "status", nullable = false)
    private String status; // SUCCESS | ARCHIVE_FAILED | FAILED

    @Column(name = "run_at", nullable = false)
    private LocalDateTime runAt;

    public ProcessedFileAudit(String jobName, String direction, String fileName, Long rowsProcessed, String status) {
        this.jobName = jobName;
        this.direction = direction;
        this.fileName = fileName;
        this.rowsProcessed = rowsProcessed;
        this.status = status;
        this.runAt = LocalDateTime.now();
    }
}