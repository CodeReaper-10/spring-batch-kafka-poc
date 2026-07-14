package com.adipta.batchkafka.tasklet;

import com.adipta.batchkafka.audit.AuditService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Archives (moves) the source file to a "processed" folder and writes
 * a row to the shared audit table. jobName/direction are supplied per
 * job so the audit row is meaningful for all three job variants.
 */
@Slf4j
public class ArchiveCleanupTasklet implements Tasklet {

    private final String inputFilePath;
    private final String jobName;
    private final String direction; // "TEXT_TO_CSV" | "CSV_TO_TEXT" | "ITEM_TO_CSV" | "CONTROL_ITEM_TO_CSV"
    private final AuditService auditService;

    public ArchiveCleanupTasklet(String inputFilePath, String jobName,
                                 String direction, AuditService auditService) {
        this.inputFilePath = inputFilePath;
        this.jobName = jobName;
        this.direction = direction;
        this.auditService = auditService;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        File source = new File(inputFilePath);
        String status = "SUCCESS";

        Long rowsProcessed = chunkContext.getStepContext().getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .containsKey("rowsProcessed")
                ? chunkContext.getStepContext().getStepExecution()
                .getJobExecution().getExecutionContext().getLong("rowsProcessed")
                : 0L;

        try {
            Path archiveDir = Path.of(source.getParent(), "processed");
            Files.createDirectories(archiveDir);
            Path target = archiveDir.resolve(source.getName());
            Files.move(source.toPath(), target, StandardCopyOption.REPLACE_EXISTING);
            log.info("[ARCHIVE] Moved {} -> {}", inputFilePath, target);
        } catch (Exception e) {
            status = "ARCHIVE_FAILED";
            contribution.setExitStatus(ExitStatus.FAILED);
            log.error("[ARCHIVE] Failed to archive {}: {}", inputFilePath, e.getMessage(), e);
        }

        auditService.recordJobRun(jobName, direction, source.getName(), rowsProcessed, status);
        return RepeatStatus.FINISHED;
    }
}