package com.adipta.batchkafka.tasklet;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;

import java.io.File;

/**
 * Validates that the source file exists and is readable before the
 * chunk step runs. Path is injected as a late-binding job parameter
 * so this single tasklet can be reused across all jobs.
 */
@Slf4j
public class FileValidationTasklet implements Tasklet {

    private final String inputFilePath;

    public FileValidationTasklet(String inputFilePath) {
        this.inputFilePath = inputFilePath;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        File file = new File(inputFilePath);

        if (!file.exists()) {
            throw new IllegalStateException("Input file not found: " + inputFilePath);
        }
        if (!file.canRead()) {
            throw new IllegalStateException("Input file not readable: " + inputFilePath);
        }
        if (file.length() == 0) {
            throw new IllegalStateException("Input file is empty: " + inputFilePath);
        }

        chunkContext.getStepContext().getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .putString("validatedFilePath", inputFilePath);

        log.info("[VALIDATE] OK: {} ({} bytes)", inputFilePath, file.length());
        return RepeatStatus.FINISHED;
    }
}