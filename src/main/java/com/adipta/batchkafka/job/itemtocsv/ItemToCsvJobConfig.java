package com.adipta.batchkafka.job.itemtocsv;

import com.adipta.batchkafka.audit.AuditService;
import com.adipta.batchkafka.tasklet.ArchiveCleanupTasklet;
import com.adipta.batchkafka.tasklet.FileValidationTasklet;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ItemToCsvJobConfig {

    private static final String JOB_NAME = "itemToCsvJob";
    private static final String DIRECTION = "ITEM_TO_CSV";
    private static final String OUTPUT_PATH = "output/item.csv";

    @Bean
    @StepScope
    public FileValidationTasklet itemFileValidationTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath) {
        return new FileValidationTasklet(inputFilePath);
    }

    @Bean
    public Step validateItemFileStep(JobRepository jobRepository,
                                     FileValidationTasklet itemFileValidationTasklet) {
        return new StepBuilder("validateItemFileStep", jobRepository)
                .tasklet(itemFileValidationTasklet)
                .build();
    }

    @Bean
    @StepScope
    public ItemJsonToCsvTasklet itemJsonToCsvTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath) {
        return new ItemJsonToCsvTasklet(inputFilePath, OUTPUT_PATH);
    }

    @Bean
    public Step transformItemToCsvStep(JobRepository jobRepository,
                                       ItemJsonToCsvTasklet itemJsonToCsvTasklet) {
        return new StepBuilder("transformItemToCsvStep", jobRepository)
                .tasklet(itemJsonToCsvTasklet)
                .build();
    }

    @Bean
    @StepScope
    public ArchiveCleanupTasklet itemArchiveCleanupTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath,
            AuditService auditService) {
        return new ArchiveCleanupTasklet(inputFilePath, JOB_NAME, DIRECTION, auditService);
    }

    @Bean
    public Step archiveItemFileStep(JobRepository jobRepository,
                                    ArchiveCleanupTasklet itemArchiveCleanupTasklet) {
        return new StepBuilder("archiveItemFileStep", jobRepository)
                .tasklet(itemArchiveCleanupTasklet)
                .build();
    }

    @Bean
    public Job itemToCsvJob(JobRepository jobRepository,
                            Step validateItemFileStep,
                            Step transformItemToCsvStep,
                            Step archiveItemFileStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(validateItemFileStep)
                .next(transformItemToCsvStep)
                .next(archiveItemFileStep)
                .build();
    }
}