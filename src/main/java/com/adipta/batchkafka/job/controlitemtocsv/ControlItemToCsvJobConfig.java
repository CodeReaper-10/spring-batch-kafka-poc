package com.adipta.batchkafka.job.controlitemtocsv;

import com.adipta.batchkafka.audit.AuditService;
import com.adipta.batchkafka.tasklet.ArchiveCleanupTasklet;
import com.adipta.batchkafka.tasklet.FileValidationTasklet;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ControlItemToCsvJobConfig {

    private static final String JOB_NAME = "controlItemToCsvJob";
    private static final String DIRECTION = "CONTROL_ITEM_TO_CSV";
    private static final String CONTROL_ITEM_OUTPUT_PATH = "output/control_item.csv";
    private static final String STORES_OUTPUT_PATH = "output/control_item_stores.csv";

    @Bean
    @StepScope
    public FileValidationTasklet controlItemFileValidationTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath) {
        return new FileValidationTasklet(inputFilePath);
    }

    @Bean
    public Step validateControlItemFileStep(JobRepository jobRepository,
                                            FileValidationTasklet controlItemFileValidationTasklet) {
        return new StepBuilder("validateControlItemFileStep", jobRepository)
                .tasklet(controlItemFileValidationTasklet)
                .build();
    }

    @Bean
    @StepScope
    public ControlItemJsonToCsvTasklet controlItemJsonToCsvTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath) {
        return new ControlItemJsonToCsvTasklet(inputFilePath, CONTROL_ITEM_OUTPUT_PATH, STORES_OUTPUT_PATH);
    }

    @Bean
    public Step transformControlItemToCsvStep(JobRepository jobRepository,
                                              ControlItemJsonToCsvTasklet controlItemJsonToCsvTasklet) {
        return new StepBuilder("transformControlItemToCsvStep", jobRepository)
                .tasklet(controlItemJsonToCsvTasklet)
                .build();
    }

    @Bean
    @StepScope
    public ArchiveCleanupTasklet controlItemArchiveCleanupTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath,
            AuditService auditService) {
        return new ArchiveCleanupTasklet(inputFilePath, JOB_NAME, DIRECTION, auditService);
    }

    @Bean
    public Step archiveControlItemFileStep(JobRepository jobRepository,
                                           ArchiveCleanupTasklet controlItemArchiveCleanupTasklet) {
        return new StepBuilder("archiveControlItemFileStep", jobRepository)
                .tasklet(controlItemArchiveCleanupTasklet)
                .build();
    }

    @Bean
    public Job controlItemToCsvJob(JobRepository jobRepository,
                                   Step validateControlItemFileStep,
                                   Step transformControlItemToCsvStep,
                                   Step archiveControlItemFileStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(validateControlItemFileStep)
                .next(transformControlItemToCsvStep)
                .next(archiveControlItemFileStep)
                .build();
    }
}