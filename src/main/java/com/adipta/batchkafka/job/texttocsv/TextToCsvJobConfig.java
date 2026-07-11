package com.adipta.batchkafka.job.texttocsv;

import com.adipta.batchkafka.audit.AuditService;
import com.adipta.batchkafka.tasklet.ArchiveCleanupTasklet;
import com.adipta.batchkafka.tasklet.FileValidationTasklet;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.ChunkOrientedStepBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineAggregator;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
public class TextToCsvJobConfig {

    private static final String JOB_NAME = "textToCsvJob";
    private static final String DIRECTION = "TEXT_TO_CSV";
    private static final int CHUNK_SIZE = 100;

    @Bean
    @StepScope
    public FileValidationTasklet fileValidationTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath) {
        return new FileValidationTasklet(inputFilePath);
    }

    @Bean
    public Step validateTextFileStep(JobRepository jobRepository,
                                     FileValidationTasklet fileValidationTasklet) {
        return new StepBuilder("validateTextFileStep", jobRepository)
                .tasklet(fileValidationTasklet)
                .build();
    }

    @Bean
    @StepScope
    public ArchiveCleanupTasklet archiveCleanupTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath,
            AuditService auditService) {
        return new ArchiveCleanupTasklet(inputFilePath, JOB_NAME, DIRECTION, auditService);
    }

    @Bean
    public Step archiveTextFileStep(JobRepository jobRepository,
                                    ArchiveCleanupTasklet archiveCleanupTasklet) {
        return new StepBuilder("archiveTextFileStep", jobRepository)
                .tasklet(archiveCleanupTasklet)
                .build();
    }

    // ---- Chunk step: text -> CSV ----

    @Bean
    @StepScope
    public FlatFileItemReader<LineRecord> textLineReader(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath) {
        return new FlatFileItemReaderBuilder<LineRecord>()
                .name("textLineReader")
                .resource(new FileSystemResource(inputFilePath))
                .lineMapper((line, lineNumber) -> new LineRecord(line))
                .build();
    }

    @Bean
    public FlatFileItemWriter<LineRecord> csvWriter() {
        DelimitedLineAggregator<LineRecord> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(item ->
                new Object[]{item.getField1(), item.getField2(), item.getField3()});

        return new FlatFileItemWriterBuilder<LineRecord>()
                .name("csvWriter")
                .resource(new FileSystemResource("output/output.csv"))
                .lineAggregator(aggregator)
                .headerCallback(writer -> writer.write("field1,field2,field3"))
                .build();
    }

    @Bean
    public ItemProcessor<LineRecord, LineRecord> lineProcessor() {
        return new LineProcessor();
    }

    // CHANGED: ChunkOrientedStepBuilder replaces StepBuilder(...).chunk(...)
    @Bean
    public Step transformTextToCsvStep(JobRepository jobRepository,
                                       FlatFileItemReader<LineRecord> textLineReader,
                                       ItemProcessor<LineRecord, LineRecord> lineProcessor,
                                       FlatFileItemWriter<LineRecord> csvWriter) {
        return new ChunkOrientedStepBuilder<LineRecord, LineRecord>(
                "transformTextToCsvStep", jobRepository, CHUNK_SIZE)
                .reader(textLineReader)
                .processor(lineProcessor)
                .writer(csvWriter)
                .build();
    }

    // ---- Job: tasklet -> chunk -> tasklet ----

    @Bean
    public Job textToCsvJob(JobRepository jobRepository,
                            Step validateTextFileStep,
                            Step transformTextToCsvStep,
                            Step archiveTextFileStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(validateTextFileStep)
                .next(transformTextToCsvStep)
                .next(archiveTextFileStep)
                .build();
    }
}