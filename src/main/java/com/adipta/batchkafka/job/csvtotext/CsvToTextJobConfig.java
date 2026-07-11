package com.adipta.batchkafka.job.csvtotext;

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
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
public class CsvToTextJobConfig {

    private static final String JOB_NAME = "csvToTextJob";
    private static final String DIRECTION = "CSV_TO_TEXT";
    private static final int CHUNK_SIZE = 100;

    @Bean
    @StepScope
    public FileValidationTasklet csvFileValidationTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath) {
        return new FileValidationTasklet(inputFilePath);
    }

    @Bean
    public Step validateCsvFileStep(JobRepository jobRepository,
                                    FileValidationTasklet csvFileValidationTasklet) {
        return new StepBuilder("validateCsvFileStep", jobRepository)
                .tasklet(csvFileValidationTasklet)
                .build();
    }

    @Bean
    @StepScope
    public ArchiveCleanupTasklet csvArchiveCleanupTasklet(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath,
            AuditService auditService) {
        return new ArchiveCleanupTasklet(inputFilePath, JOB_NAME, DIRECTION, auditService);
    }

    @Bean
    public Step archiveCsvFileStep(JobRepository jobRepository,
                                   ArchiveCleanupTasklet csvArchiveCleanupTasklet) {
        return new StepBuilder("archiveCsvFileStep", jobRepository)
                .tasklet(csvArchiveCleanupTasklet)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<CsvRow> csvRowReader(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath) {
        DefaultLineMapper<CsvRow> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
        tokenizer.setNames("field1", "field2", "field3");
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSet -> {
            CsvRow row = new CsvRow();
            row.setField1(fieldSet.readString("field1"));
            row.setField2(fieldSet.readString("field2"));
            row.setField3(fieldSet.readString("field3"));
            return row;
        });

        return new FlatFileItemReaderBuilder<CsvRow>()
                .name("csvRowReader")
                .resource(new FileSystemResource(inputFilePath))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
    }

    @Bean
    public FlatFileItemWriter<CsvRow> textLineWriter() {
        return new FlatFileItemWriterBuilder<CsvRow>()
                .name("textLineWriter")
                .resource(new FileSystemResource("output/output.txt"))
                .lineAggregator(CsvRow::toTextLine)
                .build();
    }

    @Bean
    public ItemProcessor<CsvRow, CsvRow> csvRowProcessor() {
        return item -> item; // identity
    }

    @Bean
    public Step transformCsvToTextStep(JobRepository jobRepository,
                                       FlatFileItemReader<CsvRow> csvRowReader,
                                       ItemProcessor<CsvRow, CsvRow> csvRowProcessor,
                                       FlatFileItemWriter<CsvRow> textLineWriter) {
        return new ChunkOrientedStepBuilder<CsvRow, CsvRow>(
                "transformCsvToTextStep", jobRepository, CHUNK_SIZE)
                .reader(csvRowReader)
                .processor(csvRowProcessor)
                .writer(textLineWriter)
                .build();
    }

    @Bean
    public Job csvToTextJob(JobRepository jobRepository,
                            Step validateCsvFileStep,
                            Step transformCsvToTextStep,
                            Step archiveCsvFileStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(validateCsvFileStep)
                .next(transformCsvToTextStep)
                .next(archiveCsvFileStep)
                .build();
    }
}