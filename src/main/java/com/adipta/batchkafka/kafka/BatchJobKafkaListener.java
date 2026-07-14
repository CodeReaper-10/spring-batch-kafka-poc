package com.adipta.batchkafka.kafka;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BatchJobKafkaListener {

    private final JobOperator jobOperator;
    private final Job textToCsvJob;
    private final Job csvToTextJob;
    private final Job itemToCsvJob;
    private final Job controlItemToCsvJob;

    public BatchJobKafkaListener(JobOperator jobOperator,
                                 Job textToCsvJob,
                                 Job csvToTextJob,
                                 Job itemToCsvJob,
                                 Job controlItemToCsvJob) {
        this.jobOperator = jobOperator;
        this.textToCsvJob = textToCsvJob;
        this.csvToTextJob = csvToTextJob;
        this.itemToCsvJob = itemToCsvJob;
        this.controlItemToCsvJob = controlItemToCsvJob;
    }

    @KafkaListener(topics = "batch-job-trigger", groupId = "batch-poc-group")
    public void onMessage(JobTriggerMessage message) throws Exception {
        var params = new JobParametersBuilder()
                .addString("inputFilePath", message.getInputFilePath())
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        Job jobToRun = switch (message.getJobType()) {
            case "TEXT_TO_CSV" -> textToCsvJob;
            case "CSV_TO_TEXT" -> csvToTextJob;
            case "ITEM_TO_CSV" -> itemToCsvJob;
            case "CONTROL_ITEM_TO_CSV" -> controlItemToCsvJob;
            default -> throw new IllegalArgumentException("Unknown jobType: " + message.getJobType());
        };

        jobOperator.start(jobToRun, params);
    }
}