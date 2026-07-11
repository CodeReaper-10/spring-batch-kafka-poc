package com.adipta.batchkafka.controller;

import com.adipta.batchkafka.kafka.JobTriggerMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/batch")
public class BatchProcessTriggerController {
    private final KafkaTemplate<String, JobTriggerMessage> kafkaTemplate;

    public BatchProcessTriggerController(KafkaTemplate<String, JobTriggerMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/trigger")
    public String trigger(@RequestBody JobTriggerMessage message) {
        kafkaTemplate.send("batch-job-trigger", message);
        return "Triggered: " + message.getJobType();
    }
}