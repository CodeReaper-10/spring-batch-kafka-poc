package com.adipta.batchkafka.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String BATCH_JOB_TOPIC = "batch-job-trigger";

    @Bean
    public NewTopic batchJobTopic() {
        return TopicBuilder.name(BATCH_JOB_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
}