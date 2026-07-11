package com.adipta.batchkafka.kafka;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobTriggerMessage {
    private String jobType;      // "TEXT_TO_CSV" | "CSV_TO_TEXT" | "TEXT_TO_DB"
    private String inputFilePath;
}