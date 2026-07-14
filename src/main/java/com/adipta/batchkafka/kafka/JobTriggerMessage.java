package com.adipta.batchkafka.kafka;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobTriggerMessage {
    private String jobType;      // "TEXT_TO_CSV" | "CSV_TO_TEXT" | "ITEM_TO_CSV" | "CONTROL_ITEM_TO_CSV"
    private String inputFilePath;
}