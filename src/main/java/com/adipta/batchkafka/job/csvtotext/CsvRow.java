package com.adipta.batchkafka.job.csvtotext;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CsvRow {
    private String field1;
    private String field2;
    private String field3;

    public String toTextLine() {
        return field1 + "|" + field2 + "|" + field3;
    }
}