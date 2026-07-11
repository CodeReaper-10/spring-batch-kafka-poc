package com.adipta.batchkafka.job.texttocsv;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LineRecord {
    // getters/setters omitted for brevity — use Lombok @Data in real code
    private String rawLine;
    private String field1;
    private String field2;
    private String field3;

    public LineRecord() {}

    public LineRecord(String rawLine) {
        this.rawLine = rawLine;
    }
}