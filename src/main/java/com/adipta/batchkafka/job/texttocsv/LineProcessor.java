package com.adipta.batchkafka.job.texttocsv;

import org.springframework.batch.infrastructure.item.ItemProcessor;

/**
 * Example: incoming text lines are pipe-delimited "field1|field2|field3".
 * Adjust to whatever the real text format turns out to be.
 */
public class LineProcessor implements ItemProcessor<LineRecord, LineRecord> {

    @Override
    public LineRecord process(LineRecord item) {
        String[] parts = item.getRawLine().split("\\|");
        if (parts.length >= 3) {
            item.setField1(parts[0].trim());
            item.setField2(parts[1].trim());
            item.setField3(parts[2].trim());
        }
        return item;
    }
}