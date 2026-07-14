package com.adipta.batchkafka.job.itemtocsv;

import lombok.Getter;
import lombok.Setter;

/**
 * Mirrors the {"entities": {"item": {...}}} wrapper shape of item.json.
 */
@Getter
@Setter
public class ItemEnvelope {

    private Entities entities;

    @Getter
    @Setter
    public static class Entities {
        private Item item;
    }
}