package com.adipta.batchkafka.job.controlitemtocsv;

import lombok.Getter;
import lombok.Setter;

/**
 * Mirrors the {"entities": {"controlItem": {...}}} wrapper shape of
 * Control_Item.json.
 */
@Getter
@Setter
public class ControlItemEnvelope {

    private Entities entities;

    @Getter
    @Setter
    public static class Entities {
        private ControlItem controlItem;
    }
}