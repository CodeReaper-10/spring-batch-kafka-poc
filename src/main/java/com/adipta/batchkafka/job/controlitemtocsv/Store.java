package com.adipta.batchkafka.job.controlitemtocsv;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Store {
    private String storeId;
    private String storeStatus;
    private String effectiveStartDate;
    private String effectiveEndDate;
}