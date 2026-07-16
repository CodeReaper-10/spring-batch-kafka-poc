package com.adipta.batchkafka.job.controlitemtocsv;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DeliveryInfoGroup {
    private Integer delivery1;
    private Integer delivery2;
    private Integer delivery3;
    private Integer delivery4;
    private Integer leadCode1;
    private Integer leadCode2;
    private Integer leadCode3;
    private Integer leadCode4;
    private String hldTim1;
    private String hldTim2;
    private String hldTim3;
    private String hldTim4;
    private String shlfLif1;
    private String shlfLif2;
    private String shlfLif3;
    private String shlfLif4;
}