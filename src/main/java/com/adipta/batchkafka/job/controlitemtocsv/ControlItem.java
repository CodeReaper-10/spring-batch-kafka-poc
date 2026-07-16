package com.adipta.batchkafka.job.controlitemtocsv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * This JSON carries certain values in two places: flat at this level
 * (upcOrdng1-6, dlv1-4, ld1-4, hldTim1-4, shlfLif1-4) and again nested
 * one level down (upcGroups.upcForOrdering{n}.upcCode,
 * deliveryInfoGroup.delivery{n}/leadCode{n}/hldTim{n}/shlfLif{n}). Both
 * the flat fields and the nested upcGroups/deliveryInfoGroup objects
 * are declared and mapped independently -- the two representations are
 * never assumed to describe the same real-world value just because
 * they're similarly named or happened to agree in one sample file.
 * upcType has no flat counterpart and is only ever sourced from
 * upcGroups.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ControlItem {
    private String atrPat;
    private String upcOrdng1;
    private String upcOrdng2;
    private String upcOrdng3;
    private String upcOrdng4;
    private String upcOrdng5;
    private String upcOrdng6;
    private Integer dlv1;
    private Integer dlv2;
    private Integer dlv3;
    private Integer dlv4;
    private Integer ld1;
    private Integer ld2;
    private Integer ld3;
    private Integer ld4;
    private String hldTim1;
    private String hldTim2;
    private String hldTim3;
    private String hldTim4;
    private String shlfLif1;
    private String shlfLif2;
    private String shlfLif3;
    private String shlfLif4;
    private UpcGroups upcGroups;
    private DeliveryInfoGroup deliveryInfoGroup;
    private String slin;
    private Integer version;
    private String pattern;
    private String patternTypeAndCode;
    private String patternType;
    private String effectiveStartDate;
    private String effectiveEndDate;
    private String controlItemStartDate;
    private String controlItemEndDate;
    private String firstOrderDate;
    private String controlItemStatus;
    private String deleteFlag;
    private String deleteReason;
    private String isVoided;
    private String checkInFlag;
    private Double vendorCost;
    private Double itemCost;
    private Double itemRetail;
    private Double depositPrice;
    private Double manufacturerPrice;
    private String grossProfit;
    private String itemDepositAmount;
    private String itemVendor;
    private String leastDeliverableUnit;
    private String storeSpecific;
    private String minOrderQuantity;
    private String maxOrderQuantity;
    private String orderControlQuantity;
    private String manufacturerCaseQuantity;
    private String cycle;
    private String productSalesCategoryCode;
    private String productSalesSubCategoryCode;
    private String productSortAssortmentCode;
    private String consolidatedDistributionCenterType;
    private String changeReason;
    private String freshLimitTerm;
    private String freshMgmtType;
    private String itmRank;
    private String itemReturn;
    private String lastRestoreDate;
    private String npiDate;
    private String patternGroup;
    private String paymentFlag;
    private String deleteDate;
    private String channelName;
    private String refSourceName;
    private String productDescription;

    @JsonProperty("UUID")
    private String uuid;

    private List<Store> stores;
}