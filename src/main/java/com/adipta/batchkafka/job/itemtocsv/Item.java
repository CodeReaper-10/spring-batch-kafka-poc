package com.adipta.batchkafka.job.itemtocsv;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class Item {
    private String slin;
    private Integer version;
    private String shortDescription;
    private String longDescription;
    private String effectiveStartDate;
    private String effectiveEndDate;
    private String itemStartDate;
    private String itemEndDate;
    private String itemStatus;
    private String primaryUPCCode;
    private String primaryUPCType;
    private String itemType;
    private String licenseType;
    private String patternType;
    private String orderType;
    private String itemRank;
    private String manufacturerCode;
    private String distFeeCode;
    private String checkInFlag;
    private String sizeGroup;
    private String alcoholVolume;
    private String locale;
    private String ebtEligibility;
    private String corpBrand;
    private String mipUom;
    private String retailUom;
    private String newItemReason;
    private String prebookType;
    private String changeReason;
    private String retailSize;
    private String manufactureCaseQty;
    private String country;
    private String isVoided;
    private String isCFL;
    private String productSalesCategoryCode;
    private String productSortAssortmentCode;
    private String productSalesSubCategoryCode;
    private Integer innerPackQuantity;
    private String productMovementAnalysis;
    private String pointOfSale;
    private List<String> upcList;
    private String uuid;
    private String isCaseMasterItem;
}