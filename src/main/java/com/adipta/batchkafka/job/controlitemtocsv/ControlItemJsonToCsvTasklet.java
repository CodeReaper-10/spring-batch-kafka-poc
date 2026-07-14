package com.adipta.batchkafka.job.controlitemtocsv;

import com.adipta.batchkafka.util.CsvWriterSupport;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a single Control_Item.json (entities.controlItem) and writes
 * two CSVs: one row of flat control-item fields, and one row per
 * store in the (unbounded-length) stores array, linked back to the
 * control item via slin.
 */
@Slf4j
public class ControlItemJsonToCsvTasklet implements Tasklet {

    private final String inputFilePath;
    private final String controlItemOutputPath;
    private final String storesOutputPath;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ControlItemJsonToCsvTasklet(String inputFilePath, String controlItemOutputPath, String storesOutputPath) {
        this.inputFilePath = inputFilePath;
        this.controlItemOutputPath = controlItemOutputPath;
        this.storesOutputPath = storesOutputPath;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ControlItemEnvelope envelope = objectMapper.readValue(new File(inputFilePath), ControlItemEnvelope.class);
        ControlItem controlItem = envelope.getEntities().getControlItem();

        Map<String, String> row = new LinkedHashMap<>();
        row.put("slin", controlItem.getSlin());
        row.put("version", asString(controlItem.getVersion()));
        row.put("atrPat", controlItem.getAtrPat());
        row.put("upcOrdng1", controlItem.getUpcOrdng1());
        row.put("upcOrdng2", controlItem.getUpcOrdng2());
        row.put("upcOrdng3", controlItem.getUpcOrdng3());
        row.put("upcOrdng4", controlItem.getUpcOrdng4());
        row.put("upcOrdng5", controlItem.getUpcOrdng5());
        row.put("upcOrdng6", controlItem.getUpcOrdng6());
        row.put("dlv1", asString(controlItem.getDlv1()));
        row.put("dlv2", asString(controlItem.getDlv2()));
        row.put("dlv3", asString(controlItem.getDlv3()));
        row.put("dlv4", asString(controlItem.getDlv4()));
        row.put("ld1", asString(controlItem.getLd1()));
        row.put("ld2", asString(controlItem.getLd2()));
        row.put("ld3", asString(controlItem.getLd3()));
        row.put("ld4", asString(controlItem.getLd4()));
        row.put("hldTim1", controlItem.getHldTim1());
        row.put("hldTim2", controlItem.getHldTim2());
        row.put("hldTim3", controlItem.getHldTim3());
        row.put("hldTim4", controlItem.getHldTim4());
        row.put("shlfLif1", controlItem.getShlfLif1());
        row.put("shlfLif2", controlItem.getShlfLif2());
        row.put("shlfLif3", controlItem.getShlfLif3());
        row.put("shlfLif4", controlItem.getShlfLif4());
        row.put("pattern", controlItem.getPattern());
        row.put("patternTypeAndCode", controlItem.getPatternTypeAndCode());
        row.put("patternType", controlItem.getPatternType());
        row.put("effectiveStartDate", controlItem.getEffectiveStartDate());
        row.put("effectiveEndDate", controlItem.getEffectiveEndDate());
        row.put("controlItemStartDate", controlItem.getControlItemStartDate());
        row.put("controlItemEndDate", controlItem.getControlItemEndDate());
        row.put("firstOrderDate", controlItem.getFirstOrderDate());
        row.put("controlItemStatus", controlItem.getControlItemStatus());
        row.put("deleteFlag", controlItem.getDeleteFlag());
        row.put("deleteReason", controlItem.getDeleteReason());
        row.put("isVoided", controlItem.getIsVoided());
        row.put("checkInFlag", controlItem.getCheckInFlag());
        row.put("vendorCost", asString(controlItem.getVendorCost()));
        row.put("itemCost", asString(controlItem.getItemCost()));
        row.put("itemRetail", asString(controlItem.getItemRetail()));
        row.put("depositPrice", asString(controlItem.getDepositPrice()));
        row.put("manufacturerPrice", asString(controlItem.getManufacturerPrice()));
        row.put("grossProfit", controlItem.getGrossProfit());
        row.put("itemDepositAmount", controlItem.getItemDepositAmount());
        row.put("itemVendor", controlItem.getItemVendor());
        row.put("leastDeliverableUnit", controlItem.getLeastDeliverableUnit());
        row.put("storeSpecific", controlItem.getStoreSpecific());
        row.put("minOrderQuantity", controlItem.getMinOrderQuantity());
        row.put("maxOrderQuantity", controlItem.getMaxOrderQuantity());
        row.put("orderControlQuantity", controlItem.getOrderControlQuantity());
        row.put("manufacturerCaseQuantity", controlItem.getManufacturerCaseQuantity());
        row.put("cycle", controlItem.getCycle());
        row.put("productSalesCategoryCode", controlItem.getProductSalesCategoryCode());
        row.put("productSalesSubCategoryCode", controlItem.getProductSalesSubCategoryCode());
        row.put("productSortAssortmentCode", controlItem.getProductSortAssortmentCode());
        row.put("consolidatedDistributionCenterType", controlItem.getConsolidatedDistributionCenterType());
        row.put("changeReason", controlItem.getChangeReason());
        row.put("freshLimitTerm", controlItem.getFreshLimitTerm());
        row.put("freshMgmtType", controlItem.getFreshMgmtType());
        row.put("itmRank", controlItem.getItmRank());
        row.put("itemReturn", controlItem.getItemReturn());
        row.put("lastRestoreDate", controlItem.getLastRestoreDate());
        row.put("npiDate", controlItem.getNpiDate());
        row.put("patternGroup", controlItem.getPatternGroup());
        row.put("paymentFlag", controlItem.getPaymentFlag());
        row.put("deleteDate", controlItem.getDeleteDate());
        row.put("channelName", controlItem.getChannelName());
        row.put("refSourceName", controlItem.getRefSourceName());
        row.put("productDescription", controlItem.getProductDescription());
        row.put("uuid", controlItem.getUuid());

        CsvWriterSupport.write(Path.of(controlItemOutputPath), List.copyOf(row.keySet()),
                List.of(new ArrayList<>(row.values())));

        List<Store> stores = controlItem.getStores() == null ? List.of() : controlItem.getStores();
        List<String> storesHeader = List.of("controlItemSlin", "storeId", "storeStatus",
                "effectiveStartDate", "effectiveEndDate");
        List<List<String>> storeRows = new ArrayList<>();
        for (Store store : stores) {
            storeRows.add(List.of(
                    nullToEmpty(controlItem.getSlin()),
                    nullToEmpty(store.getStoreId()),
                    nullToEmpty(store.getStoreStatus()),
                    nullToEmpty(store.getEffectiveStartDate()),
                    nullToEmpty(store.getEffectiveEndDate())));
        }
        CsvWriterSupport.write(Path.of(storesOutputPath), storesHeader, storeRows);

        chunkContext.getStepContext().getStepExecution()
                .getJobExecution().getExecutionContext()
                .putLong("rowsProcessed", 1L + stores.size());

        log.info("[TRANSFORM] Wrote {} and {} from {}", controlItemOutputPath, storesOutputPath, inputFilePath);
        return RepeatStatus.FINISHED;
    }

    private static String asString(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}