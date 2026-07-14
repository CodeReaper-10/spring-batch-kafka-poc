package com.adipta.batchkafka.job.itemtocsv;

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
 * Reads a single item.json (entities.item) and flattens it into one
 * CSV header + one data row. upcList is exploded into numbered
 * upc1..upcN columns since its length varies per item.
 */
@Slf4j
public class ItemJsonToCsvTasklet implements Tasklet {

    private final String inputFilePath;
    private final String outputFilePath;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ItemJsonToCsvTasklet(String inputFilePath, String outputFilePath) {
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outputFilePath;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ItemEnvelope envelope = objectMapper.readValue(new File(inputFilePath), ItemEnvelope.class);
        Item item = envelope.getEntities().getItem();

        Map<String, String> row = new LinkedHashMap<>();
        row.put("slin", item.getSlin());
        row.put("version", asString(item.getVersion()));
        row.put("shortDescription", item.getShortDescription());
        row.put("longDescription", item.getLongDescription());
        row.put("effectiveStartDate", item.getEffectiveStartDate());
        row.put("effectiveEndDate", item.getEffectiveEndDate());
        row.put("itemStartDate", item.getItemStartDate());
        row.put("itemEndDate", item.getItemEndDate());
        row.put("itemStatus", item.getItemStatus());
        row.put("primaryUPCCode", item.getPrimaryUPCCode());
        row.put("primaryUPCType", item.getPrimaryUPCType());
        row.put("itemType", item.getItemType());
        row.put("licenseType", item.getLicenseType());
        row.put("patternType", item.getPatternType());
        row.put("orderType", item.getOrderType());
        row.put("itemRank", item.getItemRank());
        row.put("manufacturerCode", item.getManufacturerCode());
        row.put("distFeeCode", item.getDistFeeCode());
        row.put("checkInFlag", item.getCheckInFlag());
        row.put("sizeGroup", item.getSizeGroup());
        row.put("alcoholVolume", item.getAlcoholVolume());
        row.put("locale", item.getLocale());
        row.put("ebtEligibility", item.getEbtEligibility());
        row.put("corpBrand", item.getCorpBrand());
        row.put("mipUom", item.getMipUom());
        row.put("retailUom", item.getRetailUom());
        row.put("newItemReason", item.getNewItemReason());
        row.put("prebookType", item.getPrebookType());
        row.put("changeReason", item.getChangeReason());
        row.put("retailSize", item.getRetailSize());
        row.put("manufactureCaseQty", item.getManufactureCaseQty());
        row.put("country", item.getCountry());
        row.put("isVoided", item.getIsVoided());
        row.put("isCFL", item.getIsCFL());
        row.put("productSalesCategoryCode", item.getProductSalesCategoryCode());
        row.put("productSortAssortmentCode", item.getProductSortAssortmentCode());
        row.put("productSalesSubCategoryCode", item.getProductSalesSubCategoryCode());
        row.put("innerPackQuantity", asString(item.getInnerPackQuantity()));
        row.put("productMovementAnalysis", item.getProductMovementAnalysis());
        row.put("pointOfSale", item.getPointOfSale());

        List<String> upcList = item.getUpcList() == null ? List.of() : item.getUpcList();
        for (int i = 0; i < upcList.size(); i++) {
            row.put("upc" + (i + 1), upcList.get(i));
        }

        row.put("uuid", item.getUuid());
        row.put("isCaseMasterItem", item.getIsCaseMasterItem());

        CsvWriterSupport.write(Path.of(outputFilePath), List.copyOf(row.keySet()), List.of(new ArrayList<>(row.values())));

        chunkContext.getStepContext().getStepExecution()
                .getJobExecution().getExecutionContext()
                .putLong("rowsProcessed", 1L);

        log.info("[TRANSFORM] Wrote {} from {}", outputFilePath, inputFilePath);
        return RepeatStatus.FINISHED;
    }

    private static String asString(Object value) {
        return value == null ? "" : String.valueOf(value);
    }
}