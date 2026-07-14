package com.adipta.batchkafka.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Shared plain-CSV writer for tasklets that produce a small, fixed
 * number of rows directly (no FlatFileItemWriter chunking involved).
 */
public final class CsvWriterSupport {

    private CsvWriterSupport() {
    }

    public static void write(Path outputPath, List<String> header, List<List<String>> rows) throws IOException {
        if (outputPath.getParent() != null) {
            Files.createDirectories(outputPath.getParent());
        }
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            writer.write(toCsvLine(header));
            writer.newLine();
            for (List<String> row : rows) {
                writer.write(toCsvLine(row));
                writer.newLine();
            }
        }
    }

    private static String toCsvLine(List<String> values) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                line.append(',');
            }
            line.append(escape(values.get(i)));
        }
        return line.toString();
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}