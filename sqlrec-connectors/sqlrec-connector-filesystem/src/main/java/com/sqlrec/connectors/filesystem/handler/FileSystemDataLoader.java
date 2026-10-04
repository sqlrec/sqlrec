package com.sqlrec.connectors.filesystem.handler;

import com.google.gson.JsonObject;
import com.sqlrec.common.utils.JsonRows;
import com.sqlrec.common.utils.ScalarConversions;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** Reads configured files into rows; indexing and mutation stay with the handler. */
final class FileSystemDataLoader {
    // Preserve the existing logger category for loading diagnostics.
    private static final Logger logger = LoggerFactory.getLogger(FileSystemHandler.class);
    private final FileSystemConfig fileSystemConfig;

    FileSystemDataLoader(FileSystemConfig fileSystemConfig) {
        this.fileSystemConfig = fileSystemConfig;
    }

    List<Object[]> load() {
        if (fileSystemConfig.path == null || fileSystemConfig.path.isEmpty()) {
            logger.info("No path configured, initializing as empty table");
            return new ArrayList<>();
        }

        Path filePath = resolvePath();
        if (filePath == null || !Files.exists(filePath)) {
            logger.info("Path does not exist: {}, initializing as empty table", fileSystemConfig.path);
            return new ArrayList<>();
        }

        try {
            String format = fileSystemConfig.format != null ? fileSystemConfig.format.toLowerCase() : "csv";
            switch (format) {
                case "csv":
                    return loadCsv(filePath);
                case "json":
                    return loadJson(filePath);
                default:
                    logger.warn("Unsupported format: {}, initializing as empty table", format);
                    return new ArrayList<>();
            }
        } catch (Exception e) {
            logger.warn("Failed to load data from {}: {}, initializing as empty table", filePath, e.getMessage());
            return new ArrayList<>();
        }
    }

    private Path resolvePath() {
        try {
            String pathStr = fileSystemConfig.path;
            if (pathStr.startsWith("file://")) {
                pathStr = pathStr.substring("file://".length());
                if (pathStr.startsWith("/") && pathStr.length() > 2 && pathStr.charAt(2) == ':') {
                    pathStr = pathStr.substring(1);
                }
            }
            return Paths.get(pathStr);
        } catch (Exception e) {
            logger.warn("Invalid path: {}", fileSystemConfig.path);
            return null;
        }
    }

    private List<Object[]> loadCsv(Path filePath) throws IOException {
        List<Object[]> rows = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.parse(reader)) {
            boolean header = true;
            for (CSVRecord record : parser) {
                if (record.size() == 1 && record.get(0).trim().isEmpty()) continue;
                // The first record is a header; values map to the schema by position.
                if (header) {
                    header = false;
                    continue;
                }
                rows.add(parseCsvRecord(record));
            }
        }
        return rows;
    }

    private Object[] parseCsvRecord(CSVRecord record) {
        Object[] row = new Object[fileSystemConfig.fieldSchemas.size()];
        for (int i = 0; i < fileSystemConfig.fieldSchemas.size() && i < record.size(); i++) {
            String value = record.get(i);
            if (value.isEmpty()) {
                row[i] = null;
            } else {
                try {
                    row[i] = ScalarConversions.convert(value, fileSystemConfig.fieldSchemas.get(i).getType());
                } catch (Exception e) {
                    row[i] = null;
                }
            }
        }
        return row;
    }

    private List<Object[]> loadJson(Path filePath) throws IOException {
        String content = Files.readString(filePath, StandardCharsets.UTF_8);
        List<JsonObject> objects;
        try {
            objects = JsonRows.readObjects(content);
        } catch (RuntimeException failure) {
            logger.warn("Invalid JSON input: {}, initializing as empty table", failure.getMessage());
            return new ArrayList<>();
        }
        return JsonRows.decodeRows(objects, fileSystemConfig.fieldSchemas, JsonRows.Decoding.COERCE_SCALARS);
    }
}
