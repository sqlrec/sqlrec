package com.sqlrec.connectors.filesystem.handler;

import com.google.gson.JsonObject;
import com.sqlrec.common.utils.JsonRows;
import com.sqlrec.common.utils.ScalarConversions;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
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
        try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
            String line;
            boolean firstLine = true;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (firstLine) {
                    firstLine = false;
                    continue;
                }
                rows.add(parseCsvLine(line));
            }
        }
        return rows;
    }

    private Object[] parseCsvLine(String line) {
        List<String> fields = splitCsvLine(line);
        Object[] row = new Object[fileSystemConfig.fieldSchemas.size()];
        for (int i = 0; i < fileSystemConfig.fieldSchemas.size() && i < fields.size(); i++) {
            String value = fields.get(i);
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

    private List<String> splitCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.add(current.toString());
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }
        }
        fields.add(current.toString());
        return fields;
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
