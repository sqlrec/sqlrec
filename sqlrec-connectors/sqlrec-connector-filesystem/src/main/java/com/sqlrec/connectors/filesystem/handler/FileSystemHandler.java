package com.sqlrec.connectors.filesystem.handler;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class FileSystemHandler {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemHandler.class);

    private final FileSystemConfig fileSystemConfig;
    private volatile List<Object[]> data;
    private final Object loadLock = new Object();

    public FileSystemHandler(FileSystemConfig fileSystemConfig) {
        this.fileSystemConfig = fileSystemConfig;
    }

    private List<Object[]> ensureData() {
        if (data == null) {
            synchronized (loadLock) {
                if (data == null) {
                    data = loadData();
                }
            }
        }
        return data;
    }

    private List<Object[]> loadData() {
        if (fileSystemConfig.path == null || fileSystemConfig.path.isEmpty()) {
            logger.info("No path configured, initializing as empty table");
            return new CopyOnWriteArrayList<>();
        }

        Path filePath = resolvePath();
        if (filePath == null || !Files.exists(filePath)) {
            logger.info("Path does not exist: {}, initializing as empty table", fileSystemConfig.path);
            return new CopyOnWriteArrayList<>();
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
                    return new CopyOnWriteArrayList<>();
            }
        } catch (Exception e) {
            logger.warn("Failed to load data from {}: {}, initializing as empty table", filePath, e.getMessage());
            return new CopyOnWriteArrayList<>();
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
        List<Object[]> rows = new CopyOnWriteArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
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
                Object[] row = parseCsvLine(line);
                if (row != null) {
                    rows.add(row);
                }
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
                    row[i] = DataTypeUtils.parseStringAsType(value, fileSystemConfig.fieldSchemas.get(i).getType());
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
                    fields.add(current.toString().trim());
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }
        }
        fields.add(current.toString().trim());
        return fields;
    }

    private List<Object[]> loadJson(Path filePath) throws IOException {
        List<Object[]> rows = new CopyOnWriteArrayList<>();
        String content = new String(Files.readAllBytes(filePath));
        Gson gson = new Gson();
        JsonArray jsonArray;

        try {
            JsonElement element = gson.fromJson(content, JsonElement.class);
            if (element.isJsonArray()) {
                jsonArray = element.getAsJsonArray();
            } else if (element.isJsonObject()) {
                jsonArray = new JsonArray();
                jsonArray.add(element.getAsJsonObject());
            } else {
                logger.warn("JSON content is not an array or object, initializing as empty table");
                return new CopyOnWriteArrayList<>();
            }
        } catch (Exception e) {
            logger.warn("Invalid JSON format: {}", e.getMessage());
            return new CopyOnWriteArrayList<>();
        }

        for (JsonElement element : jsonArray) {
            if (!element.isJsonObject()) {
                continue;
            }
            JsonObject obj = element.getAsJsonObject();
            Object[] row = new Object[fileSystemConfig.fieldSchemas.size()];
            for (int i = 0; i < fileSystemConfig.fieldSchemas.size(); i++) {
                FieldSchema fieldSchema = fileSystemConfig.fieldSchemas.get(i);
                JsonElement fieldElement = obj.get(fieldSchema.getName());
                if (fieldElement == null || fieldElement.isJsonNull()) {
                    row[i] = null;
                } else {
                    row[i] = convertJsonElement(fieldElement, fieldSchema.getType());
                }
            }
            rows.add(row);
        }
        return rows;
    }

    private Object convertJsonElement(JsonElement element, String type) {
        try {
            if (element.isJsonPrimitive()) {
                return DataTypeUtils.parseStringAsType(element.getAsString(), type);
            }
            return element.toString();
        } catch (Exception e) {
            return null;
        }
    }

    public List<Object[]> scan() {
        return new ArrayList<>(ensureData());
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyMap();
        }

        List<Object[]> allData = ensureData();
        Map<Object, List<Object[]>> result = new HashMap<>();
        for (Object[] row : allData) {
            Object key = row[fileSystemConfig.primaryKeyIndex];
            if (keySet.contains(key)) {
                result.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }
        return result;
    }

    public boolean upsert(Object[] data) {
        List<Object[]> allData = ensureData();
        Object primaryKeyValue = data[fileSystemConfig.primaryKeyIndex];

        for (int i = 0; i < allData.size(); i++) {
            Object[] existing = allData.get(i);
            if (primaryKeyValue.equals(existing[fileSystemConfig.primaryKeyIndex])) {
                allData.set(i, data);
                return true;
            }
        }

        allData.add(data);
        return true;
    }

    public boolean delete(Object[] data) {
        List<Object[]> allData = ensureData();
        Object primaryKeyValue = data[fileSystemConfig.primaryKeyIndex];
        return allData.removeIf(row -> primaryKeyValue.equals(row[fileSystemConfig.primaryKeyIndex]));
    }
}
