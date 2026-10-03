package com.sqlrec.connectors.filesystem.handler;

import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.ScalarConversions;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FileSystemHandler {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemHandler.class);

    private final FileSystemConfig fileSystemConfig;
    // The declared primary key is a lookup key: each key can hold multiple rows.
    private Map<Object, List<Object[]>> data;

    public FileSystemHandler(FileSystemConfig fileSystemConfig) {
        this.fileSystemConfig = fileSystemConfig;
    }

    // All callers hold this handler's monitor, including readers. A complete
    // UPDATE therefore cannot expose the interval between removal and insertion.
    private Map<Object, List<Object[]>> ensureData() {
        if (data == null) {
            data = indexByPrimaryKey(new FileSystemDataLoader(fileSystemConfig).load());
        }
        return data;
    }

    private Map<Object, List<Object[]>> indexByPrimaryKey(List<Object[]> rows) {
        Map<Object, List<Object[]>> indexedRows = new LinkedHashMap<>();
        for (Object[] row : rows) {
            Object key = getKey(row);
            if (key == null) {
                logger.warn("Skipping row with null primary key at index {}", fileSystemConfig.primaryKeyIndex);
                continue;
            }
            indexedRows.computeIfAbsent(key, ignored -> new ArrayList<>()).add(row);
        }
        // Match Redis LPUSH: the last file row is the first row for this key.
        for (List<Object[]> keyRows : indexedRows.values()) {
            Collections.reverse(keyRows);
        }
        return indexedRows;
    }

    private Object getKey(Object[] row) {
        return row[fileSystemConfig.primaryKeyIndex];
    }

    private void normalizeRowTypes(Object[] row) {
        for (int i = 0; i < row.length; i++) {
            row[i] = ScalarConversions.convert(
                    row[i],
                    DataTypeUtils.getRelDataType(fileSystemConfig.fieldSchemas.get(i).getType()).getSqlTypeName()
            );
        }
    }

    public synchronized List<Object[]> scan() {
        List<Object[]> rows = new ArrayList<>();
        for (List<Object[]> keyRows : ensureData().values()) {
            rows.addAll(snapshotRows(keyRows));
        }
        return rows;
    }

    public synchronized Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Object, List<Object[]>> allData = ensureData();
        Map<Object, List<Object[]>> result = new HashMap<>();
        for (Object key : keySet) {
            List<Object[]> rows = allData.get(key);
            if (rows != null) {
                result.put(key, snapshotRows(rows));
            }
        }
        return result;
    }

    public synchronized boolean insert(Object[] data) {
        Object[] row = preparedRow(data);
        insertPrepared(row);
        return true;
    }

    public synchronized boolean insertAll(Collection<? extends Object[]> dataList) {
        List<Object[]> rows = preparedRows(dataList);
        for (Object[] row : rows) {
            insertPrepared(row);
        }
        return !rows.isEmpty();
    }

    public synchronized boolean delete(Object[] data) {
        return deletePrepared(preparedRow(data));
    }

    public synchronized boolean deleteAll(Collection<?> dataList) {
        List<Object[]> rows = preparedRows(dataList);
        boolean removed = false;
        for (Object[] row : rows) {
            removed |= deletePrepared(row);
        }
        return removed;
    }

    private boolean deletePrepared(Object[] row) {
        Object key = getKey(row);
        List<Object[]> rows = ensureData().get(key);
        if (rows == null) {
            return false;
        }
        // Like Redis LREM with count 0, delete every matching row under this key.
        boolean removed = rows.removeIf(candidate -> Arrays.deepEquals(candidate, row));
        if (rows.isEmpty()) {
            ensureData().remove(key);
        }
        return removed;
    }

    /** Replaces selected rows and returns the number actually found. */
    public synchronized int replaceAll(List<Object[]> oldRows, List<Object[]> newRows) {
        if (oldRows.size() != newRows.size()) {
            throw new IllegalArgumentException("UPDATE row counts do not match");
        }
        // Validate the whole batch before changing the table.
        List<Object[]> originals = preparedRows(oldRows);
        List<Object[]> replacements = preparedRows(newRows);
        List<Object[]> selectedReplacements = new ArrayList<>(replacements.size());
        for (int i = 0; i < originals.size(); i++) {
            if (deleteOne(originals.get(i))) {
                selectedReplacements.add(replacements.get(i));
            }
        }
        for (Object[] replacement : selectedReplacements) {
            insertPrepared(replacement);
        }
        return selectedReplacements.size();
    }

    private boolean deleteOne(Object[] row) {
        Object key = getKey(row);
        List<Object[]> rows = ensureData().get(key);
        if (rows == null) {
            return false;
        }
        for (int i = 0; i < rows.size(); i++) {
            if (Arrays.deepEquals(rows.get(i), row)) {
                rows.remove(i);
                if (rows.isEmpty()) {
                    ensureData().remove(key);
                }
                return true;
            }
        }
        return false;
    }

    private Object[] preparedRow(Object[] data) {
        Object[] row = copyRow(data);
        if (row.length != fileSystemConfig.fieldSchemas.size()) {
            throw new IllegalArgumentException("Expected " + fileSystemConfig.fieldSchemas.size()
                    + " fields, got " + row.length);
        }
        normalizeRowTypes(row);
        validatePrimaryKey(getKey(row));
        return row;
    }

    private List<Object[]> preparedRows(Collection<?> dataList) {
        List<Object[]> rows = new ArrayList<>(dataList.size());
        for (Object data : dataList) {
            if (!(data instanceof Object[])) {
                throw new IllegalArgumentException("Expected a row as Object[]");
            }
            rows.add(preparedRow((Object[]) data));
        }
        return rows;
    }

    private void insertPrepared(Object[] row) {
        ensureData().computeIfAbsent(getKey(row), ignored -> new ArrayList<>()).add(0, row);
    }

    private static List<Object[]> snapshotRows(Collection<Object[]> rows) {
        List<Object[]> snapshot = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            snapshot.add(copyRow(row));
        }
        return snapshot;
    }

    private static Object[] copyRow(Object[] row) {
        return Arrays.copyOf(Objects.requireNonNull(row, "row"), row.length);
    }

    private void validatePrimaryKey(Object primaryKeyValue) {
        if (primaryKeyValue == null) {
            throw new IllegalArgumentException("Primary key at index "
                    + fileSystemConfig.primaryKeyIndex + " is null");
        }
    }
}
