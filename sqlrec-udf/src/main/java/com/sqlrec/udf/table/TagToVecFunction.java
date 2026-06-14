package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.*;
import java.util.stream.Collectors;

public class TagToVecFunction {
    public CacheTable evaluate(CacheTable input, String tagColName, String outputColName) {
        if (tagColName == null || tagColName.isEmpty()) {
            throw new IllegalArgumentException("tag column name is empty");
        }
        if (outputColName == null || outputColName.isEmpty()) {
            throw new IllegalArgumentException("output column name is empty");
        }

        List<RelDataTypeField> dataFields = input.getDataFields();
        List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(
                dataFields, Collections.singletonList(new FieldSchema(outputColName, "ARRAY<FLOAT>")));

        int tagColIndex = DataTypeUtils.findFieldIndex(dataFields, tagColName);
        if (tagColIndex == -1) {
            throw new IllegalArgumentException("tag column not found: " + tagColName);
        }

        if (DataTypeUtils.findFieldIndex(dataFields, outputColName) != -1) {
            throw new IllegalArgumentException("output column name already exists: " + outputColName);
        }

        Enumerable<Object[]> enumerable = input.scan(null);
        if (enumerable == null) {
            return new CacheTable("output", Linq4j.asEnumerable(Collections.emptyList()), newDataFields);
        }

        // collect all unique tags, preserving insertion order
        Map<String, Integer> tagIndexMap = new LinkedHashMap<>();
        int nextIndex = 0;
        for (Object[] row : enumerable) {
            for (String tagStr : extractTags(row[tagColIndex])) {
                if (!tagIndexMap.containsKey(tagStr)) {
                    tagIndexMap.put(tagStr, nextIndex++);
                }
            }
        }

        int vecSize = tagIndexMap.size();

        // build new data with multihot vector column
        List<Object[]> newData = new ArrayList<>();
        for (Object[] row : enumerable) {
            Object[] newRow = new Object[row.length + 1];
            System.arraycopy(row, 0, newRow, 0, row.length);

            Float[] vec = new Float[vecSize];
            Arrays.fill(vec, 0.0f);
            for (String tagStr : extractTags(row[tagColIndex])) {
                Integer idx = tagIndexMap.get(tagStr);
                if (idx != null) {
                    vec[idx] = 1.0f;
                }
            }
            newRow[row.length] = vec;
            newData.add(newRow);
        }

        return new CacheTable("output", Linq4j.asEnumerable(newData), newDataFields);
    }

    private List<String> extractTags(Object tagValue) {
        if (tagValue == null) {
            return Collections.emptyList();
        }
        if (tagValue instanceof List) {
            return ((List<?>) tagValue).stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
        }
        return Collections.singletonList(tagValue.toString());
    }
}
