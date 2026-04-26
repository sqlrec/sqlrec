package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.*;
import java.util.stream.Collectors;

public class WindowDiversify {
    public CacheTable evaluate(
            CacheTable input,
            String categoryColumnName,
            String windowSize,
            String maxCategoryNumInWindow,
            String maxReturnRecord
    ) {
        int windowSizeInt = Integer.parseInt(windowSize);
        int maxCategoryNumInWindowInt = Integer.parseInt(maxCategoryNumInWindow);
        int maxReturnRecordInt = Integer.parseInt(maxReturnRecord);

        int categoryIndex = -1;
        for (RelDataTypeField field : input.getDataFields()) {
            if (field.getName().equals(categoryColumnName)) {
                categoryIndex = field.getIndex();
                break;
            }
        }
        if (categoryIndex == -1) {
            throw new IllegalArgumentException("catalogColumnName not found: " + categoryColumnName);
        }

        List<List<String>> categorys = new ArrayList<>();
        List<Object[]> originData = new ArrayList<>();
        Enumerable<Object[]> enumerable = input.scan(null);
        for (Object[] data : enumerable) {
            originData.add(data);
            Object category = data[categoryIndex];
            if (category == null) {
                categorys.add(new ArrayList<>());
            } else {
                if (category instanceof List) {
                    categorys.add(
                            ((List<?>) category).stream()
                                    .map(Object::toString)
                                    .collect(Collectors.toList())
                    );
                } else {
                    categorys.add(Collections.singletonList(category.toString()));
                }
            }
        }

        Set<Integer> usedIndex = new HashSet<>();
        Map<String, Integer> categoryCount = new HashMap<>();
        List<Object[]> newData = new ArrayList<>();
        List<Integer> originRecordIndex = new ArrayList<>();
        for (int i = 0; i < maxReturnRecordInt; i++) {
            int validIndex = -1;
            for (int j = 0; j < originData.size(); j++) {
                if (usedIndex.contains(j)) {
                    continue;
                }
                if (validIndex == -1) {
                    validIndex = j;
                }

                boolean valid = true;
                List<String> categories = categorys.get(j);
                for (String category : categories) {
                    if (categoryCount.getOrDefault(category, 0) >= maxCategoryNumInWindowInt) {
                        valid = false;
                        break;
                    }
                }

                if (valid) {
                    validIndex = j;
                    break;
                }
            }

            if (validIndex == -1) {
                break;
            }

            newData.add(originData.get(validIndex));
            originRecordIndex.add(validIndex);
            for (String category : categorys.get(validIndex)) {
                categoryCount.put(category, categoryCount.getOrDefault(category, 0) + 1);
            }
            usedIndex.add(validIndex);

            if (i - windowSizeInt >= 0) {
                List<String> categories = categorys.get(originRecordIndex.get(i - windowSizeInt));
                for (String category : categories) {
                    categoryCount.put(category, categoryCount.get(category) - 1);
                }
            }
        }

        return new CacheTable(
                input.getTableName() + "_diversify",
                Linq4j.asEnumerable(newData),
                input.getDataFields()
        );
    }
}
