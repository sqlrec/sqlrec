package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;

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
            throw new IllegalArgumentException("categoryColumnName not found: " + categoryColumnName);
        }

        // Build rule table for RuleDiversity
        int windowNum = Math.max(1, maxReturnRecordInt - windowSizeInt + 1);

        Object[] ruleRow = new Object[]{
                windowSizeInt,              // window_size
                1,                          // window_start (1-based)
                windowNum,                  // window_num (sliding)
                categoryColumnName,         // diversity_column
                null,                       // diversity_value (null = each distinct value)
                "<",                        // op (count < maxCategoryNumInWindow)
                maxCategoryNumInWindowInt,  // diversity_num
                1.0                         // weight
        };

        List<Object[]> ruleRows = new ArrayList<>();
        ruleRows.add(ruleRow);

        CacheTable ruleTable = new CacheTable(
                "window_diversify_rule",
                Linq4j.asEnumerable(ruleRows),
                RuleDiversity.ruleTableFields()
        );

        RuleDiversity ruleDiversity = new RuleDiversity();
        return ruleDiversity.evaluate(input, ruleTable, maxReturnRecord);
    }
}
