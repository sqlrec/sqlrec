package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Linq4j;

public class WindowDiversify {
    public CacheTable evaluate(
            CacheTable input,
            String categoryColumnName,
            Integer windowSize,
            Integer maxCategoryNumInWindow,
            Integer maxReturnRecord
    ) {
        if (windowSize == null || maxCategoryNumInWindow == null || maxReturnRecord == null) {
            throw new IllegalArgumentException("windowSize, maxCategoryNumInWindow and maxReturnRecord cannot be null");
        }
        int categoryIndex = DataTypeUtils.findFieldIndex(input.getDataFields(), categoryColumnName);
        if (categoryIndex == -1) {
            throw new IllegalArgumentException("categoryColumnName not found: " + categoryColumnName);
        }

        // Build rule table for RuleDiversity
        int windowNum = Math.max(1, maxReturnRecord - windowSize + 1);

        Object[] ruleRow = new Object[]{
                windowSize,                 // window_size
                1,                          // window_start (1-based)
                windowNum,                  // window_num (sliding)
                categoryColumnName,         // diversity_column
                null,                       // diversity_value (null = each distinct value)
                "<",                        // op (count < maxCategoryNumInWindow)
                maxCategoryNumInWindow,      // diversity_num
                1.0                         // weight
        };

        CacheTable ruleTable = new CacheTable(
                "window_diversify_rule",
                Linq4j.singletonEnumerable(ruleRow),
                RuleDiversity.ruleTableFields()
        );

        RuleDiversity ruleDiversity = new RuleDiversity();
        return ruleDiversity.evaluate(input, ruleTable, maxReturnRecord);
    }
}
