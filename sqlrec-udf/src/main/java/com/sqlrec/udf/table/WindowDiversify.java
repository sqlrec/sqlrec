package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

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

        List<RelDataTypeField> ruleFields = new ArrayList<>();
        ruleFields.add(DataTypeUtils.getRelDataTypeField("window_size", 0, SqlTypeName.INTEGER));
        ruleFields.add(DataTypeUtils.getRelDataTypeField("window_start", 1, SqlTypeName.INTEGER));
        ruleFields.add(DataTypeUtils.getRelDataTypeField("window_num", 2, SqlTypeName.INTEGER));
        ruleFields.add(DataTypeUtils.getRelDataTypeField("diversity_column", 3, SqlTypeName.VARCHAR));
        ruleFields.add(DataTypeUtils.getRelDataTypeField("diversity_value", 4, SqlTypeName.VARCHAR));
        ruleFields.add(DataTypeUtils.getRelDataTypeField("op", 5, SqlTypeName.VARCHAR));
        ruleFields.add(DataTypeUtils.getRelDataTypeField("diversity_num", 6, SqlTypeName.INTEGER));
        ruleFields.add(DataTypeUtils.getRelDataTypeField("weight", 7, SqlTypeName.DOUBLE));

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
                ruleFields
        );

        RuleDiversity ruleDiversity = new RuleDiversity();
        return ruleDiversity.evaluate(input, ruleTable, maxReturnRecord);
    }
}
