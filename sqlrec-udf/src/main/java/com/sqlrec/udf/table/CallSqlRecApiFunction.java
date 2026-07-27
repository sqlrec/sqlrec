package com.sqlrec.udf.table;

import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.SqlRecApiClient;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CallSqlRecApiFunction {
    public CacheTable evaluate(ReadonlyContext context, String url, CacheTable... tables) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("url is null or empty");
        }
        if (tables == null || tables.length == 0) {
            throw new IllegalArgumentException("at least one input table is required");
        }

        // build inputs: key = table name (matches remote SQL function input placeholder)
        Map<String, List<Map<String, Object>>> inputs = new LinkedHashMap<>();
        for (CacheTable table : tables) {
            String tableName = table.getTableName();
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("input table has no name");
            }
            Enumerable<Object[]> enumerable = table.scan(null);
            List<Object[]> rows = enumerable != null ? enumerable.toList() : new ArrayList<>();
            inputs.put(tableName, DataTransformUtils.convertToMapList(rows, table.getDataFields()));
        }

        // call remote sqlrec api via common client
        ExecuteData response = SqlRecApiClient.callFunctionApi(
                url, inputs, context.getVariables(), context.getMetricsTags());
        List<Map<String, Object>> dataRows = response.getData();

        if (dataRows == null || dataRows.isEmpty()) {
            String msg = response.getMsg();
            throw new RuntimeException("remote sqlrec api call failed: "
                    + (msg == null ? "remote api returned empty data" : msg));
        }

        // infer output fields from response rows
        List<RelDataTypeField> dataFields = DataTypeUtils.inferFields(dataRows);

        // build output rows (ARRAY values kept as List object; Map/complex values serialized to JSON string)
        List<Object[]> outRows = new ArrayList<>(dataRows.size());
        for (Map<String, Object> rowMap : dataRows) {
            Object[] row = new Object[dataFields.size()];
            for (int i = 0; i < dataFields.size(); i++) {
                RelDataTypeField field = dataFields.get(i);
                Object val = rowMap.get(field.getName());
                if (field.getType().getSqlTypeName() != SqlTypeName.ARRAY
                        && (val instanceof List || val instanceof Map)) {
                    val = JsonUtils.toJson(val);
                }
                row[i] = val;
            }
            outRows.add(row);
        }

        return new CacheTable("output", Linq4j.asEnumerable(outRows), dataFields);
    }
}
