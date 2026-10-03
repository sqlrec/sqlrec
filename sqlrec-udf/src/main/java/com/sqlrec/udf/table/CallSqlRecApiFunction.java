package com.sqlrec.udf.table;

import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.SqlRecApiClient;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.common.utils.SchemaInference;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CallSqlRecApiFunction {
    private final RemoteFunctionClient client;

    public CallSqlRecApiFunction() {
        this(SqlRecApiClient::callFunctionApi);
    }

    public CallSqlRecApiFunction(RemoteFunctionClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    public CacheTable evaluate(ReadonlyContext context, String url, CacheTable... tables) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("url is null or empty");
        }
        if (tables == null || tables.length == 0) {
            throw new IllegalArgumentException("at least one input table is required");
        }

        // build inputs: key = table name (matches remote SQL function input placeholder)
        Map<String, List<Map<String, Object>>> inputs = new LinkedHashMap<>();
        for (CacheTable table : tables) {
            if (table == null) {
                throw new IllegalArgumentException("input table cannot be null");
            }
            String tableName = table.getTableName();
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("input table has no name");
            }
            if (inputs.containsKey(tableName)) {
                throw new IllegalArgumentException("duplicate input table name: " + tableName);
            }
            List<Object[]> rows = RowTransformUtils.materializeRows(table);
            inputs.put(tableName, RowTransformUtils.convertToMapList(rows, table.getDataFields()));
        }

        ExecuteData response = client.call(
                url, inputs, context.getVariables(), context.getMetricsTags());
        List<Map<String, Object>> dataRows = response.getData();

        if (dataRows == null || dataRows.isEmpty()) {
            String msg = response.getMsg();
            throw new RuntimeException("remote sqlrec api call failed: "
                    + (msg == null ? "remote api returned empty data" : msg));
        }

        // infer output fields from response rows
        List<RelDataTypeField> dataFields = SchemaInference.inferFields(dataRows);

        // build output rows (ARRAY values kept as List object; Map/complex values serialized to JSON string)
        List<Object[]> outRows = new ArrayList<>(dataRows.size());
        for (Map<String, Object> rowMap : dataRows) {
            outRows.add(RowTransformUtils.projectRow(dataFields, field -> {
                Object value = rowMap.get(field.getName());
                if (field.getType().getSqlTypeName() != SqlTypeName.ARRAY
                        && (value instanceof List || value instanceof Map)) {
                    return JsonUtils.toJson(value);
                }
                return value;
            }));
        }

        return new CacheTable("output", Linq4j.asEnumerable(outRows), dataFields);
    }

    @FunctionalInterface
    public interface RemoteFunctionClient {
        ExecuteData call(
                String url,
                Map<String, List<Map<String, Object>>> data,
                Map<String, String> params,
                Map<String, String> metricTags);
    }
}
