package com.sqlrec.udf.table;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonUtils;
import okhttp3.*;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CallSqlRecApiFunction {
    private static final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();

    @SuppressWarnings("unchecked")
    public CacheTable evaluate(ExecuteContext context, String url, CacheTable... tables) {
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

        // build request body: {data, params, metricTags}
        Map<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("data", inputs);
        requestBody.put("params", context.getVariables());
        requestBody.put("metricTags", context.getMetricsTags());
        String bodyJson = JsonUtils.toJson(requestBody);

        // call remote sqlrec api
        String responseJson = callRemoteApi(url, bodyJson);

        // parse response: {msg, data, params}
        Map<String, Object> respMap = JsonUtils.parseJsonToMap(responseJson);
        Object dataObj = respMap.get("data");
        List<Map<String, Object>> dataRows = dataObj instanceof List ? (List<Map<String, Object>>) dataObj : null;

        if (dataRows == null || dataRows.isEmpty()) {
            Object msgObj = respMap.get("msg");
            String msg = msgObj == null ? "remote api returned empty data" : msgObj.toString();
            throw new RuntimeException("remote sqlrec api call failed: " + msg);
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

    private static String callRemoteApi(String url, String bodyJson) {
        try {
            RequestBody body = RequestBody.create(
                    bodyJson,
                    MediaType.parse("application/json; charset=utf-8")
            );
            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .addHeader("Accept", "application/json")
                    .build();
            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new RuntimeException("HTTP request failed with response code: " + response.code());
                }
                return response.body() != null ? response.body().string() : "";
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to call remote sqlrec api: " + e.getMessage(), e);
        }
    }
}
