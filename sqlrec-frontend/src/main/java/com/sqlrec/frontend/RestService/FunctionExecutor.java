package com.sqlrec.frontend.RestService;

import com.google.gson.Gson;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.FunctionBindable;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionExecutor {
    public static String execute(String apiName, String requestData) throws Exception {
        FunctionBindable functionBindable = CompileManager.getApiBindSqlFunction(apiName);
        if (functionBindable == null) {
            throw new RuntimeException("cant find function for " + apiName);
        }
        CalciteSchema schema = HmsSchema.getHmsCalciteSchema();

        Map<String, Object> params = new Gson().fromJson(requestData, Map.class);
        addTableToSchema(schema, functionBindable, params);

        Enumerable<Object[]> enumerable = functionBindable.bind(schema);
        if (enumerable != null) {
            List<Object[]> results = enumerable.toList();
            Map<String, Object> result = new HashMap<>();
            result.put("data", results);
            return new Gson().toJson(result);
        }
        return "{}";
    }

    private static void addTableToSchema(CalciteSchema schema, FunctionBindable functionBindable, Map<String, Object> params) throws Exception {
        List<Map.Entry<String, List<RelDataTypeField>>> tablePlaceholders = functionBindable.getInputTables();
        for (Map.Entry<String, List<RelDataTypeField>> tablePlaceholder : tablePlaceholders) {
            String tableName = tablePlaceholder.getKey();
            List<RelDataTypeField> dataFields = tablePlaceholder.getValue();
            if (!params.containsKey(tableName)) {
                throw new Exception("table " + tableName + " not found in params");
            }
            Enumerable<Object[]> enumerable = convertDataToEnumerable(params.get(tableName), dataFields);
            CacheTable cacheTable = new CacheTable(tableName, enumerable, dataFields);
            schema.add(tableName, cacheTable);
        }
    }

    public static Enumerable<Object[]> convertDataToEnumerable(Object data, List<RelDataTypeField> dataFields) throws Exception {
        if (!(data instanceof List)) {
            throw new Exception("data must be list");
        }

        List<Object[]> list = new ArrayList<>();
        for (Object o : (List) data) {
            if (!(o instanceof Map)) {
                throw new Exception("data must be list of map");
            }
            Map<String, Object> map = (Map<String, Object>) o;
            Object[] row = new Object[dataFields.size()];
            for (int i = 0; i < dataFields.size(); i++) {
                RelDataTypeField field = dataFields.get(i);
                row[i] = map.get(field.getName());
            }
            list.add(row);
        }

        return Linq4j.asEnumerable(list);
    }
}
