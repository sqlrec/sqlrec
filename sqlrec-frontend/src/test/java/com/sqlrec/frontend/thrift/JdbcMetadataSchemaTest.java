package com.sqlrec.frontend.thrift;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.executor.SqlProcessResult;
import org.junit.jupiter.api.Test;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class JdbcMetadataSchemaTest {
    @Test
    void everyMetadataResponsePreservesItsOrderedColumnNamesAndWireTypes() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        JdbcMetadataExecutor executor = new JdbcMetadataExecutor(metadata);
        Map<String, SqlProcessResult> results = new LinkedHashMap<>();
        results.put("catalogs", executor.catalogs());
        results.put("schemas", executor.schemas("other", null));
        results.put("table_types", executor.tableTypes());
        results.put("tables", executor.tables("other", null, null, null));
        results.put("columns", executor.columns("other", null, null, null));
        results.put("functions", executor.functions("other", null, null));
        results.put("primary_keys", executor.primaryKeys("other", null, null));
        results.put("type_info", executor.typeInfo());

        JsonObject actual = new JsonObject();
        for (var result : results.entrySet()) {
            JsonArray fields = new JsonArray();
            for (var field : result.getValue().getFields()) {
                fields.add(field.getName() + ":" + field.getType().getSqlTypeName());
            }
            actual.add(result.getKey(), fields);
            for (Object[] row : result.getValue().getEnumerable()) {
                assertEquals(fields.size(), row.length, result.getKey());
            }
        }
        try (var input = getClass().getResourceAsStream("/jdbc-metadata-schemas.json")) {
            assertNotNull(input);
            assertEquals(JsonParser.parseReader(new InputStreamReader(input, StandardCharsets.UTF_8)), actual);
        }
        verifyNoInteractions(metadata);
    }
}
