package com.sqlrec.frontend.rest;

import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.entity.SqlFunction;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UiApiServiceTest {

    @Test
    void displaysSqlFunctionStatementsFromJsonArray() {
        MetadataAccess metadataAccess = mock(MetadataAccess.class);
        SqlFunction function = new SqlFunction();
        function.setName("example");
        function.setSqlList(JsonUtils.toJson(List.of(
                "create sql function example",
                "return select 'a;b' as value")));
        when(metadataAccess.getSqlFunction("example")).thenReturn(function);

        List<Map<String, String>> rows = new UiApiService(metadataAccess).getFunction("example");

        assertEquals(Map.of("col_name", "SQL 1:", "data_type", "create sql function example"), rows.get(6));
        assertEquals(Map.of("col_name", "SQL 2:", "data_type", "return select 'a;b' as value"), rows.get(7));
        assertEquals(8, rows.size());
    }

    @Test
    void mapsDatabasesToUiItems() throws Exception {
        MetadataAccess metadataAccess = mock(MetadataAccess.class);
        when(metadataAccess.getDatabases()).thenReturn(List.of("default", "analytics"));

        UiApiService service = new UiApiService(metadataAccess);

        assertEquals(
                List.of(
                        Map.of("id", "default", "name", "default"),
                        Map.of("id", "analytics", "name", "analytics")),
                service.listDatabases());
    }

    @Test
    void buildsCheckpointPageFromMetadata() {
        MetadataAccess metadataAccess = mock(MetadataAccess.class);
        when(metadataAccess.getCheckpointCountByModelName("model")).thenReturn(21);
        when(metadataAccess.getCheckpointListByModelNamePaged("model", 2, 10))
                .thenReturn(List.of());

        UiApiService service = new UiApiService(metadataAccess);
        Map<String, Object> result = service.listCheckpoints("model", 2, 10);

        assertEquals(List.of(), result.get("items"));
        assertEquals(21, result.get("total"));
        assertEquals(2, result.get("page"));
        assertEquals(10, result.get("pageSize"));
        assertEquals(3, result.get("totalPages"));
        verify(metadataAccess).getCheckpointListByModelNamePaged("model", 2, 10);
    }
}
