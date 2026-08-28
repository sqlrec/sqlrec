package com.sqlrec.frontend.rest;

import com.sqlrec.db.MetadataAccess;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UiApiServiceTest {

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
