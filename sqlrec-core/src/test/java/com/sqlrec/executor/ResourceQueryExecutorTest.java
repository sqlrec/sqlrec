package com.sqlrec.executor;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ResourceQueryExecutorTest {
    @Test
    void showTablesFiltersLikeAndNotLikeUsingFlinkSemantics() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getTables("review")).thenReturn(List.of(table("items_b"), table("other"), table("items_a")));
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        root.add("review", new AbstractSchema());
        ResourceQueryExecutor executor = new ResourceQueryExecutor(metadata, root);
        assertEquals(List.of("items_a", "items_b"), names(executor.execute(
                CompileManager.parseSql("SHOW TABLES IN hive.REVIEW LIKE 'items_%'"), "default")));
        assertEquals(List.of("other"), names(executor.execute(
                CompileManager.parseSql("SHOW TABLES IN review NOT LIKE 'items_%'"), "default")));
        assertTrue(names(executor.execute(
                CompileManager.parseSql("SHOW TABLES IN review LIKE 'ITEMS_%'"), "default")).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> executor.execute(
                CompileManager.parseSql("SHOW TABLES IN other.review"), "default"));
        assertThrows(RuntimeException.class, () -> executor.execute(
                CompileManager.parseSql("SHOW TABLES IN missing"), "default"));
    }

    private static List<Object> names(SqlProcessResult result) {
        return result.getEnumerable().select(row -> row[0]).toList();
    }

    private static Table table(String name) {
        Table table = new Table();
        table.setTableName(name);
        return table;
    }
}
