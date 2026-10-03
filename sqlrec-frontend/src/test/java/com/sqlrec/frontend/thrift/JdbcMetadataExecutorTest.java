package com.sqlrec.frontend.thrift;

import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.remote.HmsClient;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.frontend.utils.ThriftUtils;
import com.sqlrec.udf.config.FunctionConfigs;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JdbcMetadataExecutorTest {
    @Test
    void nativeHivePrimaryKeysAgreeWithColumnNullabilityAndPreserveComments() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getDatabases()).thenReturn(List.of("default"));
        Table table = new Table();
        table.setDbName("default");
        table.setTableName("native_table");
        StorageDescriptor storage = new StorageDescriptor();
        storage.setCols(List.of(new org.apache.hadoop.hive.metastore.api.FieldSchema("id", "bigint", "identifier")));
        table.setSd(storage);
        when(metadata.getTables("default")).thenReturn(List.of(table));
        when(metadata.getTable("default", "native_table")).thenReturn(table);
        SQLPrimaryKey key = new SQLPrimaryKey();
        key.setColumn_name("id");
        key.setKey_seq(1);
        key.setPk_name("native_pk");
        try (var hms = mockStatic(HmsClient.class)) {
            hms.when(() -> HmsClient.getPrimaryKeys("default", "native_table")).thenReturn(List.of(key));
            JdbcMetadataExecutor executor = new JdbcMetadataExecutor(metadata);
            Object[] column = executor.columns(null, null, null, null).getEnumerable().toList().get(0);
            assertEquals("NO", column[17]);
            assertEquals("identifier", column[11]);
            assertEquals("native_pk", executor.primaryKeys(null, "default", "native_table")
                    .getEnumerable().toList().get(0)[5]);
        }
    }

    @Test
    void timestampSizeUsesDisplayWidthAndRadixIsNullForNonNumericTypes() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getDatabases()).thenReturn(List.of("default"));
        Table table = new Table();
        table.setTableName("events");
        table.setParameters(new HashMap<>());
        table.putToParameters("flink.connector", "redis");
        table.putToParameters("flink.schema.0.name", "ts");
        table.putToParameters("flink.schema.0.data-type", "TIMESTAMP(3)");
        table.putToParameters("flink.schema.1.name", "amount");
        table.putToParameters("flink.schema.1.data-type", "DECIMAL(12, 2)");
        when(metadata.getTables("default")).thenReturn(List.of(table));
        List<Object[]> rows = new JdbcMetadataExecutor(metadata).columns(null, null, null, null)
                .getEnumerable().toList();
        assertEquals(23, rows.get(0)[6]);
        assertEquals(3, rows.get(0)[8]);
        assertNull(rows.get(0)[9]);
        assertEquals(12, rows.get(1)[6]);
        assertEquals(2, rows.get(1)[8]);
        assertEquals(10, rows.get(1)[9]);
    }

    @Test
    void nullSchemaPrimaryKeysSearchAllDatabasesAndSkipMissingTables() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getDatabases()).thenReturn(List.of("missing", "present"));
        when(metadata.getTable("missing", "items")).thenThrow(new NoSuchObjectException("dropped"));
        Table table = new Table();
        table.putToParameters("flink.schema.primary-key.columns", "b,a");
        table.putToParameters("flink.schema.primary-key.name", "pk_items");
        when(metadata.getTable("present", "items")).thenReturn(table);
        List<Object[]> rows = new JdbcMetadataExecutor(metadata).primaryKeys(null, null, "items")
                .getEnumerable().toList();
        assertEquals(2, rows.size());
        assertEquals("present", rows.get(0)[1]);
        assertEquals("b", rows.get(0)[3]);
        assertEquals((short) 2, rows.get(1)[4]);
        assertThrows(IllegalArgumentException.class,
                () -> new JdbcMetadataExecutor(metadata).primaryKeys(null, null, null));
    }

    @Test
    void foreignCatalogReturnsEmptyResultsWithoutReadingHms() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        JdbcMetadataExecutor executor = new JdbcMetadataExecutor(metadata);
        assertEquals(0, executor.schemas("other", null).getEnumerable().count());
        assertEquals(0, executor.tables("other", null, null, null).getEnumerable().count());
        assertEquals(0, executor.columns("other", null, null, null).getEnumerable().count());
        assertEquals(0, executor.functions("other", null, null).getEnumerable().count());
        assertEquals(0, executor.primaryKeys("other", null, "items").getEnumerable().count());
        verifyNoInteractions(metadata);
    }

    @Test
    void returnsFullFlinkSchemaAndPagesWithoutRequeryingHms() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getDatabases()).thenReturn(List.of("default"));
        Table table = new Table();
        table.setDbName("default");
        table.setTableName("items");
        table.setTableType("EXTERNAL_TABLE");
        table.setParameters(new HashMap<>());
        table.putToParameters("flink.schema.0.name", "id");
        table.putToParameters("flink.schema.0.data-type", "BIGINT NOT NULL");
        table.putToParameters("flink.schema.1.name", "computed");
        table.putToParameters("flink.schema.1.data-type", "BIGINT");
        table.putToParameters("flink.schema.1.expr", "id + 1");
        table.putToParameters("flink.schema.primary-key.name", "pk_items");
        table.putToParameters("flink.schema.primary-key.columns", "id");
        when(metadata.getTables("default")).thenReturn(List.of(table));
        when(metadata.getTable("default", "items")).thenReturn(table);
        JdbcMetadataExecutor executor = new JdbcMetadataExecutor(metadata);
        SqlProcessResult columns = executor.columns(null, "def%", "items", null);
        assertEquals(24, columns.getFields().size());
        List<Object[]> rows = columns.getEnumerable().toList();
        assertEquals(Types.BIGINT, rows.get(0)[4]);
        assertEquals("NO", rows.get(0)[17]);
        assertEquals("YES", rows.get(1)[23]);
        assertEquals(SqlTypeName.SMALLINT, columns.getFields().get(21).getType().getSqlTypeName());
        // Nullable SMALLINT metadata needs a valid placeholder plus the Thrift null bitmap.
        assertDoesNotThrow(() -> ThriftUtils.convertObjectArrayToTRowSet(columns.getEnumerable(), columns.getFields()));

        SqlOperation operation = SqlOperation.metadata(columns, "metadata");
        var first = operation.fetch(TFetchOrientation.FETCH_NEXT, 1);
        assertEquals(0, first.rows().getStartRowOffset());
        assertTrue(first.hasMoreRows());
        var second = operation.fetch(TFetchOrientation.FETCH_NEXT, Long.MAX_VALUE);
        assertEquals(1, second.rows().getStartRowOffset());
        assertFalse(second.hasMoreRows());
        assertEquals(1, second.rows().getColumns().get(0).getStringVal().getValuesSize());
        assertEquals(2, operation.fetch(TFetchOrientation.FETCH_FIRST, 10).rows().getColumns().get(0).getStringVal().getValuesSize());
        assertTrue(operation.fetch(TFetchOrientation.FETCH_NEXT, 10).rows().getColumns().get(0).getStringVal().getValues().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> operation.fetch(TFetchOrientation.FETCH_NEXT, 0));
        assertThrows(UnsupportedOperationException.class, () -> operation.fetch(TFetchOrientation.FETCH_PRIOR, 1));
        verify(metadata, times(1)).getTables("default");
        assertEquals((short) 1, executor.primaryKeys(null, "default", "items").getEnumerable().toList().get(0)[4]);
    }

    @Test
    void columnFilterPreservesOriginalOrdinalAndComputedColumnMetadata() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getDatabases()).thenReturn(List.of("default"));
        Table table = new Table();
        table.setTableName("items");
        table.putToParameters("flink.connector", "redis");
        table.putToParameters("flink.schema.0.name", "id");
        table.putToParameters("flink.schema.0.data-type", "BIGINT");
        table.putToParameters("flink.schema.1.name", "computed");
        table.putToParameters("flink.schema.1.data-type", "BIGINT");
        table.putToParameters("flink.schema.1.expr", "id + 1");
        table.putToParameters("flink.schema.1.comment", "computed value");
        when(metadata.getTables("default")).thenReturn(List.of(table));

        List<Object[]> rows = new JdbcMetadataExecutor(metadata).columns(null, "default", "items", "computed")
                .getEnumerable().toList();

        assertEquals(1, rows.size());
        assertEquals("computed", rows.get(0)[3]);
        assertEquals(2, rows.get(0)[16]);
        assertEquals("computed value", rows.get(0)[11]);
        assertEquals("YES", rows.get(0)[23]);
        verify(metadata, times(1)).getTables("default");
    }

    @Test
    void persistentFunctionsOverrideBuiltinsWithoutChangingTheirPosition() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getDatabases()).thenReturn(List.of("default"));
        when(metadata.getFunctions("default")).thenReturn(List.of(
                function("add_col", "FirstClass"), function("custom", "CustomClass"),
                function("add_col", "ReplacementClass"), function("ip", "IpOverride")));
        JdbcMetadataExecutor executor = new JdbcMetadataExecutor(metadata);

        SqlProcessResult result = executor.functions(null, null, null);
        List<Object[]> rows = result.getEnumerable().toList();
        List<String> names = rows.stream().map(row -> (String) row[2]).toList();
        List<String> expectedNames = new ArrayList<>(List.of("add_col", "custom", "ip"));
        FunctionConfigs.DEFAULT_JAVA_FUNCTION_CONFIGS.keySet().stream()
                .filter(name -> !name.equals("add_col")).forEach(expectedNames::add);
        FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.keySet().stream()
                .filter(name -> !name.equals("ip")).forEach(expectedNames::add);
        assertEquals(expectedNames, names);
        assertEquals("ReplacementClass", rows.get(0)[3]);
        assertEquals("CustomClass", rows.get(1)[3]);
        assertEquals("IpOverride", rows.get(2)[3]);
        assertEquals((short) DatabaseMetaData.functionResultUnknown, rows.get(0)[4]);
        assertEquals((short) DatabaseMetaData.functionReturnsTable, rows.get(3)[4]);
        assertEquals((short) DatabaseMetaData.functionNoTable, rows.get(rows.size() - 1)[4]);
        assertEquals(SqlTypeName.SMALLINT, result.getFields().get(4).getType().getSqlTypeName());
        assertEquals("default.add_col", rows.get(0)[5]);
        verify(metadata, times(1)).getFunctions("default");

        List<Object[]> filtered = executor.functions(null, "DEF%", "ADD\\_COL").getEnumerable().toList();
        assertEquals(1, filtered.size());
        assertEquals("ReplacementClass", filtered.get(0)[3]);
    }

    private static Function function(String name, String className) {
        Function function = new Function();
        function.setFunctionName(name);
        function.setClassName(className);
        return function;
    }

    @Test
    void treatsJdbcPatternsAsEscapedLikePatterns() {
        assertTrue(JdbcMetadataExecutor.matches("a_b", "a\\_b"));
        assertFalse(JdbcMetadataExecutor.matches("axb", "a\\_b"));
        assertTrue(JdbcMetadataExecutor.matches("a%b", "a\\%b"));
        assertTrue(JdbcMetadataExecutor.matches("a.b", "a.b"));
        assertFalse(JdbcMetadataExecutor.matches("axb", "a.b"));
        assertTrue(JdbcMetadataExecutor.matches("items", "I%"));
    }
}
