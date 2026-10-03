package com.sqlrec.db.remote;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.FlinkTableDdl;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Exercises pure definition conversion with Flink's type factory and no HMS connection. */
class FlinkTableDdlTest {
    private CatalogManager manager;
    private DataTypeFactory types;

    @BeforeEach
    void setUp() {
        HiveCatalog catalog = mock(HiveCatalog.class);
        when(catalog.getDefaultDatabase()).thenReturn("default");
        manager = FlinkHiveDdlAdapter.openCatalog(catalog);
        types = manager.getDataTypeFactory();
    }

    @AfterEach
    void tearDown() {
        if (manager != null) manager.close();
    }

    @Test
    void fileAndCatalogConversionsProduceTheSameSchema() throws Exception {
        String sql = "CREATE TABLE items (id BIGINT, nested ROW<label STRING, values_array ARRAY<INT>>, "
                + "attributes MAP<STRING, DECIMAL(12, 3)>, payload BYTES, event_time TIMESTAMP_LTZ(3) "
                + "METADATA FROM 'timestamp' VIRTUAL, PRIMARY KEY (id) NOT ENFORCED) "
                + "COMMENT 'same definition' WITH ('connector'='not_installed')";
        var catalog = create(sql);
        var file = FlinkTableDdl.create((SqlCreateTable) CompileManager.parseSql(sql));
        assertEquals(catalog.getResolvedSchema(), file.getResolvedSchema());
        assertEquals(catalog.getOptions(), file.getOptions());
        assertEquals(catalog.getComment(), file.getComment());
    }

    @Test
    void createValidatesAndAppliesPrimaryKeyNullabilityWithoutCallerPreparation() throws Exception {
        ResolvedCatalogTable table = create("CREATE TABLE items (id BIGINT, category STRING, "
                + "PRIMARY KEY (id) NOT ENFORCED) COMMENT 'review' PARTITIONED BY (category) "
                + "WITH ('connector'='filesystem', 'path'='file:///tmp/items')");
        assertEquals(DataTypes.BIGINT().notNull(), table.getResolvedSchema().getColumn("id").orElseThrow().getDataType());
        assertEquals(List.of("id"), table.getResolvedSchema().getPrimaryKey().orElseThrow().getColumns());
        assertEquals(List.of("category"), table.getPartitionKeys());
        assertEquals("review", table.getComment());
        assertEquals("file:///tmp/items", table.getOptions().get("path"));
        assertThrows(ValidationException.class, () -> create("CREATE TABLE invalid_table (id INT, id STRING)"));
    }

    @Test
    void likeDefaultsInheritTheSchemaAndOverwriteOptionsWithoutChangingTheSource() throws Exception {
        ResolvedCatalogTable source = source();
        ResolvedCatalogTable copy = like("CREATE TABLE copied COMMENT 'copy' WITH ('path'='file:///tmp/copy') LIKE items", source);
        assertEquals(source.getResolvedSchema(), copy.getResolvedSchema());
        assertEquals(source.getPartitionKeys(), copy.getPartitionKeys());
        assertEquals("copy", copy.getComment());
        assertEquals("filesystem", copy.getOptions().get("connector"));
        assertEquals("file:///tmp/copy", copy.getOptions().get("path"));
        assertEquals("file:///tmp/items", source.getOptions().get("path"));
        assertEquals("source", source.getComment());
    }

    @Test
    void likeSpecificStrategiesOverrideAllAndPreservePhysicalColumns() throws Exception {
        ResolvedCatalogTable source = source();
        ResolvedCatalogTable excluded = like("CREATE TABLE copied LIKE items (EXCLUDING ALL)", source);
        assertEquals(List.of("id", "category"), excluded.getResolvedSchema().getColumnNames());
        assertTrue(excluded.getResolvedSchema().getPrimaryKey().isEmpty());
        assertEquals(source.getPartitionKeys(), excluded.getPartitionKeys());
        assertTrue(excluded.getOptions().isEmpty());

        ResolvedCatalogTable included = like("CREATE TABLE copied LIKE items "
                + "(EXCLUDING ALL INCLUDING OPTIONS INCLUDING CONSTRAINTS INCLUDING PARTITIONS INCLUDING METADATA)", source);
        assertEquals(source.getResolvedSchema(), included.getResolvedSchema());
        assertEquals(source.getPartitionKeys(), included.getPartitionKeys());
        assertEquals(source.getOptions(), included.getOptions());
    }

    @Test
    void likeChecksOptionConflictsAndSupportsExcludingSourceOptions() throws Exception {
        ResolvedCatalogTable source = source();
        assertThrows(ValidationException.class, () -> like(
                "CREATE TABLE copied WITH ('path'='file:///tmp/copy') LIKE items (INCLUDING OPTIONS)", source));
        ResolvedCatalogTable copy = like("CREATE TABLE copied WITH ('path'='file:///tmp/copy') "
                + "LIKE items (EXCLUDING OPTIONS)", source);
        assertEquals(Map.of("path", "file:///tmp/copy"), copy.getOptions());
    }

    @Test
    void excludingPartitionsOnlyReplacesThemWhenNewPartitionsAreDeclared() throws Exception {
        ResolvedCatalogTable source = source();
        ResolvedCatalogTable inherited = like("CREATE TABLE copied LIKE items (EXCLUDING PARTITIONS)", source);
        assertEquals(List.of("category"), inherited.getPartitionKeys());
        ResolvedCatalogTable replaced = like("CREATE TABLE copied PARTITIONED BY (id) LIKE items (EXCLUDING PARTITIONS)", source);
        assertEquals(List.of("id"), replaced.getPartitionKeys());
        assertEquals(List.of("category"), source.getPartitionKeys());
        assertThrows(ValidationException.class, () -> like("CREATE TABLE copied PARTITIONED BY (id) LIKE items", source));
    }

    @Test
    void likeOnlyOverwritesMetadataColumnsWhenRequested() throws Exception {
        ResolvedCatalogTable source = source();
        String sql = "CREATE TABLE copied (origin STRING METADATA FROM 'replacement' VIRTUAL) LIKE items";
        assertThrows(ValidationException.class, () -> like(sql, source));
        ResolvedCatalogTable copy = like(sql + " (OVERWRITING METADATA)", source);
        var column = (Column.MetadataColumn) copy.getResolvedSchema().getColumn("origin").orElseThrow();
        assertEquals(Optional.of("replacement"), column.getMetadataKey());
        assertTrue(column.isVirtual());
        assertEquals(Optional.of("origin_key"), ((Column.MetadataColumn) source.getResolvedSchema()
                .getColumn("origin").orElseThrow()).getMetadataKey());
    }

    @Test
    void multipleColumnChangesPreserveOrderAndLeaveTheSourceUnchanged() throws Exception {
        ResolvedCatalogTable original = create("CREATE TABLE items (id BIGINT, category STRING)");
        ResolvedCatalogTable added = alter("ALTER TABLE items ADD "
                + "(note STRING FIRST, amount INT AFTER id, flag BOOLEAN)", original);
        assertEquals(List.of("note", "id", "amount", "category", "flag"), added.getResolvedSchema().getColumnNames());
        ResolvedCatalogTable modified = alter("ALTER TABLE items MODIFY "
                + "(flag BOOLEAN FIRST, note VARCHAR(100) COMMENT 'updated' AFTER amount)", added);
        assertEquals(List.of("flag", "id", "amount", "note", "category"), modified.getResolvedSchema().getColumnNames());
        assertEquals(DataTypes.VARCHAR(100), modified.getResolvedSchema().getColumn("note").orElseThrow().getDataType());
        assertEquals(Optional.of("updated"), modified.getResolvedSchema().getColumn("note").orElseThrow().getComment());
        assertEquals(List.of("id", "category"), original.getResolvedSchema().getColumnNames());
        assertEquals(List.of("note", "id", "amount", "category", "flag"), added.getResolvedSchema().getColumnNames());
    }

    @Test
    void renameUpdatesKeysAndPartitionsAndPreservesImplicitMetadataKeys() throws Exception {
        ResolvedCatalogTable original = create("CREATE TABLE items (id BIGINT, origin STRING METADATA VIRTUAL, "
                + "PRIMARY KEY (id) NOT ENFORCED) PARTITIONED BY (id)");
        ResolvedCatalogTable renamed = alter("ALTER TABLE items RENAME id TO identity_id", original);
        assertEquals(List.of("identity_id"), renamed.getPartitionKeys());
        assertEquals(List.of("identity_id"), renamed.getResolvedSchema().getPrimaryKey().orElseThrow().getColumns());
        renamed = alter("ALTER TABLE items RENAME origin TO source_name", renamed);
        var column = (Column.MetadataColumn) renamed.getResolvedSchema().getColumn("source_name").orElseThrow();
        assertTrue(column.getMetadataKey().isEmpty());
        assertTrue(column.isVirtual());
        assertEquals(List.of("id"), original.getPartitionKeys());
        assertTrue(original.getResolvedSchema().getColumn("origin").isPresent());
    }

    @Test
    void setAndResetPreserveSchemaCommentPartitionsAndSnapshot() throws Exception {
        ResolvedCatalogTable source = source();
        var original = new ResolvedCatalogTable(CatalogTable.of(source.getUnresolvedSchema(), source.getComment(),
                source.getPartitionKeys(), source.getOptions(), 42L), source.getResolvedSchema());
        ResolvedCatalogTable changed = alter("ALTER TABLE items SET ('path'='file:///tmp/changed', 'review'='value')", original);
        changed = alter("ALTER TABLE items RESET ('review')", changed);
        assertEquals(original.getResolvedSchema(), changed.getResolvedSchema());
        assertEquals(original.getComment(), changed.getComment());
        assertEquals(original.getPartitionKeys(), changed.getPartitionKeys());
        assertEquals(Optional.of(42L), changed.getSnapshot());
        assertEquals("file:///tmp/changed", changed.getOptions().get("path"));
        assertFalse(changed.getOptions().containsKey("review"));
        assertEquals("file:///tmp/items", original.getOptions().get("path"));
    }

    @Test
    void invalidChangesAndUnsupportedColumnsDoNotMutateTheSource() throws Exception {
        ResolvedCatalogTable original = source();
        for (String sql : new String[]{"ALTER TABLE items DROP id", "ALTER TABLE items DROP category",
                "ALTER TABLE items RESET ('connector')", "ALTER TABLE items RENAME id TO category",
                "ALTER TABLE items ADD extra AS id + 1", "ALTER TABLE items ADD WATERMARK FOR id AS id"}) {
            assertThrows(RuntimeException.class, () -> alter(sql, original), sql);
        }
        assertEquals(List.of("id", "category", "origin"), original.getResolvedSchema().getColumnNames());
        assertEquals(List.of("category"), original.getPartitionKeys());
        assertEquals("file:///tmp/items", original.getOptions().get("path"));
        assertThrows(UnsupportedOperationException.class, () -> create("CREATE TABLE items (id INT, extra AS id + 1)"));
        assertThrows(UnsupportedOperationException.class, () -> create(
                "CREATE TABLE items (ts TIMESTAMP(3), WATERMARK FOR ts AS ts)"));
    }

    private ResolvedCatalogTable source() throws Exception {
        return create("CREATE TABLE items (id BIGINT, category STRING, "
                + "origin STRING METADATA FROM 'origin_key' VIRTUAL, PRIMARY KEY (id) NOT ENFORCED) "
                + "COMMENT 'source' PARTITIONED BY (category) "
                + "WITH ('connector'='filesystem', 'path'='file:///tmp/items')");
    }

    private ResolvedCatalogTable create(String sql) throws Exception {
        return FlinkTableDdl.create((SqlCreateTable) CompileManager.parseSql(sql), types);
    }

    private ResolvedCatalogTable like(String sql, ResolvedCatalogTable source) throws Exception {
        return FlinkTableDdl.createLike((SqlCreateTableLike) CompileManager.parseSql(sql), source, types);
    }

    private ResolvedCatalogTable alter(String sql, ResolvedCatalogTable source) throws Exception {
        return FlinkTableDdl.alter((SqlAlterTable) CompileManager.parseSql(sql), source, types);
    }
}
