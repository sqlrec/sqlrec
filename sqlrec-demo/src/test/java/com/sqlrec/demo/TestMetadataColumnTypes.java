package com.sqlrec.demo;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.FlinkTableDdl;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.SchemaAccess;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.LocalHdfsAccess;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TestMetadataColumnTypes {
    @Test
    void fileDefinitionsRemainQueryableWithDdlDisabled(@TempDir Path directory) throws Exception {
        assertQueryable(true, directory);
    }

    @Test
    void flinkDefinitionsStoredInHmsRemainQueryable(@TempDir Path directory) throws Exception {
        assertQueryable(false, directory);
    }

    private void assertQueryable(boolean fileMode, Path directory) throws Exception {
        boolean previousFlag = SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.getDefaultValue();
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(false);
        try (var configs = mockStatic(SqlRecConfigs.class, CALLS_REAL_METHODS);
             var factory = mockStatic(MetadataAccessFactory.class)) {
            configs.when(SqlRecConfigs::isFileSystemMetadata).thenReturn(fileMode);
            Path data = directory.resolve("items.csv");
            Files.writeString(data, "id,clock,event,fixed,varying,precise_clock,precise_event,fixed_bytes,varying_bytes,local_event\n"
                    + "1,12:34:56,2026-01-02 12:34:56,a,abc,12:34:56.123,2026-01-02 12:34:56.123,abcdefgh,abc,2026-01-02 12:34:56.123\n");
            var create = (SqlCreateTable) CompileManager.parseSql("CREATE TABLE items (id BIGINT, "
                    + "clock_value TIME, event_time TIMESTAMP, fixed_value BINARY, varying_value VARBINARY, "
                    + "precise_clock TIME(3), precise_event TIMESTAMP(3), fixed_bytes BINARY(8), "
                    + "varying_bytes VARBINARY(16), local_event TIMESTAMP_LTZ(3), "
                    + "PRIMARY KEY(id) NOT ENFORCED) WITH ('connector'='filesystem', 'path'='"
                    + data.toString().replace("'", "''") + "')");
            SchemaAccess schema;
            if (fileMode) {
                schema = new SqlFileSchemaAccess(List.of(create), List.of());
            } else {
                // Exercise the real HiveCatalog serialization, simulating only HMS I/O.
                var table = HiveTableUtil.instantiateHiveTable(new ObjectPath("default", "items"),
                        FlinkTableDdl.create(create), new HiveConf(), false);
                assertTrue(table.getSd().getCols().isEmpty());
                schema = mock(SchemaAccess.class);
                when(schema.getDatabases()).thenReturn(List.of("default"));
                when(schema.getTables("default")).thenReturn(List.of(table));
            }
            var metadata = new MetadataAccess(schema,
                    new InMemoryStoreAccess(List.of(), List.of(), List.of()), new LocalHdfsAccess());
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            CalciteSchemaFactory.setGlobalSchema(null);
            CacheManager.invalidateAll();
            var executor = new SqlExecutor();
            assertArrayEquals(new Object[]{1L}, executor.executeSql("SELECT id FROM items").scan(null).single());
            var result = executor.executeSql("SELECT * FROM items");
            assertEquals(List.of(SqlTypeName.BIGINT, SqlTypeName.TIME, SqlTypeName.TIMESTAMP,
                            SqlTypeName.BINARY, SqlTypeName.VARBINARY, SqlTypeName.TIME, SqlTypeName.TIMESTAMP,
                            SqlTypeName.BINARY, SqlTypeName.VARBINARY, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                    result.getDataFields().stream().map(field -> field.getType().getSqlTypeName()).toList());
            Object[] row = result.scan(null).single();
            assertEquals(10, row.length);
            for (Object value : row) assertNotNull(value);
        } finally {
            CacheManager.invalidateAll();
            CalciteSchemaFactory.setGlobalSchema(null);
            SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(previousFlag);
        }
    }
}
