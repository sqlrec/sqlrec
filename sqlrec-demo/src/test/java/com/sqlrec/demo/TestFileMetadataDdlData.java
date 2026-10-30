package com.sqlrec.demo;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.LocalHdfsAccess;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.HmsSchema;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockStatic;

class TestFileMetadataDdlData {
    private static final String DEFINITION = "(id BIGINT, label STRING, PRIMARY KEY (id) NOT ENFORCED) "
            + "WITH ('connector'='filesystem')";
    private boolean previousFlag;
    private MockedStatic<MetadataAccessFactory> factory;
    private SqlExecutor executor;
    private SqlExecutor observer;

    @BeforeEach
    void setUp() throws Exception {
        previousFlag = SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.getDefaultValue();
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(true);
        var files = new SqlFileSchemaAccess(List.of(CompileManager.parseSql(
                "CREATE TABLE preserved " + DEFINITION)), List.of());
        var metadata = new MetadataAccess(files, new InMemoryStoreAccess(List.of(), List.of(), List.of()),
                new LocalHdfsAccess());
        factory = mockStatic(MetadataAccessFactory.class);
        factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
        CalciteSchemaFactory.setGlobalSchema(null);
        CacheManager.invalidateAll();
        executor = new SqlExecutor();
        observer = new SqlExecutor();
        executor.executeSql("INSERT INTO preserved VALUES (1, 'one')");
        assertPreservedRows();
    }

    @AfterEach
    void tearDown() {
        CacheManager.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(null);
        factory.close();
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(previousFlag);
    }

    @Test
    @SilenceLoggers(HmsSchema.class)
    void unrelatedAndNoOpDdlPreserveRowsAcrossSessions() throws Exception {
        for (String sql : List.of("CREATE TABLE unrelated " + DEFINITION,
                "ALTER TABLE unrelated ADD extra INT", "ALTER TABLE unrelated RENAME TO renamed",
                "DROP TABLE renamed", "CREATE TABLE IF NOT EXISTS preserved " + DEFINITION,
                "DROP TABLE IF EXISTS missing", "ALTER TABLE IF EXISTS missing RENAME TO preserved",
                "CREATE DATABASE unrelated_db",
                "ALTER DATABASE unrelated_db SET ('owner'='test')", "DROP DATABASE unrelated_db",
                "CREATE FUNCTION f AS 'missing.Udf'", "ALTER FUNCTION f AS 'missing.Updated'", "DROP FUNCTION f")) {
            executeAndAssertPreserved(sql);
        }
        executeAndAssertPreserved("CREATE SQL FUNCTION local_fn", "RETURN SELECT 7 AS id");
        executeAndAssertPreserved("CREATE API local_api WITH local_fn");
        executeAndAssertPreserved("CREATE OR REPLACE API local_api WITH local_fn");
        executeAndAssertPreserved("DROP API local_api");
        executeAndAssertPreserved("CREATE OR REPLACE SQL FUNCTION local_fn", "RETURN SELECT 8 AS id");
        executeAndAssertPreserved("DROP SQL FUNCTION local_fn");
    }

    @Test
    void failedDdlPreservesDefinitionsAndRowsAcrossSessions() throws Exception {
        var before = executor.executeSql("SHOW CREATE TABLE preserved").scan(null).single()[0];
        assertThrows(AlreadyExistsException.class,
                () -> executor.executeSql("CREATE TABLE preserved (other INT)"));
        assertPreservedRows();
        assertThrows(UnsupportedOperationException.class,
                () -> executor.executeSql("ALTER TABLE preserved ADD extra AS id + 1"));
        assertPreservedRows();
        assertEquals(before, executor.executeSql("SHOW CREATE TABLE preserved").scan(null).single()[0]);
    }

    @Test
    void likeAndAlterTablesRemainQueryableThroughDependentSqlFunctions() throws Exception {
        executor.executeSql("CREATE TABLE source " + DEFINITION);
        executor.executeSql("INSERT INTO source VALUES (2, 'source')");
        executor.executeSql("CREATE TABLE copied LIKE source");
        assertEquals(0, observer.executeSql("SELECT * FROM copied").scan(null).count());
        executor.executeSql("INSERT INTO copied VALUES (3, 'copied')");
        executor.executeSql("CREATE SQL FUNCTION ddl_table_query");
        executor.executeSql("RETURN SELECT * FROM copied");
        assertArrayEquals(new Object[]{3L, "copied"},
                observer.executeSql("CALL ddl_table_query()").scan(null).single());
        executor.executeSql("DROP TABLE source");
        assertArrayEquals(new Object[]{3L, "copied"},
                observer.executeSql("CALL ddl_table_query()").scan(null).single());

        executor.executeSql("ALTER TABLE copied ADD quantity INT");
        executor.executeSql("INSERT INTO copied VALUES (4, 'added', 7)");
        assertArrayEquals(new Object[]{4L, "added", 7},
                observer.executeSql("CALL ddl_table_query()").scan(null).single());
        executor.executeSql("ALTER TABLE copied MODIFY quantity BIGINT");
        executor.executeSql("INSERT INTO copied VALUES (5, 'modified', 8)");
        assertArrayEquals(new Object[]{5L, "modified", 8L},
                observer.executeSql("CALL ddl_table_query()").scan(null).single());
        executor.executeSql("ALTER TABLE copied RENAME id TO item_id");
        executor.executeSql("INSERT INTO copied VALUES (6, 'renamed', 9)");
        assertArrayEquals(new Object[]{6L, "renamed", 9L},
                observer.executeSql("SELECT * FROM copied WHERE item_id = 6").scan(null).single());
        assertEquals(List.of("item_id", "label", "quantity"),
                observer.executeSql("CALL ddl_table_query()").getDataFields().stream()
                        .map(field -> field.getName()).toList());
        executor.executeSql("ALTER TABLE copied DROP quantity");
        executor.executeSql("INSERT INTO copied VALUES (7, 'dropped column')");
        assertArrayEquals(new Object[]{7L, "dropped column"},
                observer.executeSql("CALL ddl_table_query()").scan(null).single());
        executor.executeSql("DROP TABLE copied");
        assertThrows(Exception.class, () -> observer.executeSql("CALL ddl_table_query()"));
        assertPreservedRows();
    }

    @Test
    void dropAndRecreateWithoutAnIntermediateReadStartsWithEmptyData() throws Exception {
        executor.executeSql("DROP TABLE preserved");
        executor.executeSql("CREATE TABLE preserved " + DEFINITION);
        assertEquals(0, executor.executeSql("SELECT * FROM preserved").scan(null).count());
        assertEquals(0, observer.executeSql("SELECT * FROM preserved").scan(null).count());
    }

    @Test
    void renameEvictsTheOldNameAndPreservesOtherTables() throws Exception {
        executeAndAssertPreserved("CREATE TABLE target " + DEFINITION);
        executor.executeSql("INSERT INTO target VALUES (2, 'two')");
        assertEquals(1, executor.executeSql("SELECT * FROM target").scan(null).count());
        executeAndAssertPreserved("ALTER TABLE target RENAME TO renamed");
        executeAndAssertPreserved("CREATE TABLE target " + DEFINITION);
        assertEquals(0, executor.executeSql("SELECT * FROM target").scan(null).count());
        assertEquals(0, executor.executeSql("SELECT * FROM renamed").scan(null).count());
    }

    @Test
    void databaseDropAndRecreateClearsOnlyThatDatabasesRows() throws Exception {
        executeAndAssertPreserved("CREATE DATABASE runtime_db", "CREATE TABLE runtime_db.items " + DEFINITION);
        executor.executeSql("INSERT INTO runtime_db.items VALUES (2, 'two')");
        assertEquals(1, executor.executeSql("SELECT * FROM runtime_db.items").scan(null).count());
        executeAndAssertPreserved("DROP DATABASE runtime_db CASCADE", "CREATE DATABASE runtime_db",
                "CREATE TABLE runtime_db.items " + DEFINITION);
        assertEquals(0, executor.executeSql("SELECT * FROM runtime_db.items").scan(null).count());
    }

    @Test
    void alterRebuildsItsTargetAndFlushStillRebuildsAllTables() throws Exception {
        executeAndAssertPreserved("CREATE TABLE target " + DEFINITION);
        executor.executeSql("INSERT INTO target VALUES (2, 'two')");
        executor.executeSql("ALTER TABLE preserved ADD extra INT");
        var altered = observer.executeSql("SELECT * FROM preserved");
        assertEquals(3, altered.getDataFields().size());
        assertEquals(0, altered.scan(null).count());
        assertEquals(1, observer.executeSql("SELECT * FROM target").scan(null).count());
        executor.executeSql("FLUSH");
        assertEquals(0, observer.executeSql("SELECT * FROM target").scan(null).count());
    }

    private void executeAndAssertPreserved(String... statements) throws Exception {
        for (String sql : statements) executor.executeSql(sql);
        assertPreservedRows();
    }

    private void assertPreservedRows() throws Exception {
        for (SqlExecutor session : List.of(executor, observer)) {
            var rows = session.executeSql("SELECT * FROM preserved WHERE id = 1").scan(null).toList();
            assertEquals(1, rows.size());
            assertArrayEquals(new Object[]{1L, "one"}, rows.get(0));
        }
    }
}
