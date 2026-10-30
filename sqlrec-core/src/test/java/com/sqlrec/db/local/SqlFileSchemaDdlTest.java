package com.sqlrec.db.local;

import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.executor.SqlProcessResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SqlFileSchemaDdlTest {
    @Test
    void fileAndRuntimeDefinitionsExposeIdenticalMetadata() throws Exception {
        String sql = "CREATE TABLE items (id BIGINT, origin STRING METADATA FROM 'origin_key' VIRTUAL, "
                + "PRIMARY KEY (id) NOT ENFORCED) COMMENT 'same definition' WITH ('connector'='not_installed')";
        var loaded = new SqlFileSchemaAccess(List.of(CompileManager.parseSql(sql)), List.of());
        var runtime = emptySchema();
        ddl(runtime, sql);
        var fileTable = loaded.getTable("default", "items");
        var runtimeTable = runtime.getTable("default", "items");
        assertEquals(HiveTableUtils.getMetadataFields(runtimeTable).stream().map(field -> field.getName()).toList(),
                HiveTableUtils.getMetadataFields(fileTable).stream().map(field -> field.getName()).toList());
        assertEquals(HiveTableUtils.getMetadataFields(runtimeTable).stream().map(field -> field.getType()).toList(),
                HiveTableUtils.getMetadataFields(fileTable).stream().map(field -> field.getType()).toList());
        assertEquals(show(runtime, "items"), show(loaded, "items"));
        ddl(loaded, "ALTER TABLE items RENAME TO renamed");
        assertEquals(List.of("id", "origin"), HiveTableUtils.getMetadataFields(loaded.getTable("default", "renamed"))
                .stream().map(field -> field.getName()).toList());
    }

    @Test
    void internalSchemaWritesDoNotDependOnSqlDdlPermission() throws Exception {
        boolean previous = com.sqlrec.common.config.SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.getDefaultValue();
        com.sqlrec.common.config.SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(false);
        try {
            var files = emptySchema();
            var initial = files.getDatabases();
            assertEquals(List.of("default"), initial);
            ddl(files, "CREATE DATABASE internal_db");
            ddl(files, "CREATE TABLE internal_db.items (id INT)");
            assertEquals(List.of("default", "internal_db"), files.getDatabases());
            assertEquals(1, files.getTables("internal_db").size());
        } finally {
            com.sqlrec.common.config.SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(previous);
        }
    }

    @Test
    void tableLifecycleKeepsDefinitionProjectionAndUpdateTimesConsistent() throws Exception {
        var files = emptySchema();
        ddl(files, "CREATE TABLE items (id BIGINT, category STRING, origin STRING METADATA FROM 'origin_key' VIRTUAL, "
                + "PRIMARY KEY (id) NOT ENFORCED) COMMENT 'it''s a table' PARTITIONED BY (category) "
                + "WITH ('connector'='not_installed', 'path'='before')");
        long created = HiveTableUtils.getTableModificationTime(files.getTable("default", "items"));
        assertTrue(created > 0 && created <= System.currentTimeMillis());
        assertEquals(0, created % 1000);
        assertEquals(List.of("id", "category", "origin"), names(query(files, "DESCRIBE items")));
        assertEquals("id", HiveTableUtils.getTablePrimaryKey(files.getTable("default", "items")));
        assertEquals("before", HiveTableUtils.getFlinkTableOptions(files.getTable("default", "items")).get("path"));

        ddl(files, "ALTER TABLE items ADD amount DECIMAL(12, 3) AFTER id");
        ddl(files, "ALTER TABLE items MODIFY amount DECIMAL(14, 4) FIRST");
        ddl(files, "ALTER TABLE items RENAME id TO identity_id");
        ddl(files, "ALTER TABLE items SET ('path'='after', 'extra'='value')");
        ddl(files, "ALTER TABLE items RESET ('extra')");
        assertEquals(List.of("amount", "identity_id", "category", "origin"), names(query(files, "DESCRIBE items")));
        var projection = files.getTable("default", "items");
        assertEquals("identity_id", HiveTableUtils.getTablePrimaryKey(projection));
        assertEquals(List.of("amount", "identity_id", "category", "origin"),
                HiveTableUtils.getMetadataFields(projection).stream().map(field -> field.getName()).toList());
        assertEquals("after", HiveTableUtils.getFlinkTableOptions(projection).get("path"));
        assertFalse(HiveTableUtils.getFlinkTableOptions(projection).containsKey("extra"));
        String shown = show(files, "items");
        assertTrue(shown.contains("it''s a table"));
        assertTrue(shown.contains("METADATA"));
        assertTrue(shown.contains("PARTITIONED BY"));
        assertNotNull(CompileManager.parseSql(shown));

        ddl(files, "ALTER TABLE items DROP PRIMARY KEY");
        ddl(files, "ALTER TABLE items DROP identity_id");
        ddl(files, "ALTER TABLE items RENAME TO renamed");
        assertThrows(NoSuchObjectException.class, () -> files.getTable("default", "items"));
        assertTrue(show(files, "renamed").contains("`renamed`"));
        ddl(files, "DROP TABLE renamed");
        assertTrue(files.getTables("default").isEmpty());
        ddl(files, "DROP TABLE IF EXISTS renamed");
        ddl(files, "ALTER TABLE IF EXISTS renamed SET ('path'='ignored')");
    }

    @Test
    void likeResolvesAndCopiesTheSourceWithoutRetainingADependency() throws Exception {
        var files = emptySchema();
        ddl(files, "CREATE TABLE source (id BIGINT, category STRING, PRIMARY KEY (id) NOT ENFORCED) "
                + "PARTITIONED BY (category) WITH ('connector'='not_installed', 'path'='source')");
        ddl(files, "CREATE TABLE copied WITH ('path'='copy') LIKE source");
        assertEquals(List.of("id", "category"), names(query(files, "DESCRIBE copied")));
        assertFalse(show(files, "copied").contains("LIKE"));
        assertEquals("copy", HiveTableUtils.getFlinkTableOptions(files.getTable("default", "copied")).get("path"));
        ddl(files, "ALTER TABLE source ADD extra INT");
        ddl(files, "DROP TABLE source");
        assertEquals(List.of("id", "category"), names(query(files, "DESCRIBE copied")));
    }

    @Test
    void failedAndNoOpChangesLeaveExistingDefinitionsUnchanged() throws Exception {
        var files = emptySchema();
        ddl(files, "CREATE TABLE items (id BIGINT, PRIMARY KEY (id) NOT ENFORCED)");
        String before = show(files, "items");
        long version = HiveTableUtils.getTableModificationTime(files.getTable("default", "items"));
        assertThrows(AlreadyExistsException.class, () -> ddl(files, "CREATE TABLE ITEMS (other INT)"));
        ddl(files, "CREATE TABLE IF NOT EXISTS ITEMS (other INT)");
        for (String sql : List.of("ALTER TABLE items DROP id", "ALTER TABLE items ADD id STRING",
                "ALTER TABLE items RESET ('connector')", "ALTER TABLE items ADD extra AS id + 1",
                "ALTER TABLE items ADD WATERMARK FOR id AS id", "ALTER TABLE items RENAME TO other.items")) {
            assertThrows(Exception.class, () -> ddl(files, sql), sql);
            assertEquals(before, show(files, "items"), sql);
            assertEquals(version, HiveTableUtils.getTableModificationTime(files.getTable("default", "items")), sql);
        }
        assertThrows(NoSuchObjectException.class, () -> ddl(files, "CREATE TABLE missing.items (id INT)"));
        assertThrows(UnsupportedOperationException.class, () -> ddl(files, "CREATE TABLE other.defaultdb.items (id INT)"));
        ddl(files, "CREATE TABLE destination (id INT)");
        assertThrows(AlreadyExistsException.class, () -> ddl(files, "ALTER TABLE items RENAME TO destination"));
        assertEquals(before, show(files, "items"));
    }

    @Test
    void databaseLifecycleReturnsIndependentListsAndCascadeRemovesUdfs() throws Exception {
        var files = emptySchema();
        var initial = files.getDatabases();
        ddl(files, "CREATE DATABASE review COMMENT 'description' WITH ('owner'='test')");
        var created = files.getDatabases();
        assertEquals(List.of("default"), initial);
        files.getDatabases().clear();
        assertEquals(List.of("default", "review"), files.getDatabases());
        assertThrows(AlreadyExistsException.class, () -> ddl(files, "CREATE DATABASE REVIEW"));
        ddl(files, "CREATE DATABASE IF NOT EXISTS REVIEW");
        ddl(files, "ALTER DATABASE REVIEW SET ('owner'='updated')");
        assertEquals(created, files.getDatabases());
        ddl(files, "CREATE FUNCTION review.f AS 'missing.Udf'");
        assertThrows(ValidationException.class, () -> ddl(files, "DROP DATABASE review"));
        assertThrows(ValidationException.class,
                () -> files.executeMetadataDdl(CompileManager.parseSql("DROP DATABASE REVIEW CASCADE"), "review"));
        ddl(files, "DROP DATABASE review CASCADE");
        assertThrows(NoSuchObjectException.class, () -> files.getFunction("review", "f"));
        var dropped = files.getDatabases();
        ddl(files, "DROP DATABASE IF EXISTS review");
        assertEquals(dropped, files.getDatabases());
        assertEquals(List.of("default"), files.getDatabases());
        ddl(files, "CREATE DATABASE review");
        assertTrue(files.getFunctions("review").isEmpty());
    }

    @Test
    void javaUdfsPreserveResourcesAndReturnDefensiveCopiesWithoutLoadingClasses() throws Exception {
        var files = emptySchema();
        ddl(files, "CREATE FUNCTION f AS 'missing.Udf' LANGUAGE JAVA USING JAR 'file:///missing.jar'");
        var function = files.getFunction("DEFAULT", "F");
        assertEquals("missing.Udf", function.getClassName());
        assertEquals(ResourceType.JAR, function.getResourceUris().get(0).getResourceType());
        assertEquals("file:///missing.jar", function.getResourceUris().get(0).getUri());
        function.setClassName("corrupted");
        function.getResourceUris().clear();
        assertEquals("missing.Udf", files.getFunction("default", "f").getClassName());
        assertEquals(1, files.getFunctions("default").get(0).getResourceUrisSize());
        assertThrows(AlreadyExistsException.class, () -> ddl(files, "CREATE FUNCTION F AS 'missing.Other'"));
        ddl(files, "CREATE FUNCTION IF NOT EXISTS F AS 'missing.Other'");
        ddl(files, "ALTER FUNCTION f AS 'missing.Updated'");
        assertEquals("missing.Updated", files.getFunction("default", "f").getClassName());
        assertEquals(1, files.getFunction("default", "f").getResourceUrisSize());
        assertEquals(List.of("f"), names(query(files, "SHOW USER FUNCTIONS")));
        assertThrows(UnsupportedOperationException.class, () -> ddl(files, "CREATE FUNCTION python_f AS 'f' LANGUAGE PYTHON"));
        ddl(files, "DROP FUNCTION F");
        ddl(files, "DROP FUNCTION IF EXISTS f");
        ddl(files, "ALTER FUNCTION IF EXISTS f AS 'missing.Ignored'");
        assertTrue(files.getFunctions("default").isEmpty());
    }

    @Test
    void tableResultsAreDefensiveCopiesAndQuotedIdentifiersAreResolvedByParts() throws Exception {
        var files = emptySchema();
        ddl(files, "CREATE DATABASE `db.name`");
        ddl(files, "CREATE TABLE hive.`db.name`.`table.name` (id INT) WITH ('connector'='not_installed')");
        var table = files.getTable("DB.NAME", "TABLE.NAME");
        table.getSd().getCols().clear();
        table.getParameters().clear();
        files.getTables("db.name").get(0).setTableName("corrupted");
        assertEquals(1, files.getTable("db.name", "table.name").getSd().getColsSize());
        assertEquals(List.of("id"), names(query(files, "DESCRIBE hive.`db.name`.`table.name`")));
        assertTrue(show(files, "hive.`db.name`.`table.name`").contains("`table.name`"));
    }

    @Test
    void runtimeChangesNeverRewriteFilesAndRestartRestoresDefinitions(@TempDir Path directory) throws Exception {
        String original = "CREATE TABLE items (id INT) WITH ('connector'='not_installed');";
        Path file = directory.resolve("schema.sql");
        Files.writeString(file, original);
        SqlFileParser parser = new SqlFileParser(directory.toString());
        parser.load();
        var files = new SqlFileSchemaAccess(parser.getTableNodes(), parser.getUdfFunctionNodes());
        ddl(files, "ALTER TABLE items ADD extra STRING");
        ddl(files, "CREATE TABLE runtime_only (id INT)");
        ddl(files, "DROP TABLE items");
        assertEquals(original, Files.readString(file));
        SqlFileParser restartedParser = new SqlFileParser(directory.toString());
        restartedParser.load();
        var restarted = new SqlFileSchemaAccess(restartedParser.getTableNodes(), restartedParser.getUdfFunctionNodes());
        assertEquals(List.of("items"), restarted.getTables("default").stream().map(table -> table.getTableName()).toList());
        assertEquals(List.of("id"), names(query(restarted, "DESCRIBE items")));
    }

    @Test
    void concurrentCreatesHaveExactlyOneWinnerAndReadersReceiveCompleteDefinitions() throws Exception {
        var files = emptySchema();
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(6)) {
            List<Future<Boolean>> attempts = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                attempts.add(workers.submit(() -> {
                    start.await();
                    try {
                        ddl(files, "CREATE TABLE items (id INT, label STRING)");
                        return true;
                    } catch (AlreadyExistsException expected) {
                        assertEquals(2, files.getTable("default", "items").getSd().getColsSize());
                        return false;
                    }
                }));
            }
            start.countDown();
            int successes = 0;
            for (var attempt : attempts) if (attempt.get()) successes++;
            assertEquals(1, successes);
            assertEquals(1, files.getTables("default").size());
        }
    }

    @Test
    void unsupportedDdlIsRejectedWithoutChangingTheStore() throws Exception {
        var files = emptySchema();
        for (String sql : List.of("CREATE TABLE t AS SELECT 1 AS id", "CREATE OR REPLACE TABLE t AS SELECT 1 AS id",
                "CREATE TEMPORARY TABLE t (id INT)", "CREATE TABLE t (id INT, extra AS id + 1)",
                "CREATE TABLE t (ts TIMESTAMP(3), WATERMARK FOR ts AS ts)", "CREATE VIEW v AS SELECT 1 AS id",
                "CREATE TEMPORARY FUNCTION f AS 'missing.Udf'",
                "CREATE TEMPORARY SYSTEM FUNCTION f AS 'missing.Udf'",
                "ALTER TEMPORARY FUNCTION f AS 'missing.Udf'", "DROP TEMPORARY FUNCTION f",
                "DROP TEMPORARY SYSTEM FUNCTION f", "DROP TEMPORARY TABLE t")) {
            assertThrows(UnsupportedOperationException.class, () -> ddl(files, sql), sql);
        }
        ddl(files, "CREATE TABLE items (id INT) PARTITIONED BY (id)");
        String before = show(files, "items");
        for (String sql : List.of("ALTER TABLE items ADD PARTITION (id=1)", "ALTER TABLE items DROP PARTITION (id=1)")) {
            assertThrows(UnsupportedOperationException.class, () -> ddl(files, sql), sql);
            assertEquals(before, show(files, "items"));
        }
        assertThrows(org.apache.calcite.sql.parser.SqlParseException.class,
                () -> ddl(files, "ALTER TABLE items PARTITION (id='1') SET ('path'='ignored')"));
        assertEquals(before, show(files, "items"));
    }

    @Test
    void missingObjectsFailWithoutMutationAndIfExistsVariantsAreNoOps() throws Exception {
        var files = emptySchema();
        var before = files.getDatabases();
        for (String sql : List.of("ALTER DATABASE missing SET ('owner'='test')", "DROP DATABASE missing",
                "ALTER TABLE missing ADD extra INT", "DROP TABLE missing",
                "ALTER FUNCTION missing AS 'missing.Udf'", "DROP FUNCTION missing")) {
            assertThrows(NoSuchObjectException.class, () -> ddl(files, sql), sql);
            assertEquals(before, files.getDatabases(), sql);
        }
        for (String sql : List.of("DROP DATABASE IF EXISTS missing", "ALTER TABLE IF EXISTS missing ADD extra INT",
                "DROP TABLE IF EXISTS missing", "ALTER FUNCTION IF EXISTS missing AS 'missing.Udf'",
                "DROP FUNCTION IF EXISTS missing")) {
            ddl(files, sql);
            assertEquals(before, files.getDatabases(), sql);
        }
        assertTrue(files.getTables("default").isEmpty());
        assertTrue(files.getFunctions("default").isEmpty());
        assertThrows(NoSuchObjectException.class, () -> files.getFunction("default", "missing"));
    }

    @Test
    void defaultDatabaseIsProtectedAndCascadeDropsAllTables() throws Exception {
        var files = emptySchema();
        ddl(files, "CREATE DATABASE review");
        files.executeMetadataDdl(CompileManager.parseSql("CREATE TABLE items (id INT)"), "review");
        var before = files.getDatabases();
        assertThrows(ValidationException.class,
                () -> files.executeMetadataDdl(CompileManager.parseSql("DROP DATABASE `default` CASCADE"), "review"));
        assertThrows(ValidationException.class, () -> ddl(files, "DROP DATABASE review RESTRICT"));
        assertEquals(before, files.getDatabases());
        ddl(files, "DROP DATABASE review CASCADE");
        assertEquals(List.of("default"), files.getDatabases());
        assertTrue(files.getTables("review").isEmpty());
        assertThrows(NoSuchObjectException.class, () -> files.getTable("review", "items"));
    }

    @Test
    void fullyQualifiedDdlUsesTheTargetDatabaseEvenIfTheSessionDatabaseIsMissing() throws Exception {
        var files = emptySchema();
        for (String sql : List.of("CREATE DATABASE review", "CREATE TABLE hive.review.source (id INT)",
                "CREATE TABLE hive.review.copied LIKE hive.review.source",
                "ALTER TABLE hive.review.copied ADD extra STRING", "ALTER TABLE hive.review.copied RENAME TO renamed",
                "CREATE FUNCTION hive.review.f AS 'missing.Udf'", "ALTER FUNCTION hive.review.f AS 'missing.Updated'")) {
            files.executeMetadataDdl(CompileManager.parseSql(sql), "missing_session");
        }
        assertEquals(List.of("id", "extra"), names(query(files, "DESCRIBE review.renamed")));
        assertEquals("missing.Updated", files.getFunction("review", "f").getClassName());
        var before = files.getDatabases();
        for (String sql : List.of("CREATE TABLE review.failed LIKE source", "CREATE TABLE unqualified (id INT)")) {
            assertThrows(NoSuchObjectException.class,
                    () -> files.executeMetadataDdl(CompileManager.parseSql(sql), "missing_session"), sql);
        }
        assertEquals(before, files.getDatabases());
        assertThrows(NoSuchObjectException.class, () -> files.getTable("review", "failed"));
        for (String sql : List.of("DROP FUNCTION hive.review.f", "DROP TABLE hive.review.renamed",
                "DROP TABLE hive.review.source", "DROP DATABASE review")) {
            files.executeMetadataDdl(CompileManager.parseSql(sql), "missing_session");
        }
        assertEquals(List.of("default"), files.getDatabases());
    }

    @Test
    void foreignCatalogDdlNeverMutatesLocalObjects() throws Exception {
        var files = emptySchema();
        ddl(files, "CREATE TABLE items (id INT)");
        String before = show(files, "items");
        var snapshot = files.getDatabases();
        for (String sql : List.of("CREATE DATABASE other.review", "ALTER DATABASE other.review SET ('owner'='test')",
                "DROP DATABASE other.review", "CREATE TABLE other.review.items (id INT)",
                "ALTER TABLE other.review.items ADD extra INT", "DROP TABLE other.review.items",
                "CREATE FUNCTION other.review.f AS 'missing.Udf'", "ALTER FUNCTION other.review.f AS 'missing.Udf'",
                "DROP FUNCTION other.review.f")) {
            assertThrows(UnsupportedOperationException.class, () -> ddl(files, sql), sql);
        }
        assertEquals(snapshot, files.getDatabases());
        assertEquals(before, show(files, "items"));
    }

    @Test
    void concurrentAlterAndReadReturnCompleteTableSnapshots() throws Exception {
        var files = emptySchema();
        ddl(files, "CREATE TABLE items (id INT)");
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(3)) {
            Future<?> writer = workers.submit(() -> {
                start.await();
                for (int i = 0; i < 30; i++) {
                    ddl(files, "ALTER TABLE items ADD extra STRING");
                    ddl(files, "ALTER TABLE items DROP extra");
                }
                return null;
            });
            List<Future<?>> readers = new ArrayList<>();
            for (int i = 0; i < 2; i++) readers.add(workers.submit(() -> {
                start.await();
                for (int j = 0; j < 100; j++) {
                    var table = files.getTable("default", "items");
                    var columns = HiveTableUtils.getMetadataFields(table).stream().map(field -> field.getName()).toList();
                    assertTrue(columns.equals(List.of("id")) || columns.equals(List.of("id", "extra")));
                    assertEquals(columns, table.getSd().getCols().stream().map(column -> column.getName()).toList());
                }
                return null;
            }));
            start.countDown();
            writer.get(10, TimeUnit.SECONDS);
            for (var reader : readers) reader.get(10, TimeUnit.SECONDS);
        }
        assertEquals(List.of("id"), names(query(files, "DESCRIBE items")));
    }

    @Test
    void startupRejectsDuplicateAndUnsupportedDefinitions() throws Exception {
        var create = CompileManager.parseSql("CREATE TABLE items (id INT)");
        assertThrows(AlreadyExistsException.class,
                () -> new SqlFileSchemaAccess(List.of(create, CompileManager.parseSql("CREATE TABLE ITEMS (other INT)")), List.of()));
        assertThrows(UnsupportedOperationException.class,
                () -> new SqlFileSchemaAccess(List.of(), List.of(CompileManager.parseSql(
                        "CREATE FUNCTION f AS 'missing.Python' LANGUAGE PYTHON"))));
        assertThrows(UnsupportedOperationException.class,
                () -> new SqlFileSchemaAccess(List.of(CompileManager.parseSql("CREATE TEMPORARY TABLE t (id INT)")), List.of()));
        var files = new SqlFileSchemaAccess(List.of(create, CompileManager.parseSql(
                "CREATE TABLE IF NOT EXISTS ITEMS (other INT)")), List.of());
        assertEquals(List.of("id"), names(query(files, "DESCRIBE items")));
    }

    @Test
    void fileLikeDefinitionsUseTheSameResolvedRepresentationAsRuntimeLike() throws Exception {
        var files = new SqlFileSchemaAccess(List.of(
                CompileManager.parseSql("CREATE TABLE source (id INT)"),
                CompileManager.parseSql("CREATE TABLE copied LIKE source")), List.of());
        assertEquals(List.of("id"), names(query(files, "DESCRIBE copied")));
        assertFalse(show(files, "copied").contains("LIKE"));
    }

    private static SqlFileSchemaAccess emptySchema() throws Exception {
        return new SqlFileSchemaAccess(List.of(), List.of());
    }

    private static void ddl(SqlFileSchemaAccess files, String sql) throws Exception {
        files.executeMetadataDdl(CompileManager.parseSql(sql), "default");
    }

    private static SqlProcessResult query(SqlFileSchemaAccess files, String sql) throws Exception {
        return files.executeMetadataQuery(CompileManager.parseSql(sql), "default");
    }

    private static String show(SqlFileSchemaAccess files, String table) throws Exception {
        return (String) names(query(files, "SHOW CREATE TABLE " + table)).get(0);
    }

    private static List<Object> names(SqlProcessResult result) {
        return result.getEnumerable().select(row -> row[0]).toList();
    }
}
