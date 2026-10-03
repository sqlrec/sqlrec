package com.sqlrec.demo;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.local.SqlFileParser;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockStatic;

public class TestMetadataQuery {
    private static final List<String> TABLES = List.of(
            "demo_category_hot_item", "demo_exposure_item", "demo_user_interest_category",
            "genre_hot_item", "global_hot_item", "item_embedding", "item_table", "itemcf_i2i",
            "rec_log_kafka", "user_exposure_item", "user_interest_genre", "user_recent_click_item", "user_table");
    private static final List<String> USER_FUNCTIONS = List.of("demo_scalar_udf", "demo_table_udf");
    private static final Map<String, String> CREATE_TABLE_SQL = new HashMap<>();
    private static String previousSqlDir;

    private SqlExecutor sqlExecutor;

    @BeforeAll
    static void setUpMetadata() throws URISyntaxException {
        String moduleDir = Paths.get(TestMetadataQuery.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()).getParent().getParent().toString();
        String sqlDir = Paths.get(moduleDir, "src", "main", "sql").toString();
        previousSqlDir = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(sqlDir);

        // Use the same environment substitution as file metadata loading when comparing DDL.
        SqlFileParser parser = new SqlFileParser(sqlDir);
        parser.load();
        for (var node : parser.getTableNodes()) {
            var table = (SqlCreateTable) node;
            CREATE_TABLE_SQL.put(table.getTableName().getSimple(), CompileManager.getSqlStr(table));
        }
    }

    @AfterAll
    static void restoreConfig() {
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(previousSqlDir);
    }

    @BeforeEach
    void setUpExecutor() {
        sqlExecutor = new SqlExecutor();
    }

    @Test
    void testShowDatabasesAndTables() throws Exception {
        CacheTable databases = sqlExecutor.executeSql("SHOW DATABASES");
        assertEquals(List.of("database name"), fields(databases));
        assertEquals(List.of("default"), names(databases));

        for (String sql : List.of("SHOW TABLES", "SHOW TABLES IN `default`", "SHOW TABLES FROM hive.`DEFAULT`")) {
            CacheTable tables = sqlExecutor.executeSql(sql);
            assertEquals(List.of("table name"), fields(tables));
            assertEquals(TABLES, names(tables), sql);
        }
    }

    @Test
    void testShowTablesFilters() throws Exception {
        assertEquals(List.of("demo_category_hot_item", "demo_exposure_item", "demo_user_interest_category"),
                names(sqlExecutor.executeSql("SHOW TABLES LIKE 'demo_%'")));
        assertEquals(TABLES.stream().filter(name -> !name.startsWith("demo_")).toList(),
                names(sqlExecutor.executeSql("SHOW TABLES IN hive.`default` NOT LIKE 'demo_%'")));
        assertTrue(names(sqlExecutor.executeSql("SHOW TABLES LIKE 'DEMO_%'")).isEmpty());
        assertTrue(names(sqlExecutor.executeSql("SHOW TABLES LIKE 'missing_%'")).isEmpty());
    }

    @Test
    void testDescribePersistentTables() throws Exception {
        for (String name : List.of("genre_hot_item", "`default`.genre_hot_item", "hive.`DEFAULT`.GENRE_HOT_ITEM")) {
            CacheTable description = sqlExecutor.executeSql("DESCRIBE " + name);
            assertEquals(List.of("name", "type", "null", "key", "extras", "watermark"), fields(description));
            List<Object[]> rows = description.scan(null).toList();
            assertEquals(3, rows.size());
            assertArrayEquals(new Object[]{"genre", "STRING", false, "PRI(genre)", null, null}, rows.get(0));
            assertArrayEquals(new Object[]{"movie_id", "BIGINT", true, null, null, null}, rows.get(1));
            assertArrayEquals(new Object[]{"score", "FLOAT", true, null, null, null}, rows.get(2));
        }

        // Metadata queries also work for the Milvus and Kafka definitions without scanning their data.
        List<Object[]> vectorColumns = sqlExecutor.executeSql("DESCRIBE item_embedding").scan(null).toList();
        assertEquals(List.of("id", "title", "genres", "embedding"),
                vectorColumns.stream().map(row -> row[0]).toList());
        assertArrayEquals(new Object[]{"genres", "ARRAY<STRING>", true, null, null, null}, vectorColumns.get(2));
        assertArrayEquals(new Object[]{"embedding", "ARRAY<DOUBLE>", true, null, null, null}, vectorColumns.get(3));
        assertEquals(List.of("user_id", "movie_id", "title", "rec_reason", "req_time", "req_id"),
                names(sqlExecutor.executeSql("DESC hive.`default`.rec_log_kafka")));
    }

    @Test
    void testShowCreateTableMatchesFileDefinitions() throws Exception {
        for (String table : TABLES) {
            String expected = CREATE_TABLE_SQL.get(table);
            assertNotNull(expected, table);
            for (String name : List.of(table, "`default`." + table, "hive.`default`." + table)) {
                CacheTable before = sqlExecutor.executeSql("SHOW CREATE TABLE " + name);
                assertEquals(List.of("result"), fields(before));
                assertEquals(List.of(expected), names(before), name);

                // DESCRIBE validates primary keys; it must not change the saved declaration.
                sqlExecutor.executeSql("DESCRIBE " + name);
                assertEquals(List.of(expected), names(sqlExecutor.executeSql("SHOW CREATE TABLE " + name)), name);
            }
        }
    }

    @Test
    void testShowFunctionsAndFilters() throws Exception {
        for (String sql : List.of("SHOW USER FUNCTIONS", "SHOW USER FUNCTIONS IN `default`",
                "SHOW USER FUNCTIONS FROM hive.`DEFAULT`")) {
            CacheTable functions = sqlExecutor.executeSql(sql);
            assertEquals(List.of("function name"), fields(functions));
            assertEquals(USER_FUNCTIONS, names(functions), sql);
        }
        assertEquals(List.of("demo_scalar_udf"),
                names(sqlExecutor.executeSql("SHOW USER FUNCTIONS LIKE 'demo_scalar_%'")));
        assertEquals(List.of("demo_scalar_udf"),
                names(sqlExecutor.executeSql("SHOW USER FUNCTIONS ILIKE 'DEMO_SCALAR_%'")));
        assertTrue(names(sqlExecutor.executeSql("SHOW USER FUNCTIONS LIKE 'DEMO_%'")).isEmpty());
        assertEquals(List.of("demo_table_udf"),
                names(sqlExecutor.executeSql("SHOW USER FUNCTIONS NOT LIKE 'demo_scalar_%'")));
        assertEquals(List.of("demo_table_udf"),
                names(sqlExecutor.executeSql("SHOW USER FUNCTIONS NOT ILIKE 'DEMO_SCALAR_%'")));

        List<String> allFunctions = names(sqlExecutor.executeSql("SHOW FUNCTIONS"));
        assertTrue(allFunctions.containsAll(USER_FUNCTIONS));
        assertTrue(allFunctions.contains("abs"), "SHOW FUNCTIONS should include Flink built-in functions");
        assertEquals(allFunctions.stream().sorted().distinct().toList(), allFunctions);
        assertEquals(List.of("abs"), names(sqlExecutor.executeSql("SHOW FUNCTIONS ILIKE 'ABS'")));
        assertFalse(names(sqlExecutor.executeSql("SHOW USER FUNCTIONS")).contains("abs"));
    }

    @Test
    void testSessionCacheTableTakesPrecedenceOnlyForUnqualifiedNames() throws Exception {
        String cacheSql = "CACHE TABLE genre_hot_item AS SELECT 1 AS cached_id";
        // Construct session tables directly so this metadata test never initializes business connectors.
        CalciteSchema sessionSchema = CalciteSchema.createRootSchema(false);
        CacheTable cached = new CacheTable("genre_hot_item", null, DataTypeUtils.getStringTypeField("cached_id"));
        cached.setCreateSql(CompileManager.getSqlStr(CompileManager.parseSql(cacheSql)));
        sessionSchema.add("genre_hot_item", cached);
        sessionSchema.add("metadata_query_cache",
                new CacheTable("metadata_query_cache", null, DataTypeUtils.getStringTypeField("cached_id")));
        try (var schemas = mockStatic(CalciteSchemaFactory.class)) {
            schemas.when(CalciteSchemaFactory::createCalciteSchema).thenReturn(sessionSchema);
            sqlExecutor = new SqlExecutor();
        }

        CacheTable cachedDescription = sqlExecutor.executeSql("DESCRIBE genre_hot_item");
        assertEquals(List.of("name", "type"), fields(cachedDescription));
        assertEquals(List.of("cached_id"), names(cachedDescription));
        assertEquals(List.of(CompileManager.getSqlStr(CompileManager.parseSql(cacheSql))),
                names(sqlExecutor.executeSql("SHOW CREATE TABLE genre_hot_item")));
        for (String name : List.of("`default`.genre_hot_item", "hive.`default`.genre_hot_item")) {
            assertEquals(List.of("genre", "movie_id", "score"), names(sqlExecutor.executeSql("DESCRIBE " + name)));
            assertEquals(List.of(CREATE_TABLE_SQL.get("genre_hot_item")),
                    names(sqlExecutor.executeSql("SHOW CREATE TABLE " + name)));
        }

        List<String> expectedTables = new ArrayList<>(TABLES);
        expectedTables.add("metadata_query_cache");
        expectedTables.sort(String::compareTo);
        assertEquals(expectedTables, names(sqlExecutor.executeSql("SHOW TABLES")));
        assertEquals(List.of("metadata_query_cache"),
                names(sqlExecutor.executeSql("SHOW TABLES LIKE 'metadata_query_%'")));
        assertEquals(TABLES, names(new SqlExecutor().executeSql("SHOW TABLES")), "cache tables belong to one session");
    }

    @Test
    void testMissingObjectsAndUnconfiguredCatalogsFail() {
        for (String sql : List.of("DESCRIBE missing_table", "SHOW CREATE TABLE `default`.missing_table",
                "SHOW USER FUNCTIONS IN missing_database")) {
            assertThrows(NoSuchObjectException.class, () -> sqlExecutor.executeSql(sql), sql);
        }
        assertThrows(IllegalArgumentException.class,
                () -> sqlExecutor.executeSql("SHOW TABLES IN missing_database"));
        for (String sql : List.of("DESCRIBE other.`default`.genre_hot_item",
                "SHOW CREATE TABLE other.`default`.genre_hot_item", "SHOW TABLES IN other.`default`",
                "SHOW FUNCTIONS IN other.`default`")) {
            assertThrows(UnsupportedOperationException.class, () -> sqlExecutor.executeSql(sql), sql);
        }
    }

    private static List<String> fields(CacheTable result) {
        return result.getDataFields().stream().map(field -> field.getName()).toList();
    }

    private static List<String> names(CacheTable result) {
        return result.scan(null).select(row -> (String) row[0]).toList();
    }
}
