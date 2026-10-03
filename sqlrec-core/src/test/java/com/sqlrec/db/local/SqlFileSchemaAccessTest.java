package com.sqlrec.db.local;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.executor.SqlProcessResult;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlFileSchemaAccessTest {
    private static final String TABLE = """
            CREATE TABLE hive.review.items (
                id BIGINT COMMENT 'identifier',
                category STRING,
                amount DECIMAL(10, 2),
                event_time TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
                PRIMARY KEY (id) NOT ENFORCED
            ) COMMENT 'table comment' PARTITIONED BY (category)
            WITH ('connector'='not_installed', 'custom.option'='it''s preserved')
            """;

    @Test
    void describesFileDefinitionsWithoutLoadingTheirConnector() throws Exception {
        var files = tables(TABLE);
        var result = query(files, "DESCRIBE hive.REVIEW.ITEMS", "default");
        assertEquals(List.of("name", "type", "null", "key", "extras", "watermark", "comment"),
                result.getFields().stream().map(field -> field.getName()).toList());
        var rows = result.getEnumerable().toList();
        assertEquals(List.of("id", "category", "amount", "event_time"), names(result));
        assertEquals("BIGINT", rows.get(0)[1]);
        assertEquals(false, rows.get(0)[2]);
        assertEquals("PRI(id)", rows.get(0)[3]);
        assertEquals("identifier", rows.get(0)[6]);
        assertEquals("DECIMAL(10, 2)", rows.get(2)[1]);
        assertEquals(true, rows.get(2)[2]);
        assertTrue(rows.get(3)[4].toString().contains("timestamp"));
        assertTrue(rows.get(3)[4].toString().contains("VIRTUAL"));
        assertTrue(rows.stream().allMatch(row -> row[5] == null));
        assertEquals("review", files.getTable("REVIEW", "ITEMS").getDbName());
    }

    @Test
    void showCreatePreservesTheDeclarationEvenAfterDescribeValidation() throws Exception {
        var files = tables(TABLE);
        String before = (String) names(query(files, "SHOW CREATE TABLE items", "review")).get(0);
        query(files, "DESCRIBE items", "review");
        String after = (String) names(query(files, "SHOW CREATE TABLE review.items", "default")).get(0);
        assertEquals(before, after);
        assertEquals(CompileManager.getSqlStr(CompileManager.parseSql(TABLE)), after);
        var roundTrip = (SqlCreateTable) CompileManager.parseSql(after);
        assertEquals(4, roundTrip.getColumnList().size());
        assertTrue(after.contains("METADATA"));
        assertTrue(after.contains("PARTITIONED BY"));
        assertTrue(after.contains("PRIMARY KEY"));
        assertTrue(after.contains("table comment"));
        assertTrue(after.contains("it''s preserved"));
        assertEquals("result", query(files, "SHOW CREATE TABLE items", "review").getFields().get(0).getName());
    }

    @Test
    void functionsSupportQualificationFilteringAndBuiltinListingWithoutLoadingClasses() throws Exception {
        var files = new SqlFileSchemaAccess(List.of(), List.of(
                CompileManager.parseSql("CREATE FUNCTION hive.review.alpha AS 'missing.Alpha'"),
                CompileManager.parseSql("CREATE FUNCTION review.beta AS 'missing.Beta'")));
        assertEquals(List.of("alpha", "beta"), names(query(files, "SHOW USER FUNCTIONS IN hive.REVIEW", "default")));
        assertEquals(List.of("alpha"), names(query(files, "SHOW USER FUNCTIONS LIKE 'a%'", "review")));
        assertEquals(List.of("alpha"), names(query(files, "SHOW USER FUNCTIONS ILIKE 'A%'", "review")));
        assertTrue(names(query(files, "SHOW USER FUNCTIONS LIKE 'A%'", "review")).isEmpty());
        assertEquals(List.of("beta"), names(query(files, "SHOW USER FUNCTIONS NOT LIKE 'a%'", "review")));
        assertEquals(List.of("beta"), names(query(files, "SHOW USER FUNCTIONS NOT ILIKE 'A%'", "review")));
        var all = names(query(files, "SHOW FUNCTIONS", "review"));
        assertTrue(all.containsAll(List.of("alpha", "beta")));
        assertTrue(all.size() > 2);
        assertEquals(all.stream().map(Object::toString).sorted().distinct().toList(), all);
        assertEquals("function name", query(files, "SHOW FUNCTIONS", "review").getFields().get(0).getName());
        assertEquals("review", files.getFunction("REVIEW", "ALPHA").getDbName());
    }

    @Test
    void emptyFilesStillExposeBuiltinFunctionsInTheDefaultDatabase() throws Exception {
        var files = new SqlFileSchemaAccess(List.of(), List.of());
        assertTrue(names(query(files, "SHOW USER FUNCTIONS", "default")).isEmpty());
        assertFalse(names(query(files, "SHOW FUNCTIONS", "default")).isEmpty());
    }

    @Test
    void unsupportedSchemaFeaturesCanBeShownButAreNotSilentlyOmittedFromDescribe() throws Exception {
        for (String ddl : List.of(
                "CREATE TABLE review.items (id INT, extra AS id + 1)",
                "CREATE TABLE review.items (ts TIMESTAMP(3), WATERMARK FOR ts AS ts)",
                "CREATE TABLE review.items AS SELECT 1 AS id",
                "CREATE TABLE review.items LIKE review.source")) {
            var files = tables(ddl);
            assertEquals(1, files.getTables("review").size());
            assertFalse(names(query(files, "SHOW CREATE TABLE items", "review")).isEmpty());
            assertThrows(UnsupportedOperationException.class, () -> query(files, "DESCRIBE items", "review"), ddl);
        }
        var metadataOnly = tables("CREATE TABLE review.items (ts TIMESTAMP(3) METADATA FROM 'timestamp')");
        assertEquals(1, metadataOnly.getTables("review").size());
        assertEquals(List.of("ts"), names(query(metadataOnly, "DESCRIBE items", "review")));
    }

    @Test
    void missingObjectsAndForeignCatalogsFailExplicitlyAndDdlRemainsReadOnly() throws Exception {
        var files = tables(TABLE);
        for (String sql : List.of("DESCRIBE missing", "SHOW CREATE TABLE missing", "SHOW FUNCTIONS IN missing")) {
            assertThrows(NoSuchObjectException.class, () -> query(files, sql, "review"), sql);
        }
        for (String sql : List.of("DESCRIBE other.review.items", "SHOW CREATE TABLE other.review.items",
                "SHOW FUNCTIONS IN other.review")) {
            assertThrows(UnsupportedOperationException.class, () -> query(files, sql, "review"), sql);
        }
        assertThrows(UnsupportedOperationException.class, () -> query(files, "SELECT 1", "review"));
        assertThrows(UnsupportedOperationException.class,
                () -> files.executeMetadataDdl(CompileManager.parseSql("DROP TABLE items"), "review"));
    }

    @Test
    void quotedNamesWithDotsAreResolvedByIdentifierParts() throws Exception {
        var files = tables("CREATE TABLE hive.`db.name`.`table.name` (id INT)");
        assertEquals(List.of("id"), names(query(files, "DESCRIBE hive.`db.name`.`table.name`", "default")));
    }

    private static SqlFileSchemaAccess tables(String ddl) throws Exception {
        return new SqlFileSchemaAccess(List.of(CompileManager.parseSql(ddl)), List.of());
    }

    private static SqlProcessResult query(SqlFileSchemaAccess files, String sql, String database) throws Exception {
        return files.executeMetadataQuery(CompileManager.parseSql(sql), database);
    }

    private static List<Object> names(SqlProcessResult result) {
        return result.getEnumerable().select(row -> row[0]).toList();
    }
}
