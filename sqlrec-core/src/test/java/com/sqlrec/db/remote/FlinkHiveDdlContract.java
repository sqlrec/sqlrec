package com.sqlrec.db.remote;

import com.sqlrec.common.utils.HiveTableUtils;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.resource.ResourceType;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/** The same DDL assertions run against real HMS and the runtime test's simulated HMS. */
final class FlinkHiveDdlContract {
    private FlinkHiveDdlContract() {}

    static void tableLifecycle(HiveCatalog reader, String database) throws Exception {
        ObjectPath original = new ObjectPath(database, "items");
        String table = original.getFullName();
        String create = "CREATE TABLE " + table
                + " (id BIGINT, amount DECIMAL(10, 2), category STRING, PRIMARY KEY (id) NOT ENFORCED)"
                + " COMMENT 'table review' PARTITIONED BY (category)"
                + " WITH ('connector'='filesystem', 'path'='file:///tmp/sqlrec-items', 'format'='json')";
        ddl(create, database);
        CatalogTable definition = (CatalogTable) reader.getTable(original);
        assertEquals(List.of("id", "amount", "category"), columnNames(definition));
        assertEquals(List.of("category"), definition.getPartitionKeys());
        assertEquals("table review", definition.getComment());
        assertEquals(List.of("id"), definition.getUnresolvedSchema().getPrimaryKey().orElseThrow().getColumnNames());
        assertEquals("filesystem", definition.getOptions().get("connector"));
        assertEquals("json", definition.getOptions().get("format"));
        String schema = definition.getUnresolvedSchema().toString();

        assertThrows(RuntimeException.class, () -> ddl(create, database));
        ddl(create.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
                .replace("sqlrec-items", "must-not-replace"), database);
        assertEquals("file:///tmp/sqlrec-items", reader.getTable(original).getOptions().get("path"));
        assertThrows(RuntimeException.class, () -> ddl("ALTER TABLE " + database + ".missing SET ('path'='x')", database));
        assertThrows(RuntimeException.class, () -> ddl("DROP DATABASE " + database, "default"));
        assertTrue(reader.tableExists(original));

        ddl("ALTER TABLE " + table + " SET ('path'='file:///tmp/sqlrec-items-v2')", database);
        assertEquals("file:///tmp/sqlrec-items-v2", reader.getTable(original).getOptions().get("path"));
        assertEquals(schema, reader.getTable(original).getUnresolvedSchema().toString());
        ddl("ALTER TABLE " + table + " ADD note STRING", database);
        assertEquals(List.of("id", "amount", "category", "note"), columnNames(reader.getTable(original)));
        ddl("ALTER TABLE " + table + " MODIFY note VARCHAR(100) COMMENT 'updated note'", database);
        var note = (Schema.UnresolvedPhysicalColumn) reader.getTable(original).getUnresolvedSchema().getColumns().get(3);
        assertEquals("VARCHAR(100)", HiveTableUtils.getMetadataFields(reader.getHiveTable(original)).get(3).getType());
        assertEquals("updated note", note.getComment().orElseThrow());
        ddl("ALTER TABLE " + table + " DROP note", database);
        assertEquals(schema, reader.getTable(original).getUnresolvedSchema().toString());

        ddl("ALTER TABLE " + table + " RENAME TO items_renamed", database);
        ObjectPath renamed = new ObjectPath(database, "items_renamed");
        assertFalse(reader.tableExists(original));
        assertTrue(reader.tableExists(renamed));
        definition = (CatalogTable) reader.getTable(renamed);
        assertEquals(schema, definition.getUnresolvedSchema().toString());
        assertEquals(List.of("category"), definition.getPartitionKeys());
        assertEquals("table review", definition.getComment());
        assertEquals("file:///tmp/sqlrec-items-v2", definition.getOptions().get("path"));
        String shown = (String) FlinkHiveDdlAdapter.executeQuery("SHOW CREATE TABLE " + renamed.getFullName(), database)
                .getEnumerable().toList().get(0)[0];
        assertTrue(shown.contains("sqlrec-items-v2"), shown);
        assertTrue(shown.contains("PRIMARY KEY"), shown);
        assertEquals(List.of("id", "amount", "category"),
                FlinkHiveDdlAdapter.executeQuery("DESCRIBE " + renamed.getFullName(), database)
                        .getEnumerable().select(row -> (String) row[0]).toList());

        ddl("DROP TABLE " + renamed.getFullName(), database);
        assertFalse(reader.tableExists(renamed));
        assertThrows(RuntimeException.class, () -> FlinkHiveDdlAdapter.executeQuery(
                "DESCRIBE " + renamed.getFullName(), database));
        assertThrows(RuntimeException.class, () -> ddl("DROP TABLE " + renamed.getFullName(), database));
        ddl("DROP TABLE IF EXISTS " + renamed.getFullName(), database);
    }

    static void databaseAndFunctionLifecycle(HiveCatalog reader, String database, Path directory) throws Exception {
        ddl("CREATE DATABASE " + database + " COMMENT 'database review' WITH ('review'='initial')", "default");
        assertTrue(reader.databaseExists(database));
        assertEquals("database review", reader.getDatabase(database).getComment());
        assertEquals("initial", reader.getDatabase(database).getProperties().get("review"));
        assertThrows(RuntimeException.class, () -> ddl("CREATE DATABASE " + database, "default"));
        ddl("CREATE DATABASE IF NOT EXISTS " + database + " WITH ('review'='must-not-replace')", "default");
        assertEquals("initial", reader.getDatabase(database).getProperties().get("review"));
        ddl("ALTER DATABASE " + database + " SET ('review'='updated')", "default");
        assertEquals("updated", reader.getDatabase(database).getProperties().get("review"));
        assertEquals("database review", reader.getDatabase(database).getComment());

        ObjectPath function = new ObjectPath(database, "review_udf");
        ddl("CREATE FUNCTION " + function.getFullName() + " AS 'example.ReviewUdf' LANGUAGE JAVA", database);
        assertTrue(reader.functionExists(function));
        assertEquals("example.ReviewUdf", reader.getFunction(function).getClassName());
        assertEquals(FunctionLanguage.JAVA, reader.getFunction(function).getFunctionLanguage());
        ddl("ALTER FUNCTION " + function.getFullName() + " AS 'example.ReviewUdfV2' LANGUAGE JAVA", database);
        assertEquals("example.ReviewUdfV2", reader.getFunction(function).getClassName());
        assertEquals(FunctionLanguage.JAVA, reader.getFunction(function).getFunctionLanguage());

        Path jar = directory.resolve("review-udf.jar");
        try (JarOutputStream ignored = new JarOutputStream(Files.newOutputStream(jar))) {
            // Metadata registration should preserve the resource without instantiating the UDF.
        }
        ObjectPath jarFunction = new ObjectPath(database, "review_jar_udf");
        String uri = jar.toUri().toString();
        ddl("CREATE FUNCTION " + jarFunction.getFullName()
                + " AS 'example.JarUdf' LANGUAGE JAVA USING JAR '" + uri + "'", database);
        var saved = reader.getFunction(jarFunction);
        assertEquals("example.JarUdf", saved.getClassName());
        assertEquals(FunctionLanguage.JAVA, saved.getFunctionLanguage());
        assertEquals(1, saved.getFunctionResources().size());
        assertEquals(ResourceType.JAR, saved.getFunctionResources().get(0).getResourceType());
        assertEquals(uri, saved.getFunctionResources().get(0).getUri());
        ddl("DROP FUNCTION " + jarFunction.getFullName(), database);
        assertFalse(reader.functionExists(jarFunction));
        ddl("DROP FUNCTION " + function.getFullName(), database);
        assertFalse(reader.functionExists(function));
        assertThrows(RuntimeException.class, () -> ddl("DROP FUNCTION " + function.getFullName(), database));
        ddl("DROP FUNCTION IF EXISTS " + function.getFullName(), database);

        ddl("DROP DATABASE " + database, "default");
        assertFalse(reader.databaseExists(database));
        assertThrows(RuntimeException.class, () -> ddl("DROP DATABASE " + database, "default"));
        ddl("DROP DATABASE IF EXISTS " + database, "default");
    }

    private static List<String> columnNames(org.apache.flink.table.catalog.CatalogBaseTable table) {
        return table.getUnresolvedSchema().getColumns().stream().map(Schema.UnresolvedColumn::getName).toList();
    }

    private static void ddl(String sql, String database) throws Exception {
        FlinkHiveDdlAdapter.executeDdl(sql, database);
    }

    static void closeAdapter() throws Exception {
        var close = FlinkHiveDdlAdapter.class.getDeclaredMethod("closeCatalog");
        close.setAccessible(true);
        close.invoke(null);
    }
}
