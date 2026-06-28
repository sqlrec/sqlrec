package com.sqlrec.connectors.filesystem.handler;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class FileSystemHandlerTest {

    @TempDir
    Path tempDir;

    private List<FieldSchema> fieldSchemas;

    @BeforeEach
    void setUp() {
        fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("age", "INTEGER")
        );
    }

    private FileSystemConfig createConfig(String path, String format) {
        FileSystemConfig config = new FileSystemConfig();
        config.path = path;
        config.format = format;
        config.fieldSchemas = fieldSchemas;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        return config;
    }

    // ===== CSV Tests =====

    @Test
    void testLoadCsv() throws IOException {
        Path csvFile = tempDir.resolve("test.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20",
                "2,bob,25",
                "3,charlie,30"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
        assertEquals(20, rows.get(0)[2]);
    }

    @Test
    void testLoadCsvWithQuotedFields() throws IOException {
        Path csvFile = tempDir.resolve("quoted.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,\"alice, jr.\",20",
                "2,\"bob \"\"the builder\"\"\",25"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertEquals(2, rows.size());
        assertEquals("alice, jr.", rows.get(0)[1]);
        assertEquals("bob \"the builder\"", rows.get(1)[1]);
    }

    @Test
    void testLoadCsvWithEmptyValues() throws IOException {
        Path csvFile = tempDir.resolve("empty.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,,20",
                "2,bob,"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertEquals(2, rows.size());
        assertNull(rows.get(0)[1]);
        assertNull(rows.get(1)[2]);
    }

    @Test
    void testLoadCsvWithBlankLines() throws IOException {
        Path csvFile = tempDir.resolve("blanks.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "",
                "1,alice,20",
                "",
                "2,bob,25"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertEquals(2, rows.size());
    }

    // ===== JSON Tests =====

    @Test
    void testLoadJsonArray() throws IOException {
        Path jsonFile = tempDir.resolve("test.json");
        String json = "[{\"id\": 1, \"name\": \"alice\", \"age\": 20}, {\"id\": 2, \"name\": \"bob\", \"age\": 25}, {\"id\": 3, \"name\": \"charlie\", \"age\": 30}]";
        Files.writeString(jsonFile, json);

        FileSystemConfig config = createConfig(jsonFile.toString(), "json");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
        assertEquals(20, rows.get(0)[2]);
    }

    @Test
    void testLoadJsonSingleObject() throws IOException {
        Path jsonFile = tempDir.resolve("single.json");
        Files.writeString(jsonFile, "{\"id\": 1, \"name\": \"alice\", \"age\": 20}");

        FileSystemConfig config = createConfig(jsonFile.toString(), "json");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
    }

    @Test
    void testLoadJsonWithNullFields() throws IOException {
        Path jsonFile = tempDir.resolve("nulls.json");
        String json = "[{\"id\": 1, \"name\": null, \"age\": 20}, {\"id\": 2, \"name\": \"bob\", \"age\": null}]";
        Files.writeString(jsonFile, json);

        FileSystemConfig config = createConfig(jsonFile.toString(), "json");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertEquals(2, rows.size());
        assertNull(rows.get(0)[1]);
        assertNull(rows.get(1)[2]);
    }

    @Test
    void testLoadInvalidJson() throws IOException {
        Path jsonFile = tempDir.resolve("invalid.json");
        Files.writeString(jsonFile, "not valid json");

        FileSystemConfig config = createConfig(jsonFile.toString(), "json");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertTrue(rows.isEmpty());
    }

    // ===== Empty Table Tests =====

    @Test
    void testNoPathConfigured() {
        FileSystemConfig config = createConfig(null, "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertTrue(rows.isEmpty());
    }

    @Test
    void testEmptyPath() {
        FileSystemConfig config = createConfig("", "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertTrue(rows.isEmpty());
    }

    @Test
    void testNonExistentPath() {
        FileSystemConfig config = createConfig("/nonexistent/path/data.csv", "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertTrue(rows.isEmpty());
    }

    @Test
    void testUnsupportedFormat() throws IOException {
        Path dataFile = tempDir.resolve("data.csv");
        Files.write(dataFile, Arrays.asList("id,name,age", "1,alice,20"));

        FileSystemConfig config = createConfig(dataFile.toString(), "parquet");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertTrue(rows.isEmpty());
    }

    // ===== Primary Key Lookup Tests =====

    @Test
    void testGetByPrimaryKey() throws IOException {
        Path csvFile = tempDir.resolve("pk.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20",
                "2,bob,25",
                "3,charlie,30"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        Map<Object, List<Object[]>> result = handler.getByPrimaryKey(Collections.singleton(2));
        assertEquals(1, result.size());
        assertNotNull(result.get(2));
        assertEquals("bob", result.get(2).get(0)[1]);
    }

    @Test
    void testGetByPrimaryKeyMultipleKeys() throws IOException {
        Path csvFile = tempDir.resolve("multi_pk.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20",
                "2,bob,25",
                "3,charlie,30"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        Set<Object> keys = new HashSet<>(Arrays.asList(1, 3));
        Map<Object, List<Object[]>> result = handler.getByPrimaryKey(keys);
        assertEquals(2, result.size());
        assertEquals("alice", result.get(1).get(0)[1]);
        assertEquals("charlie", result.get(3).get(0)[1]);
    }

    @Test
    void testGetByPrimaryKeyEmptyKeySet() throws IOException {
        Path csvFile = tempDir.resolve("empty_key.csv");
        Files.write(csvFile, Arrays.asList("id,name,age", "1,alice,20"));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        Map<Object, List<Object[]>> result = handler.getByPrimaryKey(Collections.emptySet());
        assertTrue(result.isEmpty());
    }

    // ===== Write (Memory-Only) Tests =====

    @Test
    void testUpsertInsert() throws IOException {
        Path csvFile = tempDir.resolve("upsert.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        handler.upsert(new Object[]{2, "bob", 25});

        List<Object[]> rows = handler.scan();
        assertEquals(2, rows.size());
    }

    @Test
    void testUpsertUpdate() throws IOException {
        Path csvFile = tempDir.resolve("upsert_update.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        handler.upsert(new Object[]{1, "alice_updated", 21});

        List<Object[]> rows = handler.scan();
        assertEquals(1, rows.size());
        assertEquals("alice_updated", rows.get(0)[1]);
        assertEquals(21, rows.get(0)[2]);
    }

    @Test
    void testDelete() throws IOException {
        Path csvFile = tempDir.resolve("delete.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20",
                "2,bob,25"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        handler.delete(new Object[]{1, "alice", 20});

        List<Object[]> rows = handler.scan();
        assertEquals(1, rows.size());
        assertEquals(2, rows.get(0)[0]);
    }

    @Test
    void testUpsertOnEmptyTable() {
        FileSystemConfig config = createConfig(null, "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        handler.upsert(new Object[]{1, "alice", 20});

        List<Object[]> rows = handler.scan();
        assertEquals(1, rows.size());
    }

    // ===== file:// URI Tests =====

    @Test
    void testFileUriPrefix() throws IOException {
        Path csvFile = tempDir.resolve("uri.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20"
        ));

        FileSystemConfig config = createConfig("file:///" + csvFile.toString().replace('\\', '/'), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        List<Object[]> rows = handler.scan();
        assertEquals(1, rows.size());
    }

    // ===== Lazy Loading Test =====

    @Test
    void testLazyLoading() throws IOException {
        Path csvFile = tempDir.resolve("lazy.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        // Data should not be loaded until first access
        List<Object[]> rows1 = handler.scan();
        assertEquals(1, rows1.size());

        // Second access should return the same data
        List<Object[]> rows2 = handler.scan();
        assertEquals(1, rows2.size());
    }
}
