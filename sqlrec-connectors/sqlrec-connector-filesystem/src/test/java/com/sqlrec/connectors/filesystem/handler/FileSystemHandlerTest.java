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

    @Test
    void testLoadsAndLooksUpMultipleCsvRowsPerKey() throws IOException {
        Path csvFile = tempDir.resolve("list.csv");
        Files.write(csvFile, List.of("id,name,age", "1,alice,20", "1,bob,25", "2,charlie,30"));
        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        assertEquals(3, handler.scan().size());
        List<Object[]> rows = handler.getByPrimaryKey(Set.of(1)).get(1);
        assertEquals(2, rows.size());
        assertEquals("bob", rows.get(0)[1]);
        assertEquals("alice", rows.get(1)[1]);

        rows.get(0)[1] = "changed";
        assertEquals("bob", handler.getByPrimaryKey(Set.of(1)).get(1).get(0)[1]);
    }

    @Test
    void testLoadsMultipleJsonRowsPerKey() throws IOException {
        Path jsonFile = tempDir.resolve("list.json");
        Files.writeString(jsonFile, "[{\"id\":1,\"name\":\"alice\",\"age\":20},"
                + "{\"id\":1,\"name\":\"bob\",\"age\":25}]");
        FileSystemConfig config = createConfig(jsonFile.toString(), "json");
        FileSystemHandler handler = new FileSystemHandler(config);

        assertEquals(2, handler.getByPrimaryKey(Set.of(1)).get(1).size());
    }

    @Test
    void testInsertAndDeleteMatchingRowsUnderOneKey() {
        FileSystemConfig config = createConfig(null, "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        handler.insert(new Object[]{1, "alice", 20});
        handler.insert(new Object[]{1, "bob", 25});
        handler.insert(new Object[]{1, "bob", 25});
        assertEquals(3, handler.getByPrimaryKey(Set.of(1)).get(1).size());
        assertFalse(handler.delete(new Object[]{1, "missing", 25}));
        assertTrue(handler.delete(new Object[]{1, "bob", 25}));
        List<Object[]> remaining = handler.getByPrimaryKey(Set.of(1)).get(1);
        assertEquals(1, remaining.size());
        assertEquals("alice", remaining.get(0)[1]);
        assertTrue(handler.delete(new Object[]{1, "alice", 20}));
        assertFalse(handler.getByPrimaryKey(Set.of(1)).containsKey(1));
    }

    @Test
    void testReplaceAllRemovesOldRowsBeforeInsertingNewRows() {
        FileSystemHandler handler = new FileSystemHandler(createConfig(null, "csv"));
        handler.insert(new Object[]{1, "alice", 20});
        handler.insert(new Object[]{1, "alice", 21});
        handler.insert(new Object[]{1, "bob", 30});
        List<Object[]> previousSnapshot = handler.scan();

        assertEquals(2, handler.replaceAll(
                List.of(new Object[]{1, "alice", 20}, new Object[]{1, "alice", 21}),
                List.of(new Object[]{1, "alice", 21}, new Object[]{1, "alice", 22})));

        List<Object[]> rows = handler.getByPrimaryKey(Set.of(1)).get(1);
        assertEquals(3, rows.size());
        assertEquals(1, rows.stream().filter(row -> row[1].equals("alice") && row[2].equals(21)).count());
        assertEquals(1, rows.stream().filter(row -> row[1].equals("alice") && row[2].equals(22)).count());
        assertEquals(1, rows.stream().filter(row -> row[1].equals("bob") && row[2].equals(30)).count());
        assertEquals(1, previousSnapshot.stream()
                .filter(row -> row[1].equals("alice") && row[2].equals(20)).count());
    }

    @Test
    void testReplaceAllValidatesEntireBatchBeforeChangingData() {
        FileSystemHandler handler = new FileSystemHandler(createConfig(null, "csv"));
        handler.insert(new Object[]{1, "alice", 20});

        assertThrows(IllegalArgumentException.class, () -> handler.replaceAll(
                List.of(new Object[]{1, "alice", 20}, new Object[]{null, "missing", 30}),
                List.of(new Object[]{1, "alice", 21}, new Object[]{2, "bob", 31})));
        assertThrows(IllegalArgumentException.class, () -> handler.replaceAll(
                Collections.singletonList(new Object[]{1, "alice", 20}),
                Collections.singletonList(new Object[]{null, "alice", 21})));
        assertThrows(IllegalArgumentException.class, () -> handler.replaceAll(
                Collections.singletonList(new Object[]{1, "alice", 20}), List.of()));
        assertThrows(IllegalArgumentException.class, () -> handler.replaceAll(
                Collections.singletonList(new Object[]{1, "alice", 20}),
                Collections.singletonList(new Object[]{1, "alice"})));

        List<Object[]> rows = handler.getByPrimaryKey(Set.of(1)).get(1);
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{1, "alice", 20}, rows.get(0));
        assertEquals(1, handler.scan().size());
    }

    @Test
    void testReplaceAllCountsOnlyMatchingDuplicateRows() {
        FileSystemHandler handler = new FileSystemHandler(createConfig(null, "csv"));
        handler.insert(new Object[]{1, "alice", 20});
        handler.insert(new Object[]{1, "alice", 20});
        handler.insert(new Object[]{1, "bob", 30});

        assertEquals(2, handler.replaceAll(
                List.of(new Object[]{1, "alice", 20}, new Object[]{1, "alice", 20},
                        new Object[]{1, "missing", 40}),
                List.of(new Object[]{1, "alice", 21}, new Object[]{1, "alice", 22},
                        new Object[]{1, "missing", 41})));

        List<Object[]> rows = handler.getByPrimaryKey(Set.of(1)).get(1);
        assertEquals(3, rows.size());
        assertEquals(1, rows.stream().filter(row -> row[1].equals("alice") && row[2].equals(21)).count());
        assertEquals(1, rows.stream().filter(row -> row[1].equals("alice") && row[2].equals(22)).count());
        assertEquals(1, rows.stream().filter(row -> row[1].equals("bob") && row[2].equals(30)).count());
        assertEquals(0, handler.replaceAll(
                Collections.singletonList(new Object[]{1, "missing", 40}),
                Collections.singletonList(new Object[]{1, "missing", 41})));
    }

    @Test
    void testBatchInsertAndDeleteValidateBeforeChangingData() {
        FileSystemHandler handler = new FileSystemHandler(createConfig(null, "csv"));

        assertFalse(handler.insertAll(List.of()));
        assertFalse(handler.deleteAll(List.of()));
        assertEquals(0, handler.replaceAll(List.of(), List.of()));

        assertThrows(IllegalArgumentException.class, () -> handler.insertAll(List.of(
                new Object[]{1, "alice", 20}, new Object[]{null, "invalid", 30})));
        assertTrue(handler.scan().isEmpty());
        assertTrue(handler.insertAll(List.of(
                new Object[]{1, "alice", 20}, new Object[]{1, "bob", 25})));

        assertThrows(IllegalArgumentException.class, () -> handler.deleteAll(List.of(
                new Object[]{1, "alice", 20}, new Object[]{null, "invalid", 30})));
        assertEquals(2, handler.getByPrimaryKey(Set.of(1)).get(1).size());
        assertTrue(handler.deleteAll(List.of(
                new Object[]{1, "alice", 20}, new Object[]{1, "bob", 25})));
        assertTrue(handler.scan().isEmpty());
    }

    // ===== Write (Memory-Only) Tests =====

    @Test
    void testInsertNewKey() throws IOException {
        Path csvFile = tempDir.resolve("insert.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        handler.insert(new Object[]{2, "bob", 25});

        List<Object[]> rows = handler.scan();
        assertEquals(2, rows.size());
    }

    @Test
    void testInsertRetainsExistingRowWithSameKey() throws IOException {
        Path csvFile = tempDir.resolve("insert_same_key.csv");
        Files.write(csvFile, Arrays.asList(
                "id,name,age",
                "1,alice,20"
        ));

        FileSystemConfig config = createConfig(csvFile.toString(), "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        handler.insert(new Object[]{1, "alice_updated", 21});

        List<Object[]> rows = handler.getByPrimaryKey(Set.of(1)).get(1);
        assertEquals(2, rows.size());
        assertEquals("alice_updated", rows.get(0)[1]);
        assertEquals(21, rows.get(0)[2]);
        assertEquals("alice", rows.get(1)[1]);
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
    void testInsertOnEmptyTable() {
        FileSystemConfig config = createConfig(null, "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        handler.insert(new Object[]{1, "alice", 20});

        List<Object[]> rows = handler.scan();
        assertEquals(1, rows.size());
    }

    @Test
    void testInsertRejectsNullPrimaryKey() {
        FileSystemConfig config = createConfig(null, "csv");
        FileSystemHandler handler = new FileSystemHandler(config);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> handler.insert(new Object[]{null, "alice", 20})
        );

        assertTrue(exception.getMessage().contains("Primary key"));
    }

    @Test
    void testInsertRejectsWrongFieldCount() {
        FileSystemHandler handler = new FileSystemHandler(createConfig(null, "csv"));

        assertThrows(IllegalArgumentException.class,
                () -> handler.insert(new Object[]{1, "alice"}));
        assertThrows(IllegalArgumentException.class,
                () -> handler.insert(new Object[]{1, "alice", 20, "extra"}));
        assertTrue(handler.scan().isEmpty());
    }

    @Test
    void testInsertNormalizesRowTypes() {
        FileSystemConfig config = new FileSystemConfig();
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "BIGINT"),
                new FieldSchema("score", "FLOAT")
        );
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        FileSystemHandler handler = new FileSystemHandler(config);

        handler.insert(new Object[]{1, 100.0D});

        Map<Object, List<Object[]>> result = handler.getByPrimaryKey(Collections.singleton(1L));
        assertEquals(1, result.get(1L).size());
        assertEquals(1L, result.get(1L).get(0)[0]);
        assertEquals(100.0F, result.get(1L).get(0)[1]);
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
