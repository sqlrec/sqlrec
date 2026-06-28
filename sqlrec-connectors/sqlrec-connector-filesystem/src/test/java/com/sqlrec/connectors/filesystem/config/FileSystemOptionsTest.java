package com.sqlrec.connectors.filesystem.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FileSystemOptionsTest {

    @Test
    void testConnectorIdentifier() {
        assertEquals("filesystem", FileSystemOptions.CONNECTOR_IDENTIFIER);
    }

    @Test
    void testGetFileSystemConfigWithAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("path", "file:///data/test.csv");
        options.put("format", "csv");

        FileSystemConfig config = FileSystemOptions.getFileSystemConfig(options);

        assertEquals("file:///data/test.csv", config.path);
        assertEquals("csv", config.format);
    }

    @Test
    void testGetFileSystemConfigWithJsonFormat() {
        Map<String, String> options = new HashMap<>();
        options.put("path", "/data/test.json");
        options.put("format", "json");

        FileSystemConfig config = FileSystemOptions.getFileSystemConfig(options);

        assertEquals("/data/test.json", config.path);
        assertEquals("json", config.format);
    }

    @Test
    void testGetFileSystemConfigDefaultFormat() {
        Map<String, String> options = new HashMap<>();
        options.put("path", "/data/test.csv");

        FileSystemConfig config = FileSystemOptions.getFileSystemConfig(options);

        assertEquals("csv", config.format);
    }

    @Test
    void testGetFileSystemConfigNoPath() {
        Map<String, String> options = new HashMap<>();

        FileSystemConfig config = FileSystemOptions.getFileSystemConfig(options);

        assertNull(config.path);
        assertEquals("csv", config.format);
    }

    @Test
    void testGetFileSystemConfigInvalidFormat() {
        Map<String, String> options = new HashMap<>();
        options.put("path", "/data/test");
        options.put("format", "parquet");

        assertThrows(IllegalArgumentException.class, () -> FileSystemOptions.getFileSystemConfig(options));
    }
}
