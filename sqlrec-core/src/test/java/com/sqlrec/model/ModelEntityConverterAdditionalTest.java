package com.sqlrec.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ModelEntityConverterAdditionalTest {

    @Test
    public void testFixPathProtocolWithAlreadyPrefixedPath() {
        List<String> paths = Arrays.asList(
                "hdfs://localhost:9000/path/to/table",
                "s3://bucket/path/to/table"
        );

        List<String> result = ModelEntityConverter.fixPathProtocol(paths);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("hdfs://localhost:9000/path/to/table", result.get(0));
        assertEquals("s3://bucket/path/to/table", result.get(1));
    }

    @Test
    public void testFixPathProtocolWithEmptyList() {
        List<String> result = ModelEntityConverter.fixPathProtocol(Collections.emptyList());

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFixPathProtocolWithMixedPaths() {
        List<String> paths = Arrays.asList(
                "hdfs://localhost:9000/path/to/table",
                "/path/to/local/table"
        );

        List<String> result = ModelEntityConverter.fixPathProtocol(paths);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("hdfs://localhost:9000/path/to/table", result.get(0));
        assertTrue(result.get(1).contains("/path/to/local/table"));
    }
}
