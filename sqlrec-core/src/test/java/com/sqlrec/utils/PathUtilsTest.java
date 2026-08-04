package com.sqlrec.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PathUtilsTest {

    // --- normalizePath ---

    @Test
    public void testNormalizePathSimple() {
        assertEquals("/a/b/c", PathUtils.normalizePath("/a/b/c"));
    }

    @Test
    public void testNormalizePathTrailingSlashes() {
        assertEquals("/a/b", PathUtils.normalizePath("/a/b/"));
        assertEquals("/a/b", PathUtils.normalizePath("/a/b///"));
    }

    @Test
    public void testNormalizePathWithProtocol() {
        assertEquals("/user/model", PathUtils.normalizePath("hdfs://namenode:8020/user/model"));
    }

    @Test
    public void testNormalizePathWithProtocolNoPath() {
        assertEquals("/namenode:8020", PathUtils.normalizePath("hdfs://namenode:8020"));
    }

    @Test
    public void testNormalizePathRelativePath() {
        assertEquals("/relative/path", PathUtils.normalizePath("relative/path"));
    }

    @Test
    public void testNormalizePathNull() {
        assertEquals("", PathUtils.normalizePath(null));
    }

    @Test
    public void testNormalizePathEmpty() {
        assertEquals("", PathUtils.normalizePath(""));
    }

    @Test
    public void testNormalizePathRoot() {
        assertEquals("", PathUtils.normalizePath("/"));
    }

    // --- validateModelPath ---

    @Test
    public void testValidateModelPathSamePath() {
        assertDoesNotThrow(() ->
                PathUtils.validateModelPath("/models/x", "/models/x"));
    }

    @Test
    public void testValidateModelPathUnderModelPath() {
        assertDoesNotThrow(() ->
                PathUtils.validateModelPath("/models/x/checkpoint1", "/models/x"));
    }

    @Test
    public void testValidateModelPathOutsideModelPathThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath("/other/path", "/models/x"));
    }

    @Test
    public void testValidateModelPathWithProtocol() {
        assertDoesNotThrow(() ->
                PathUtils.validateModelPath("hdfs://nn:8020/models/x/cp1", "hdfs://nn:8020/models/x"));
    }

    @Test
    public void testValidateModelPathPrefixMatchNotThrows() {
        // /models/x should NOT match /models/xy (prefix should be path-level, not string-level)
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath("/models/xy/cp1", "/models/x"));
    }
}
