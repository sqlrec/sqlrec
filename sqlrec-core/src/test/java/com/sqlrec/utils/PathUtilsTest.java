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

    // --- dot segment removal ---

    @Test
    public void testNormalizePathResolvesDotSegments() {
        assertEquals("/models/y", PathUtils.normalizePath("/models/x/../y"));
        // escaping above the root is invalid (FilenameUtils.normalize returns null)
        // and collapses to "", so validateModelPath rejects it
        assertEquals("", PathUtils.normalizePath("/a/../../b"));
        assertEquals("/models/x", PathUtils.normalizePath("/models/./x/"));
        assertEquals("/models/x/cp1", PathUtils.normalizePath("/models/x/cp1/."));
    }

    @Test
    public void testNormalizePathRootAfterDotDots() {
        // "/a/.." collapses all the way to the root (represented as "")
        assertEquals("", PathUtils.normalizePath("/a/.."));
        assertEquals("", PathUtils.normalizePath("/a/../"));
    }

    @Test
    public void testValidateModelPathRejectsTraversalOutsideModelPath() {
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath("/models/x/../../etc/passwd", "/models/x"));
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath("/models/x/cp1/../../../user/other", "/models/x"));
    }

    @Test
    public void testValidateModelPathAcceptsTraversalStayingInside() {
        // "/models/x/cp1/.." resolves back to the model path itself, which is allowed
        assertDoesNotThrow(() ->
                PathUtils.validateModelPath("/models/x/cp1/..", "/models/x"));
        assertDoesNotThrow(() ->
                PathUtils.validateModelPath("/models/x/../x/cp1", "/models/x"));
    }

    @Test
    public void testValidateModelPathRejectsTraversalWithProtocol() {
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath("hdfs://nn:8020/models/x/../../etc", "hdfs://nn:8020/models/x"));
    }

    // --- authority checks ---

    @Test
    public void testValidateModelPathRejectsDifferentAuthority() {
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath("hdfs://nn1:8020/models/x/cp1", "hdfs://nn2:8020/models/x"));
    }

    @Test
    public void testValidateModelPathAcceptsMissingAuthorityOnOneSide() {
        // scheme-less path against a scheme-ful model path stays allowed (compat)
        assertDoesNotThrow(() ->
                PathUtils.validateModelPath("/models/x/cp1", "hdfs://nn:8020/models/x"));
        assertDoesNotThrow(() ->
                PathUtils.validateModelPath("hdfs:///models/x/cp1", "hdfs://nn:8020/models/x"));
    }

    // --- blank path checks ---

    @Test
    public void testValidateModelPathRejectsBlankPaths() {
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath("/models/x/cp1", ""));
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath("/models/x/cp1", "   "));
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath(null, "/models/x"));
        assertThrows(IllegalArgumentException.class,
                () -> PathUtils.validateModelPath("  ", "/models/x"));
    }
}
