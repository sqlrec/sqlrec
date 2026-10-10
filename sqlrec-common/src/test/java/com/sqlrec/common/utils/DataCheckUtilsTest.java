package com.sqlrec.common.utils;

import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.linq4j.Linq4j;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class DataCheckUtilsTest {
    @Test
    void rejectsNullActualResultAndDifferentRowCounts() {
        List<Object[]> expected = Collections.singletonList(new Object[]{1});
        assertEquals("actualResult is null", assertThrows(AssertionError.class,
                () -> DataCheckUtils.checkResultEqual(null, expected)).getMessage());
        assertEquals("size mismatch: expected 1, actual 0", assertThrows(AssertionError.class,
                () -> DataCheckUtils.checkResultEqual(Collections.emptyList(), expected)).getMessage());
        assertEquals("size mismatch: expected 0, actual 1", assertThrows(AssertionError.class,
                () -> DataCheckUtils.checkResultEqual(expected, Collections.emptyList())).getMessage());
    }

    @Test
    @SilenceLoggers(DataCheckUtils.class)
    void rejectsDifferentCellsWithExpectedAndActualValues() {
        AssertionError error = assertThrows(AssertionError.class,
                () -> DataCheckUtils.checkResultEqual(
                        Collections.singletonList(new Object[]{new int[]{1, 3}, null}),
                        Collections.singletonList(new Object[]{new int[]{1, 2}, null})));
        assertEquals("Row 0 mismatch: expected [[1, 2], null], actual [[1, 3], null]", error.getMessage());
    }

    @Test
    void acceptsDeeplyEqualRowsAndPreservesOptionalExpectedResults() {
        assertDoesNotThrow(() -> DataCheckUtils.checkResultEqual(
                Collections.singletonList(new Object[]{new int[]{1, 2}, null}),
                Collections.singletonList(new Object[]{new int[]{1, 2}, null})));
        assertDoesNotThrow(() -> DataCheckUtils.checkResultEqual(
                Collections.singletonList(null), Collections.singletonList(null)));
        assertDoesNotThrow(() -> DataCheckUtils.checkResultEqual(null, null));
    }

    @Test
    @SilenceLoggers(DataCheckUtils.class)
    void rejectsLaterMismatchesAndNullRows() {
        List<Object[]> expected = Arrays.asList(new Object[]{1}, new Object[]{2});
        assertEquals("Row 1 mismatch: expected [2], actual [2, 3]", assertThrows(AssertionError.class,
                () -> DataCheckUtils.checkResultEqual(
                        Arrays.asList(new Object[]{1}, new Object[]{2, 3}), expected)).getMessage());
        assertEquals("Row 1 mismatch: expected [2], actual null", assertThrows(AssertionError.class,
                () -> DataCheckUtils.checkResultEqual(
                        Arrays.asList(new Object[]{1}, null), expected)).getMessage());
    }

    @Test
    @SilenceLoggers(DataCheckUtils.class)
    void checksCacheTableRowsAndPropagatesMismatches() {
        CacheTable table = new CacheTable("result",
                Linq4j.asEnumerable(Collections.singletonList(new Object[]{1})), List.of());
        assertDoesNotThrow(() -> DataCheckUtils.check(table, Collections.singletonList(new Object[]{1})));
        assertThrows(AssertionError.class,
                () -> DataCheckUtils.check(table, Collections.singletonList(new Object[]{2})));
        assertDoesNotThrow(() -> DataCheckUtils.check(
                new CacheTable("empty", null, List.of()), List.of()));
    }

    @Test
    @SilenceLoggers(DataCheckUtils.class)
    void rejectsMismatchesEvenWhenJvmAssertionsAreDisabled() throws Exception {
        Class<?> checker = new ClassLoader(DataCheckUtils.class.getClassLoader()) {
            Class<?> loadWithoutAssertions() throws IOException {
                setClassAssertionStatus(DataCheckUtils.class.getName(), false);
                try (InputStream input = DataCheckUtils.class.getResourceAsStream("DataCheckUtils.class")) {
                    byte[] bytes = input.readAllBytes();
                    return defineClass(DataCheckUtils.class.getName(), bytes, 0, bytes.length);
                }
            }
        }.loadWithoutAssertions();
        assertFalse(checker.desiredAssertionStatus());

        InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                () -> checker.getMethod("checkResultEqual", List.class, List.class).invoke(null,
                        Collections.singletonList(new Object[]{1}),
                        Collections.singletonList(new Object[]{2})));
        assertInstanceOf(AssertionError.class, failure.getCause());
    }
}
