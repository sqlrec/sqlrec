package com.sqlrec.common.utils;

import com.sqlrec.common.schema.CacheTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DataCheckUtils {
    private static final Logger log = LoggerFactory.getLogger(DataCheckUtils.class);

    public static void checkResultEqual(List<Object[]> actualResult, List<Object[]> expectedResult) {
        if (expectedResult == null) {
            return;
        }

        assert actualResult != null : "actualResult is null";
        assert actualResult.size() == expectedResult.size() :
                "size mismatch: expected " + expectedResult.size() + ", actual " + actualResult.size();
        for (int i = 0; i < actualResult.size(); i++) {
            Object[] actualRow = actualResult.get(i);
            Object[] expectedRow = expectedResult.get(i);
            if (!java.util.Arrays.deepEquals(actualRow, expectedRow)) {
                log.error("Row {} mismatch:", i);
                log.error("  Expected: {}", java.util.Arrays.deepToString(expectedRow));
                log.error("  Actual:   {}", java.util.Arrays.deepToString(actualRow));
                assert false : "Row " + i + " mismatch";
            }
        }
    }

    public static void check(CacheTable t, List<Object[]> expectedResult) {
        checkResultEqual(t.scan(null).toList(), expectedResult);
    }
}
