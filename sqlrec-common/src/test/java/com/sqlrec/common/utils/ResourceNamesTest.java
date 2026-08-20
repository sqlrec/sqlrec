package com.sqlrec.common.utils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * ResourceNames is the single normalization entry for sqlrec resource names
 * (sql function / api / model / service / checkpoint): trim + lower case.
 */
class ResourceNamesTest {

    @Test
    void normalizeTrimsAndLowerCases() {
        assertEquals("my_func", ResourceNames.normalize("  My_Func  "));
        assertEquals("myfunc", ResourceNames.normalize("MYFUNC"));
    }

    @Test
    void normalizeHandlesNull() {
        assertNull(ResourceNames.normalize(null));
        assertNull(ResourceNames.of(null));
    }

    @Test
    void normalizeKeepsNonAsciiCharacters() {
        assertEquals("最佳model", ResourceNames.normalize("最佳Model"));
    }

    @Test
    void ofUsesSimpleNameOfIdentifier() {
        SqlIdentifier identifier = new SqlIdentifier("MyFunc", SqlParserPos.ZERO);
        assertEquals("myfunc", ResourceNames.of(identifier));
    }
}
