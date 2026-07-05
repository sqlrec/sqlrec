package com.sqlrec.frontend.cli;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SqlStatementUtils}.
 * These are pure string-logic tests with no external dependencies, so they run
 * in the default {@code test} phase (no {@code @Tag("integration")}).
 */
class SqlStatementUtilsTest {

    // ===================== splitStatements =====================

    @Test
    void splitNullReturnsEmpty() {
        assertTrue(SqlStatementUtils.splitStatements(null).isEmpty());
    }

    @Test
    void splitEmptyReturnsEmpty() {
        assertTrue(SqlStatementUtils.splitStatements("").isEmpty());
    }

    @Test
    void splitBlankReturnsEmpty() {
        assertTrue(SqlStatementUtils.splitStatements("   \n\t  ").isEmpty());
    }

    @Test
    void splitSingleStatementWithoutSemicolon() {
        List<String> r = SqlStatementUtils.splitStatements("select 1");
        assertEquals(List.of("select 1"), r);
    }

    @Test
    void splitSingleStatementWithSemicolon() {
        List<String> r = SqlStatementUtils.splitStatements("select 1;");
        assertEquals(List.of("select 1"), r);
    }

    @Test
    void splitMultipleStatements() {
        List<String> r = SqlStatementUtils.splitStatements("select 1; select 2; select 3");
        assertEquals(List.of("select 1", "select 2", "select 3"), r);
    }

    @Test
    void splitTrailingSemicolonProducesNoEmptyTrailing() {
        List<String> r = SqlStatementUtils.splitStatements("select 1;;;");
        assertEquals(List.of("select 1"), r);
    }

    @Test
    void splitConsecutiveSemicolonsProduceNoEmptyStatements() {
        List<String> r = SqlStatementUtils.splitStatements(";;select 1;;select 2;;");
        assertEquals(List.of("select 1", "select 2"), r);
    }

    @Test
    void splitTrimsWhitespaceAroundStatements() {
        List<String> r = SqlStatementUtils.splitStatements("   select 1   ;   select 2   ");
        assertEquals(List.of("select 1", "select 2"), r);
    }

    @Test
    void splitSemicolonInSingleQuotedStringIsNotASeparator() {
        List<String> r = SqlStatementUtils.splitStatements("select 'a;b;c'; select 2");
        assertEquals(List.of("select 'a;b;c'", "select 2"), r);
    }

    @Test
    void splitSemicolonInDoubleQuotedIdentifierIsNotASeparator() {
        List<String> r = SqlStatementUtils.splitStatements("select \"a;b;c\"; select 2");
        assertEquals(List.of("select \"a;b;c\"", "select 2"), r);
    }

    @Test
    void splitSemicolonInLineCommentIsNotASeparator() {
        List<String> r = SqlStatementUtils.splitStatements("select 1 -- a;b;c\n; select 2");
        assertEquals(List.of("select 1", "select 2"), r);
    }

    @Test
    void splitSemicolonInBlockCommentIsNotASeparator() {
        List<String> r = SqlStatementUtils.splitStatements("select 1 /* a;b;c */; select 2");
        assertEquals(List.of("select 1", "select 2"), r);
    }

    @Test
    void splitEscapedSingleQuoteDoesNotTerminateString() {
        // '' inside a single-quoted string is an escaped quote, not a terminator
        List<String> r = SqlStatementUtils.splitStatements("select 'it''s a;b'; select 2");
        assertEquals(List.of("select 'it''s a;b'", "select 2"), r);
    }

    @Test
    void splitEscapedDoubleQuoteDoesNotTerminateIdentifier() {
        // "" inside a double-quoted identifier is an escaped quote
        List<String> r = SqlStatementUtils.splitStatements("select \"a\"\"b;c\"; select 2");
        assertEquals(List.of("select \"a\"\"b;c\"", "select 2"), r);
    }

    @Test
    void splitPreservesQuotesInOutput() {
        String sql = "select 1 'str' \"id\"";
        List<String> r = SqlStatementUtils.splitStatements(sql + ";");
        assertEquals(List.of(sql), r);
    }

    @Test
    void splitHandlesSqlrecDialect() {
        // Non-standard sqlrec extensions must pass through untouched
        List<String> r = SqlStatementUtils.splitStatements(
                "cache table t1 as select cast(1 as bigint) as user_id; call main_rec(t1)");
        assertEquals(
                List.of("cache table t1 as select cast(1 as bigint) as user_id", "call main_rec(t1)"),
                r);
    }

    @Test
    void splitUnclosedQuoteKeepsRestInLastStatement() {
        // An unterminated string just means the statement never completes; everything
        // after the opening quote becomes part of the final (incomplete) statement.
        List<String> r = SqlStatementUtils.splitStatements("select 'unterminated; select 2");
        assertEquals(List.of("select 'unterminated; select 2"), r);
    }

    // ===================== isCompleteStatement =====================

    @Test
    void isCompleteNullIsTrue() {
        assertTrue(SqlStatementUtils.isCompleteStatement(null));
    }

    @Test
    void isCompleteBlankIsTrue() {
        assertTrue(SqlStatementUtils.isCompleteStatement("   "));
    }

    @Test
    void isCompleteNoSemicolonIsFalse() {
        assertFalse(SqlStatementUtils.isCompleteStatement("select 1"));
    }

    @Test
    void isCompleteTrailingSemicolonIsTrue() {
        assertTrue(SqlStatementUtils.isCompleteStatement("select 1;"));
    }

    @Test
    void isCompleteTrailingSemicolonWithWhitespaceIsTrue() {
        assertTrue(SqlStatementUtils.isCompleteStatement("select 1;   \n"));
    }

    @Test
    void isCompleteUnclosedSingleQuoteIsFalse() {
        assertFalse(SqlStatementUtils.isCompleteStatement("select 'a"));
    }

    @Test
    void isCompleteClosedSingleQuoteWithSemicolonIsTrue() {
        assertTrue(SqlStatementUtils.isCompleteStatement("select 'a';"));
    }

    @Test
    void isCompleteEscapedQuoteDoesNotCloseString() {
        assertFalse(SqlStatementUtils.isCompleteStatement("select 'it''s"));
    }

    @Test
    void isCompleteEscapedQuoteProperlyClosedIsTrue() {
        assertTrue(SqlStatementUtils.isCompleteStatement("select 'it''s';"));
    }

    @Test
    void isCompleteUnclosedDoubleQuoteIsFalse() {
        assertFalse(SqlStatementUtils.isCompleteStatement("select \"a"));
    }

    @Test
    void isCompleteClosedDoubleQuoteWithSemicolonIsTrue() {
        assertTrue(SqlStatementUtils.isCompleteStatement("select \"a\";"));
    }

    @Test
    void isCompleteUnclosedBlockCommentIsFalse() {
        assertFalse(SqlStatementUtils.isCompleteStatement("/* not closed"));
    }

    @Test
    void isCompleteClosedBlockCommentWithSemicolonIsTrue() {
        assertTrue(SqlStatementUtils.isCompleteStatement("/* closed */ select 1;"));
    }

    @Test
    void isCompleteLineCommentWithoutNewlineDoesNotChangeLastSignificant() {
        // "select 1; -- trailing comment (no newline)" — last significant char is ';'
        assertTrue(SqlStatementUtils.isCompleteStatement("select 1; -- trailing"));
    }

    @Test
    void isCompleteLineCommentFollowedByMoreInput() {
        // After the comment line, there is no ';' so it is incomplete
        assertFalse(SqlStatementUtils.isCompleteStatement("select 1 -- comment\n more"));
    }

    @Test
    void isCompleteLineCommentThenSemicolonOnNextLine() {
        assertTrue(SqlStatementUtils.isCompleteStatement("select 1\n-- comment\n;"));
    }

    @Test
    void isCompleteSemicolonInsideStringDoesNotCount() {
        assertFalse(SqlStatementUtils.isCompleteStatement("select 'a;b'"));
    }

    @Test
    void isCompleteSemicolonInsideBlockCommentDoesNotCount() {
        // The ';' is inside a comment, last significant char is 't' of select -> false
        assertFalse(SqlStatementUtils.isCompleteStatement("select /* ; */ 1"));
    }

    @Test
    void isCompleteSemicolonAfterBlockComment() {
        assertTrue(SqlStatementUtils.isCompleteStatement("select /* x */ 1;"));
    }
}
