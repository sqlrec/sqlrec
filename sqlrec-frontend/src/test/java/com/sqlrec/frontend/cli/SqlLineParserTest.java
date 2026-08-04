package com.sqlrec.frontend.cli;

import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser.ParseContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SqlLineParserTest {

    private final SqlLineParser parser = new SqlLineParser();

    @Test
    public void testCompleteStatement() {
        // A statement ending with semicolon is complete
        ParsedLine result = parser.parse("SELECT 1;", 10, ParseContext.ACCEPT_LINE);

        assertNotNull(result);
    }

    @Test
    public void testIncompleteStatementThrowsEOFError() {
        // A statement without semicolon is incomplete
        assertThrows(EOFError.class,
                () -> parser.parse("SELECT 1", 8, ParseContext.ACCEPT_LINE));
    }

    @Test
    public void testEmptyLineDoesNotThrow() {
        ParsedLine result = parser.parse("", 0, ParseContext.ACCEPT_LINE);

        assertNotNull(result);
    }

    @Test
    public void testWhitespaceOnlyDoesNotThrow() {
        ParsedLine result = parser.parse("   ", 3, ParseContext.ACCEPT_LINE);

        assertNotNull(result);
    }

    @Test
    public void testStatementWithSemicolonInString() {
        // Semicolon inside a string literal should not make the statement complete
        assertThrows(EOFError.class,
                () -> parser.parse("SELECT 'hello;world'", 20, ParseContext.ACCEPT_LINE));
    }

    @Test
    public void testStatementWithSemicolonInStringAndEnding() {
        // Semicolon inside string + actual ending semicolon
        ParsedLine result = parser.parse("SELECT 'hello;world';", 21, ParseContext.ACCEPT_LINE);

        assertNotNull(result);
    }

    @Test
    public void testNonAcceptContextDoesNotCheckCompleteness() {
        // In non-ACCEPT_LINE context (e.g. secondary prompt), incomplete statements should not throw
        ParsedLine result = parser.parse("SELECT", 6, ParseContext.SECONDARY_PROMPT);

        assertNotNull(result);
    }

    @Test
    public void testMultiStatementWithSemicolon() {
        // Multiple statements separated by semicolons, ending with semicolon
        ParsedLine result = parser.parse("SELECT 1; SELECT 2;", 19, ParseContext.ACCEPT_LINE);

        assertNotNull(result);
    }
}
