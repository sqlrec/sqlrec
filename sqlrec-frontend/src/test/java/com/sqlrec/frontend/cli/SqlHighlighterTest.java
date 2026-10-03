package com.sqlrec.frontend.cli;

import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Text preservation and token style tests for {@link SqlHighlighter}.
 * <p>
 * The highlighter must never alter the text content — it only attaches ANSI
 * styles. These tests verify both the original text and style boundaries,
 * including escaped quotes and incomplete tokens. ({@link SqlHighlighter#highlight} does not use its
 * {@link LineReader} argument, so {@code null} is acceptable.)
 */
class SqlHighlighterTest {

    private final SqlHighlighter highlighter = new SqlHighlighter();

    private void assertTextPreserved(String buffer) {
        AttributedString result = assertDoesNotThrow(() -> highlighter.highlight(null, buffer));
        assertEquals(buffer, result.toString(),
                "highlighter must preserve text content for: " + buffer);
    }

    @Test
    void emptyBuffer() {
        assertTextPreserved("");
    }

    @Test
    void simpleSelect() {
        assertTextPreserved("select 1");
    }

    @Test
    void keywordsCaseInsensitive() {
        assertTextPreserved("SELECT * FROM t WHERE a = 1");
    }

    @Test
    void stringWithSemicolon() {
        assertTextPreserved("select 'a;b;c' from t");
    }

    @Test
    void doubleQuotedIdentifier() {
        assertTextPreserved("select \"col name\" from \"my table\"");
    }

    @Test
    void lineComment() {
        assertTextPreserved("select 1 -- this is a comment\nfrom t");
    }

    @Test
    void blockComment() {
        assertTextPreserved("select /* block\ncomment */ 1 from t");
    }

    @Test
    void numbers() {
        assertTextPreserved("select 1, 2.5, 100 from t");
    }

    @Test
    void mixedContent() {
        assertTextPreserved("select a, 'str', \"id\", 123 -- c\nfrom t /* b */ where x = 1;");
    }

    @Test
    void unterminatedStringDoesNotThrow() {
        assertTextPreserved("select 'unterminated");
    }

    @Test
    void unterminatedBlockCommentDoesNotThrow() {
        assertTextPreserved("select 1 /* unterminated");
    }

    @Test
    void setErrorPatternAndIndexDoNotThrow() {
        assertDoesNotThrow(() -> highlighter.setErrorPattern(null));
        assertDoesNotThrow(() -> highlighter.setErrorIndex(-1));
    }

    @Test
    void appliesTokenStylesAndResetsBetweenTokens() {
        String buffer = "select ordinary 12.3 'text' \"name\"";
        AttributedString result = highlighter.highlight(null, buffer);

        assertEquals(buffer, result.toString());
        assertStyledRange(result, 0, 6, AttributedStyle.BOLD.foreground(AttributedStyle.BLUE));
        assertStyledRange(result, 6, 16, AttributedStyle.DEFAULT);
        assertStyledRange(result, 16, 20, AttributedStyle.BOLD.foreground(AttributedStyle.YELLOW));
        assertStyledRange(result, 20, 21, AttributedStyle.DEFAULT);
        assertStyledRange(result, 21, 27, AttributedStyle.BOLD.foreground(AttributedStyle.GREEN));
        assertStyledRange(result, 27, 28, AttributedStyle.DEFAULT);
        assertStyledRange(result, 28, buffer.length(), AttributedStyle.BOLD.foreground(AttributedStyle.CYAN));
    }

    @Test
    void keepsEscapedQuotesAndCommentMarkersInsideQuotedTokens() {
        for (char quote : new char[]{'\'', '"'}) {
            String token = quote + "a" + quote + quote + "--/*b" + quote;
            String buffer = token + " x";
            AttributedString result = highlighter.highlight(null, buffer);
            AttributedStyle style = AttributedStyle.BOLD.foreground(
                    quote == '\'' ? AttributedStyle.GREEN : AttributedStyle.CYAN);

            assertEquals(buffer, result.toString());
            assertStyledRange(result, 0, token.length(), style);
            assertStyledRange(result, token.length(), buffer.length(), AttributedStyle.DEFAULT);
        }
    }

    @Test
    void stylesUnterminatedQuotesThroughEndOfInput() {
        for (char quote : new char[]{'\'', '"'}) {
            String buffer = quote + "unfinished" + quote + quote;
            AttributedString result = highlighter.highlight(null, buffer);

            assertEquals(buffer, result.toString());
            assertStyledRange(result, 0, buffer.length(), AttributedStyle.BOLD.foreground(
                    quote == '\'' ? AttributedStyle.GREEN : AttributedStyle.CYAN));
        }
    }

    @Test
    void commentStylesStopAtTheirOriginalBoundaries() {
        String buffer = "-- 'select'\nx /* \"select\" */ y /* unfinished";
        AttributedString result = highlighter.highlight(null, buffer);
        int lineEnd = buffer.indexOf('\n');
        int blockStart = buffer.indexOf("/*");
        int blockEnd = buffer.indexOf("*/") + 2;
        int unfinishedStart = buffer.lastIndexOf("/*");

        assertEquals(buffer, result.toString());
        assertStyledRange(result, 0, lineEnd, AttributedStyle.DEFAULT.faint());
        assertStyledRange(result, lineEnd, blockStart, AttributedStyle.DEFAULT);
        assertStyledRange(result, blockStart, blockEnd, AttributedStyle.DEFAULT.faint());
        assertStyledRange(result, blockEnd, unfinishedStart, AttributedStyle.DEFAULT);
        assertStyledRange(result, unfinishedStart, buffer.length(), AttributedStyle.DEFAULT.faint());
    }

    private static void assertStyledRange(
            AttributedString result, int start, int end, AttributedStyle expected) {
        for (int i = start; i < end; i++) {
            assertEquals(expected, result.styleAt(i), "style at character " + i);
        }
    }
}
