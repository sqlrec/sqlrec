package com.sqlrec.frontend.cli;

import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Smoke tests for {@link SqlHighlighter}.
 * <p>
 * The highlighter must never alter the text content — it only attaches ANSI
 * styles — so the core invariant verified here is that
 * {@code highlight(null, buffer).toString()} equals the input buffer for a range
 * of SQL inputs. ({@link SqlHighlighter#highlight} does not use its
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
}
