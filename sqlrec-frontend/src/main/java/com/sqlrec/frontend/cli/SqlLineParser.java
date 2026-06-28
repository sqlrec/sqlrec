package com.sqlrec.frontend.cli;

import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.DefaultParser;

/**
 * JLine parser that enables multi-line SQL input via the {@link EOFError}
 * mechanism. When the user presses Enter and the current buffer is not a
 * complete SQL statement (i.e. does not end with an unquoted, un-commented
 * semicolon), an {@code EOFError} is thrown, causing JLine to display the
 * secondary prompt and continue reading.
 */
public class SqlLineParser extends DefaultParser {

    @Override
    public ParsedLine parse(String line, int cursor, ParseContext context) {
        ParsedLine parsed = super.parse(line, cursor, context);
        // Only check completeness when the user presses Enter to submit
        if (context != ParseContext.ACCEPT_LINE) {
            return parsed;
        }
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return parsed;
        }
        if (!SqlStatementUtils.isCompleteStatement(line)) {
            throw new EOFError(line.length(), cursor, "Incomplete statement", ";");
        }
        return parsed;
    }
}
