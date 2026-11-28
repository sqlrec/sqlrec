package com.sqlrec.common.utils;

import java.util.ArrayList;
import java.util.List;

/** Scans SQL quotes and comments for statement splitting and syntax highlighting. */
public final class SqlStatementUtils {

    private SqlStatementUtils() {
    }

    public enum RegionKind {
        LINE_COMMENT, BLOCK_COMMENT, STRING, QUOTED_IDENTIFIER
    }

    public static final class Region {
        private final RegionKind kind;
        private final int end;
        private final boolean closed;

        private Region(RegionKind kind, int end, boolean closed) {
            this.kind = kind;
            this.end = end;
            this.closed = closed;
        }

        public RegionKind getKind() {
            return kind;
        }

        /** Returns the exclusive end offset. */
        public int getEnd() {
            return end;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    /** Scans a quote or comment beginning at {@code start}, or returns null at an ordinary character. */
    public static Region scanRegion(String text, int start) {
        char first = text.charAt(start);
        if (first == '-' && text.startsWith("--", start)) {
            int end = text.indexOf('\n', start + 2);
            return new Region(RegionKind.LINE_COMMENT, end < 0 ? text.length() : end, true);
        }
        if (first == '/' && text.startsWith("/*", start)) {
            int end = text.indexOf("*/", start + 2);
            return new Region(RegionKind.BLOCK_COMMENT, end < 0 ? text.length() : end + 2, end >= 0);
        }
        if (first == '\'' || first == '"' || first == '`') {
            return scanQuotedRegion(text, start, first);
        }
        return null;
    }

    private static Region scanQuotedRegion(String text, int start, char quote) {
        RegionKind kind = quote == '\'' ? RegionKind.STRING : RegionKind.QUOTED_IDENTIFIER;
        int end = start + 1;
        while (end < text.length()) {
            char current = text.charAt(end);
            end++;
            if (current == quote) {
                if (end == text.length() || text.charAt(end) != quote) {
                    return new Region(kind, end, true);
                }
                end++;
            }
        }
        return new Region(kind, end, false);
    }

    /**
     * Splits SQL text by top-level ';', appending each trimmed statement to
     * {@code out}. Semicolons inside single/double/backtick quotes or line/block
     * comments are skipped. Comments are stripped from the output.
     * Returns the remaining text after the last top-level ';' (may be empty).
     */
    static String splitTo(String text, List<String> out) {
        StringBuilder cur = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            Region region = scanRegion(text, i);
            if (region != null) {
                switch (region.getKind()) {
                    case LINE_COMMENT:
                        break;
                    case BLOCK_COMMENT:
                        // Separate adjacent tokens or leave a marker for an unclosed comment.
                        cur.append(region.isClosed() ? " " : "/*");
                        break;
                    case STRING:
                    case QUOTED_IDENTIFIER:
                        cur.append(text, i, region.getEnd());
                        break;
                }
                i = region.getEnd();
                continue;
            }
            char c = text.charAt(i);
            if (c == ';') {
                String s = cur.toString().trim();
                if (!s.isEmpty()) {
                    out.add(s);
                }
                cur.setLength(0);
                i++;
                continue;
            }
            cur.append(c);
            i++;
        }
        return cur.toString();
    }

    /**
     * Split SQL text by top-level ';', ignoring empty statements.
     * Semicolons inside single/double/backtick quotes or line/block comments are skipped.
     *
     * @return a list of non-empty trimmed statements; never {@code null}.
     */
    public static List<String> splitStatements(String text) {
        List<String> stmts = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            return stmts;
        }
        String rest = splitTo(text, stmts).trim();
        if (!rest.isEmpty()) {
            stmts.add(rest);
        }
        return stmts;
    }

    /**
     * Returns true if the text forms a complete SQL statement: the last
     * significant (non-whitespace) top-level character is ';' and there are no
     * unclosed quotes or block comments. An empty/blank input is considered
     * complete.
     */
    public static boolean isCompleteStatement(String text) {
        if (text == null || text.trim().isEmpty()) {
            return true;
        }
        List<String> sink = new ArrayList<>();
        String rest = splitTo(text, sink).trim();
        return rest.isEmpty();
    }
}
