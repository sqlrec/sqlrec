package com.sqlrec.frontend.cli;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure (no JLine / Calcite dependency) helpers for SQL statement splitting
 * and completeness detection. Safe to unit-test in isolation.
 * <p>
 * The lexer only recognises quotes and comments; it does not build a parse
 * tree, so it is transparent to sqlrec-specific dialect extensions such as
 * {@code cache table ...} or {@code call main_rec(...)}.
 */
public final class SqlStatementUtils {

    private SqlStatementUtils() {
    }

    /**
     * Split SQL text by top-level ';', ignoring empty statements.
     * Semicolons inside single/double quotes or line/block comments are skipped.
     *
     * @return a list of non-empty trimmed statements; never {@code null}.
     */
    public static List<String> splitStatements(String text) {
        List<String> stmts = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            return stmts;
        }
        StringBuilder cur = new StringBuilder();
        int i = 0;
        int n = text.length();
        while (i < n) {
            char c = text.charAt(i);
            // line comment --
            if (c == '-' && i + 1 < n && text.charAt(i + 1) == '-') {
                int start = i;
                while (i < n && text.charAt(i) != '\n') {
                    i++;
                }
                cur.append(text, start, i);
                continue;
            }
            // block comment /* */
            if (c == '/' && i + 1 < n && text.charAt(i + 1) == '*') {
                int start = i;
                i += 2;
                while (i < n && !(text.charAt(i) == '*' && i + 1 < n && text.charAt(i + 1) == '/')) {
                    i++;
                }
                if (i < n) {
                    i += 2;
                }
                cur.append(text, start, i);
                continue;
            }
            // single-quoted string
            if (c == '\'') {
                cur.append(c);
                i++;
                while (i < n) {
                    char cc = text.charAt(i);
                    cur.append(cc);
                    i++;
                    if (cc == '\'') {
                        if (i < n && text.charAt(i) == '\'') {
                            cur.append('\'');
                            i++;
                            continue;
                        }
                        break;
                    }
                }
                continue;
            }
            // double-quoted identifier
            if (c == '"') {
                cur.append(c);
                i++;
                while (i < n) {
                    char cc = text.charAt(i);
                    cur.append(cc);
                    i++;
                    if (cc == '"') {
                        if (i < n && text.charAt(i) == '"') {
                            cur.append('"');
                            i++;
                            continue;
                        }
                        break;
                    }
                }
                continue;
            }
            if (c == ';') {
                String s = cur.toString().trim();
                if (!s.isEmpty()) {
                    stmts.add(s);
                }
                cur.setLength(0);
                i++;
                continue;
            }
            cur.append(c);
            i++;
        }
        String rest = cur.toString().trim();
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
        int i = 0;
        int n = text.length();
        char lastSignificant = 0;
        while (i < n) {
            char c = text.charAt(i);
            if (c == '-' && i + 1 < n && text.charAt(i + 1) == '-') {
                i += 2;
                while (i < n && text.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            if (c == '/' && i + 1 < n && text.charAt(i + 1) == '*') {
                i += 2;
                boolean closed = false;
                while (i < n) {
                    if (text.charAt(i) == '*' && i + 1 < n && text.charAt(i + 1) == '/') {
                        i += 2;
                        closed = true;
                        break;
                    }
                    i++;
                }
                if (!closed) {
                    return false;
                }
                lastSignificant = 'a';
                continue;
            }
            if (c == '\'') {
                i++;
                boolean closed = false;
                while (i < n) {
                    if (text.charAt(i) == '\'') {
                        if (i + 1 < n && text.charAt(i + 1) == '\'') {
                            i += 2;
                            continue;
                        }
                        i++;
                        closed = true;
                        break;
                    }
                    i++;
                }
                if (!closed) {
                    return false;
                }
                lastSignificant = 'a';
                continue;
            }
            if (c == '"') {
                i++;
                boolean closed = false;
                while (i < n) {
                    if (text.charAt(i) == '"') {
                        if (i + 1 < n && text.charAt(i + 1) == '"') {
                            i += 2;
                            continue;
                        }
                        i++;
                        closed = true;
                        break;
                    }
                    i++;
                }
                if (!closed) {
                    return false;
                }
                lastSignificant = 'a';
                continue;
            }
            if (!Character.isWhitespace(c)) {
                lastSignificant = c;
            }
            i++;
        }
        return lastSignificant == ';';
    }
}
