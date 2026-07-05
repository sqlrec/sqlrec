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
     * Splits SQL text by top-level ';', appending each trimmed statement to
     * {@code out}. Semicolons inside single/double quotes or line/block
     * comments are skipped. Comments are stripped from the output.
     * Returns the remaining text after the last top-level ';' (may be empty).
     */
    static String splitTo(String text, List<String> out) {
        StringBuilder cur = new StringBuilder();
        int i = 0;
        int n = text.length();
        while (i < n) {
            char c = text.charAt(i);
            // line comment -- skip entirely (including the trailing newline is left for the next iteration)
            if (c == '-' && i + 1 < n && text.charAt(i + 1) == '-') {
                i += 2;
                while (i < n && text.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            // block comment /* */ — skip if closed; append "/*" if unclosed so that
            // isCompleteStatement sees a non-empty remainder and returns false
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
                    cur.append("/*");
                }
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
     * Semicolons inside single/double quotes or line/block comments are skipped.
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
