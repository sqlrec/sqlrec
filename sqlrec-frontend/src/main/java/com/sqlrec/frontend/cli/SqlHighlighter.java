package com.sqlrec.frontend.cli;

import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Lexer-based SQL syntax highlighter for the JLine REPL.
 * <p>
 * Colouring rules:
 * <ul>
 *     <li>line/block comments — faint</li>
 *     <li>single-quoted string literals — bold green</li>
 *     <li>double-quoted identifiers — bold cyan</li>
 *     <li>numbers — bold yellow</li>
 *     <li>SQL keywords — bold blue</li>
 *     <li>everything else — default</li>
 * </ul>
 */
public class SqlHighlighter implements Highlighter {

    private static final Set<String> KEYWORDS = Set.of(
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "JOIN",
            "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "ON", "AS", "AND", "OR",
            "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE",
            "END", "DISTINCT", "UNION", "ALL", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
            "DELETE", "CREATE", "TABLE", "VIEW", "DATABASE", "SCHEMA", "DROP", "ALTER", "ADD",
            "COLUMN", "INDEX", "IF", "EXISTS", "WITH", "RECURSIVE", "CAST", "INT", "INTEGER",
            "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL", "STRING", "VARCHAR",
            "BOOLEAN", "DATE", "TIMESTAMP", "ARRAY", "MAP", "ROW", "USE", "SHOW", "TABLES",
            "DATABASES", "FUNCTIONS", "DESCRIBE", "DESC", "EXPLAIN", "CACHE", "UNCACHE",
            "FUNCTION", "MODEL", "SERVICE", "API", "CHECKPOINT", "TRAIN", "EXPORT", "RECALL",
            "CALL", "OVER", "PARTITION", "WINDOW", "ROWS", "RANGE", "PRECEDING", "FOLLOWING",
            "CURRENT", "INTERVAL", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "CONSTRAINT",
            "DEFAULT", "UNIQUE", "TRUE", "FALSE", "ASC", "TBLPROPERTIES", "LOCATION", "PARTITIONED"
    );

    private Pattern errorPattern;
    private int errorIndex = -1;

    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        AttributedStringBuilder sb = new AttributedStringBuilder();
        int i = 0;
        int n = buffer.length();
        while (i < n) {
            char c = buffer.charAt(i);
            // line comment
            if (c == '-' && i + 1 < n && buffer.charAt(i + 1) == '-') {
                int start = i;
                while (i < n && buffer.charAt(i) != '\n') {
                    i++;
                }
                sb.style(AttributedStyle.DEFAULT.faint());
                sb.append(buffer, start, i);
                sb.style(AttributedStyle.DEFAULT);
                continue;
            }
            // block comment
            if (c == '/' && i + 1 < n && buffer.charAt(i + 1) == '*') {
                int start = i;
                i += 2;
                while (i < n && !(buffer.charAt(i) == '*' && i + 1 < n && buffer.charAt(i + 1) == '/')) {
                    i++;
                }
                if (i < n) {
                    i += 2;
                }
                sb.style(AttributedStyle.DEFAULT.faint());
                sb.append(buffer, start, i);
                sb.style(AttributedStyle.DEFAULT);
                continue;
            }
            // single-quoted string
            if (c == '\'') {
                int start = i;
                i++;
                while (i < n) {
                    if (buffer.charAt(i) == '\'') {
                        i++;
                        if (i < n && buffer.charAt(i) == '\'') {
                            i++;
                            continue;
                        }
                        break;
                    }
                    i++;
                }
                sb.style(AttributedStyle.BOLD.foreground(AttributedStyle.GREEN));
                sb.append(buffer, start, i);
                sb.style(AttributedStyle.DEFAULT);
                continue;
            }
            // double-quoted identifier
            if (c == '"') {
                int start = i;
                i++;
                while (i < n) {
                    if (buffer.charAt(i) == '"') {
                        i++;
                        if (i < n && buffer.charAt(i) == '"') {
                            i++;
                            continue;
                        }
                        break;
                    }
                    i++;
                }
                sb.style(AttributedStyle.BOLD.foreground(AttributedStyle.CYAN));
                sb.append(buffer, start, i);
                sb.style(AttributedStyle.DEFAULT);
                continue;
            }
            // number
            if (Character.isDigit(c)) {
                int start = i;
                while (i < n && (Character.isDigit(buffer.charAt(i)) || buffer.charAt(i) == '.')) {
                    i++;
                }
                sb.style(AttributedStyle.BOLD.foreground(AttributedStyle.YELLOW));
                sb.append(buffer, start, i);
                sb.style(AttributedStyle.DEFAULT);
                continue;
            }
            // word / keyword
            if (Character.isLetter(c) || c == '_') {
                int start = i;
                while (i < n && (Character.isLetterOrDigit(buffer.charAt(i)) || buffer.charAt(i) == '_')) {
                    i++;
                }
                String word = buffer.substring(start, i);
                if (KEYWORDS.contains(word.toUpperCase())) {
                    sb.style(AttributedStyle.BOLD.foreground(AttributedStyle.BLUE));
                } else {
                    sb.style(AttributedStyle.DEFAULT);
                }
                sb.append(word);
                sb.style(AttributedStyle.DEFAULT);
                continue;
            }
            // other characters are output as-is
            sb.append(c);
            i++;
        }
        return sb.toAttributedString();
    }

    @Override
    public void setErrorPattern(Pattern errorPattern) {
        this.errorPattern = errorPattern;
    }

    @Override
    public void setErrorIndex(int errorIndex) {
        this.errorIndex = errorIndex;
    }
}
