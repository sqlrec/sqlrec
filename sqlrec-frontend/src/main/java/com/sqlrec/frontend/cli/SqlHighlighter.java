package com.sqlrec.frontend.cli;

import com.sqlrec.common.utils.SqlStatementUtils;
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
 *     <li>double-quoted and backtick-quoted identifiers — bold cyan</li>
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
            SqlStatementUtils.Region region = SqlStatementUtils.scanRegion(buffer, i);
            if (region != null) {
                AttributedStyle style = switch (region.getKind()) {
                    case LINE_COMMENT, BLOCK_COMMENT -> AttributedStyle.DEFAULT.faint();
                    case STRING -> AttributedStyle.BOLD.foreground(AttributedStyle.GREEN);
                    case QUOTED_IDENTIFIER -> AttributedStyle.BOLD.foreground(AttributedStyle.CYAN);
                };
                appendStyled(sb, buffer, i, region.getEnd(), style);
                i = region.getEnd();
                continue;
            }
            char c = buffer.charAt(i);
            // number
            if (Character.isDigit(c)) {
                int start = i;
                while (i < n && (Character.isDigit(buffer.charAt(i)) || buffer.charAt(i) == '.')) {
                    i++;
                }
                appendStyled(sb, buffer, start, i,
                        AttributedStyle.BOLD.foreground(AttributedStyle.YELLOW));
                continue;
            }
            // word / keyword
            if (Character.isLetter(c) || c == '_') {
                int start = i;
                while (i < n && (Character.isLetterOrDigit(buffer.charAt(i)) || buffer.charAt(i) == '_')) {
                    i++;
                }
                String word = buffer.substring(start, i);
                AttributedStyle style = KEYWORDS.contains(word.toUpperCase())
                        ? AttributedStyle.BOLD.foreground(AttributedStyle.BLUE)
                        : AttributedStyle.DEFAULT;
                appendStyled(sb, buffer, start, i, style);
                continue;
            }
            // other characters are output as-is
            sb.append(c);
            i++;
        }
        return sb.toAttributedString();
    }

    private static void appendStyled(
            AttributedStringBuilder builder, String buffer, int start, int end, AttributedStyle style) {
        builder.style(style);
        builder.append(buffer, start, end);
        builder.style(AttributedStyle.DEFAULT);
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
