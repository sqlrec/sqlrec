package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

import java.util.function.IntConsumer;

/**
 * Helpers for {@code unparse} of indented, comma-separated SQL node lists.
 *
 * <p>The GBDT/parser classes (CREATE MODEL / CREATE SERVICE / TRAIN MODEL / EXPORT MODEL /
 * DEFINE INPUT TABLE) all reproduced the same ~10-line skeleton:
 * <pre>{@code
 * writer.print("(\n");
 * for (int i = 0; i < list.size(); i++) {
 *     writer.print("  ");
 *     writer.setNeedWhitespace(false);
 *     list.get(i).unparse(writer, leftPrec, rightPrec);
 *     if (i < size - 1) { writer.setNeedWhitespace(false); writer.print(",\n"); }
 *     else             { writer.setNeedWhitespace(false); writer.print("\n)"); }
 * }
 * }</pre>
 * Centralising it removes the brittle manual {@code setNeedWhitespace} management and the risk of
 * the comma/close branches drifting apart across statements.
 */
public final class SqlUnparseUtils {

    private SqlUnparseUtils() {
    }

    /**
     * Unparse a {@link SqlNodeList} as an indented, comma-separated, parenthesised block:
     * {@code (\n  item0,\n  item1,\n  ...\n)}.
     *
     * <p>No-op when the list is empty or null (so callers can drop their own size guards if they
     * only guarded to avoid emitting the opening parenthesis).
     */
    public static void unparseIndentedList(SqlWriter writer, SqlNodeList list, int leftPrec, int rightPrec) {
        if (list == null) {
            return;
        }
        unparseIndentedList(writer, list.size(), i -> list.get(i).unparse(writer, leftPrec, rightPrec));
    }

    /**
     * Unparse an indented, comma-separated, parenthesised block where each item is produced by
     * {@code unparser}. Use this overload when an "item" is more than a single node (e.g.
     * {@code DEFINE INPUT TABLE} emits {@code name type} per row).
     */
    public static void unparseIndentedList(SqlWriter writer, int size, IntConsumer unparser) {
        if (size <= 0) {
            return;
        }
        writer.print("(\n");
        for (int i = 0; i < size; i++) {
            writer.print("  ");
            writer.setNeedWhitespace(false);
            unparser.accept(i);
            if (i < size - 1) {
                writer.setNeedWhitespace(false);
                writer.print(",\n");
            } else {
                writer.setNeedWhitespace(false);
                writer.print("\n)");
            }
        }
    }
}
