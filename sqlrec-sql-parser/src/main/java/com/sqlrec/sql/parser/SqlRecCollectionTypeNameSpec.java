package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;

import java.util.Objects;

/** SQLRec's {@code ARRAY<T>} / {@code MULTISET<T>} type spelling. */
public final class SqlRecCollectionTypeNameSpec extends SqlCollectionTypeNameSpec {
    private final SqlTypeNameSpec elementType;
    private final SqlTypeName collectionType;
    private final boolean elementNullable;

    public SqlRecCollectionTypeNameSpec(
            SqlTypeNameSpec elementType,
            SqlTypeName collectionType,
            boolean elementNullable,
            SqlParserPos pos) {
        super(elementType, collectionType, pos);
        this.elementType = Objects.requireNonNull(elementType, "elementType");
        this.collectionType = Objects.requireNonNull(collectionType, "collectionType");
        this.elementNullable = elementNullable;
    }

    public boolean isElementNullable() {
        return elementNullable;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(collectionType.getName());
        writer.setNeedWhitespace(false);
        writer.print("<");
        writer.setNeedWhitespace(true);
        elementType.unparse(writer, leftPrec, rightPrec);
        if (!elementNullable) {
            writer.keyword("NOT NULL");
        }
        writer.print(">");
    }

    @Override
    public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
        if (!(spec instanceof SqlRecCollectionTypeNameSpec that)) {
            return litmus.fail("{} != {}", this, spec);
        }
        if (collectionType != that.collectionType || elementNullable != that.elementNullable) {
            return litmus.fail("{} != {}", this, spec);
        }
        return elementType.equalsDeep(that.elementType, litmus);
    }
}
