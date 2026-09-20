package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

/** A key/value option owned by SQLRec resource statements. */
public final class SqlRecOption extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SQLREC_OPTION", SqlKind.OTHER);

    private final SqlNode key;
    private final SqlNode value;

    public SqlRecOption(SqlParserPos pos, SqlNode key, SqlNode value) {
        super(pos);
        this.key = Objects.requireNonNull(key, "key");
        this.value = Objects.requireNonNull(value, "value");
    }

    public SqlNode getKey() {
        return key;
    }

    public SqlNode getValue() {
        return value;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(key, value);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }
}
