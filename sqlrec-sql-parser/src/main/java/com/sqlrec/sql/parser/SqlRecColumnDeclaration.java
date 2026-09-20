package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

/** A regular name/type declaration owned by the SQLRec grammar. */
public final class SqlRecColumnDeclaration extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SQLREC_COLUMN", SqlKind.COLUMN_DECL);

    private final SqlIdentifier name;
    private final SqlDataTypeSpec dataType;

    public SqlRecColumnDeclaration(
            SqlParserPos pos,
            SqlIdentifier name,
            SqlDataTypeSpec dataType) {
        super(pos);
        this.name = Objects.requireNonNull(name, "name");
        this.dataType = Objects.requireNonNull(dataType, "dataType");
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlDataTypeSpec getDataType() {
        return dataType;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(name, dataType);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);
        dataType.unparse(writer, leftPrec, rightPrec);
    }
}
