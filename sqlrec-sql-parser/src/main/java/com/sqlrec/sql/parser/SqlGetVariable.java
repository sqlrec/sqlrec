package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlGetVariable extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("GET_VARIABLE", SqlKind.OTHER);
    private SqlNode variableName;

    public SqlGetVariable(SqlParserPos pos, SqlNode variableName) {
        super(pos);
        this.variableName = variableName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("get");
        writer.literal("(");
        variableName.unparse(writer, leftPrec, rightPrec);
        writer.literal(")");
    }

    public SqlCharStringLiteral getVariableName() {
        return (SqlCharStringLiteral) variableName;
    }
}
