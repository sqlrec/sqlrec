package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class SqlGetVariable extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("GET_VARIABLE", SqlKind.OTHER);
    private SqlNode variableName;
    private SqlNode defaultValue;

    public SqlGetVariable(SqlParserPos pos, SqlNode variableName) {
        super(pos);
        this.variableName = variableName;
        this.defaultValue = null;
    }

    public SqlGetVariable(SqlParserPos pos, SqlNode variableName, SqlNode defaultValue) {
        super(pos);
        this.variableName = variableName;
        this.defaultValue = defaultValue;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> operands = new ArrayList<>();
        operands.add(variableName);
        if (defaultValue != null) {
            operands.add(defaultValue);
        }
        return operands;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (defaultValue != null) {
            writer.keyword("get_or_default");
        } else {
            writer.keyword("get");
        }
        writer.literal("(");
        variableName.unparse(writer, leftPrec, rightPrec);
        if (defaultValue != null) {
            writer.literal(",");
            defaultValue.unparse(writer, leftPrec, rightPrec);
        }
        writer.literal(")");
    }

    public SqlCharStringLiteral getVariableName() {
        return (SqlCharStringLiteral) variableName;
    }

    public SqlNode getDefaultValue() {
        return defaultValue;
    }

    public boolean hasDefaultValue() {
        return defaultValue != null;
    }
}
