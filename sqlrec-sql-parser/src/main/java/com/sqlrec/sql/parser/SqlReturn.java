package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class SqlReturn extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("RETURN", SqlKind.OTHER);

    private SqlIdentifier tableName;
    private SqlNode select;
    private SqlCallSqlFunction callSqlFunction;

    public SqlReturn(SqlParserPos pos, SqlIdentifier tableName) {
        this(pos, tableName, null, null);
    }

    public SqlReturn(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNode select,
            SqlCallSqlFunction callSqlFunction
    ) {
        super(pos);
        int sourceCount = 0;
        sourceCount += tableName == null ? 0 : 1;
        sourceCount += select == null ? 0 : 1;
        sourceCount += callSqlFunction == null ? 0 : 1;
        if (sourceCount > 1) {
            throw new IllegalArgumentException("return statement can only contain one result source");
        }
        this.tableName = tableName;
        this.select = select;
        this.callSqlFunction = callSqlFunction;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> operands = new ArrayList<>();
        if (tableName != null) {
            operands.add(tableName);
        }
        if (select != null) {
            operands.add(select);
        }
        if (callSqlFunction != null) {
            operands.add(callSqlFunction);
        }
        return operands;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("RETURN");
        if (callSqlFunction != null) {
            callSqlFunction.unparse(writer, leftPrec, rightPrec);
        } else if (select != null) {
            select.unparse(writer, leftPrec, rightPrec);
        } else if (tableName != null) {
            tableName.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public SqlNode getSelect() {
        return select;
    }

    public SqlCallSqlFunction getCallSqlFunction() {
        return callSqlFunction;
    }
}
