package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SqlCache extends SqlCall implements SqlRecStatement {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CACHE", SqlKind.OTHER);

    private final SqlIdentifier tableName;
    private final SqlNode select;
    private final SqlCallSqlFunction callSqlFunction;

    public SqlCache(SqlParserPos pos, SqlIdentifier tableName, SqlNode select, SqlCallSqlFunction callSqlFunction) {
        super(pos);
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        if ((select == null) == (callSqlFunction == null)) {
            throw new IllegalArgumentException("cache statement requires exactly one source");
        }
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
        operands.add(tableName);
        if (select != null) {
            operands.add(select);
        }
        if (callSqlFunction != null) {
            operands.add(callSqlFunction);
        }
        return List.copyOf(operands);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CACHE");
        writer.keyword("TABLE");
        tableName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        if (callSqlFunction != null) {
            callSqlFunction.unparse(writer, leftPrec, rightPrec);
        } else if (select != null) {
            select.unparse(writer, leftPrec, rightPrec);
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
