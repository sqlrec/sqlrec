package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlCache extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CACHE", SqlKind.OTHER);

    private SqlIdentifier tableName;
    private SqlSelect select;
    private SqlCallSqlFunction callSqlFunction;

    public SqlCache(SqlParserPos pos, SqlIdentifier tableName, SqlSelect select, SqlCallSqlFunction callSqlFunction) {
        super(pos);
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
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CACHE");
        writer.keyword("TABLE");
        tableName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        if (callSqlFunction != null) {
            callSqlFunction.unparse(writer, leftPrec, rightPrec);
        } else {
            select.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public SqlSelect getSelect() {
        return select;
    }

    public SqlCallSqlFunction getCallSqlFunction() {
        return callSqlFunction;
    }
}
