package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlCache extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CACHE", SqlKind.OTHER);

    private SqlIdentifier tableName;
    private SqlSelect select;
    private SqlIdentifier funcName;
    private List<SqlIdentifier> inputTableList;

    public SqlCache(SqlParserPos pos, SqlIdentifier tableName, SqlSelect select, SqlIdentifier funcName, List<SqlIdentifier> inputTableList) {
        super(pos);
        this.tableName = tableName;
        this.select = select;
        this.funcName = funcName;
        this.inputTableList = inputTableList;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }
}
