package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlCallSqlFunction extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CALL_SQL_FUNCTION", SqlKind.OTHER);
    private SqlIdentifier funcName;
    private List<SqlIdentifier> inputTableList;

    public SqlCallSqlFunction(SqlParserPos pos, SqlIdentifier funcName, List<SqlIdentifier> inputTableList) {
        super(pos);
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
