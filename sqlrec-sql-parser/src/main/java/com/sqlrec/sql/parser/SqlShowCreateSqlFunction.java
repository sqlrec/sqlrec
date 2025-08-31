package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlShowCreateSqlFunction extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW CREATE SQL FUNCTION", SqlKind.OTHER);
    private SqlIdentifier funcName;

    public SqlShowCreateSqlFunction(SqlParserPos pos, SqlIdentifier funcName) {
        super(pos);
        this.funcName = funcName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    public SqlIdentifier getFuncName() {
        return funcName;
    }
}
