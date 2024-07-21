package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlCache extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CACHE", SqlKind.OTHER);

    public SqlCache(SqlParserPos pos, SqlIdentifier tableName, SqlSelect select, SqlIdentifier funcName, List<SqlIdentifier> inputTableList) {
        super(pos);
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
