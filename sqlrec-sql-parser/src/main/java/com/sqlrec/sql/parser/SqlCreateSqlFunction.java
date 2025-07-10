package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlCreateSqlFunction extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_SQL_FUNCTION", SqlKind.OTHER);

    private SqlIdentifier funcName;
    private boolean orReplace;

    public SqlCreateSqlFunction(SqlParserPos pos, SqlIdentifier funcName, boolean orReplace) {
        super(pos);
        this.funcName = funcName;
        this.orReplace = orReplace;
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
