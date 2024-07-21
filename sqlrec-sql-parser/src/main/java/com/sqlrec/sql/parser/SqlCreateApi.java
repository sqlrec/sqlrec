package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlCreateApi extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_API", SqlKind.OTHER);

    public SqlCreateApi(SqlParserPos pos, SqlIdentifier apiName, SqlIdentifier funcName) {
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
