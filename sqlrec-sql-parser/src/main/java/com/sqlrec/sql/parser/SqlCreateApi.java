package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlCreateApi extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_API", SqlKind.OTHER);

    private SqlIdentifier apiName;
    private SqlIdentifier funcName;
    private boolean orReplace;

    public SqlCreateApi(SqlParserPos pos, SqlIdentifier apiName, SqlIdentifier funcName, boolean orReplace) {
        super(pos);
        this.apiName = apiName;
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

    public String getApiName() {
        return apiName.getSimple();
    }

    public String getFuncName() {
        return funcName.getSimple();
    }

    public boolean isOrReplace() {
        return orReplace;
    }
}
