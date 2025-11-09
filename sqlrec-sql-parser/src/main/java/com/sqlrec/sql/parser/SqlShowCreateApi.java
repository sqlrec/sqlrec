package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlShowCreateApi extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW CREATE API", SqlKind.OTHER);
    private SqlIdentifier apiName;

    public SqlShowCreateApi(SqlParserPos pos, SqlIdentifier apiName) {
        super(pos);
        this.apiName = apiName;
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
        writer.keyword("desc");
        writer.keyword("api");
        apiName.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getApiName() {
        return apiName;
    }
}
