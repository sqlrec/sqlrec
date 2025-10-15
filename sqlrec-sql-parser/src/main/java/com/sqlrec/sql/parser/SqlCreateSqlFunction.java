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

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("create");
        if (orReplace) {
            writer.keyword("or");
            writer.keyword("replace");
        }
        writer.keyword("sql");
        writer.keyword("function");
        funcName.unparse(writer, leftPrec, rightPrec);
    }

    public boolean isOrReplace() {
        return orReplace;
    }

    public void setOrReplace(boolean orReplace) {
        this.orReplace = orReplace;
    }

    public SqlIdentifier getFuncName() {
        return funcName;
    }

    public void setFuncName(SqlIdentifier funcName) {
        this.funcName = funcName;
    }
}
