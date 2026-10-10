package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlCreateSqlFunction extends SqlCall implements SqlRecStatement {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_SQL_FUNCTION", SqlKind.OTHER);

    private final SqlIdentifier funcName;
    private final boolean orReplace;

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
        return List.of(funcName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (orReplace) {
            writer.keyword("OR REPLACE");
        }
        writer.keyword("SQL FUNCTION");
        funcName.unparse(writer, leftPrec, rightPrec);
    }

    public boolean isOrReplace() {
        return orReplace;
    }

    public SqlIdentifier getFuncName() {
        return funcName;
    }
}
