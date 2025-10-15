package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlReturn extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("RETURN", SqlKind.OTHER);

    private SqlIdentifier tableName;

    public SqlReturn(SqlParserPos pos, SqlIdentifier tableName) {
        super(pos);
        this.tableName = tableName;
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
        writer.keyword("return");
        if (tableName != null) {
            tableName.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }
}
