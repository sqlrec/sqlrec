package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlAssert extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ASSERT", SqlKind.OTHER);

    private SqlNode select;

    public SqlAssert(SqlParserPos pos, SqlNode select) {
        super(pos);
        this.select = select;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(select);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ASSERT");
        select.unparse(writer, leftPrec, rightPrec);
    }

    public SqlNode getSelect() {
        return select;
    }
}
