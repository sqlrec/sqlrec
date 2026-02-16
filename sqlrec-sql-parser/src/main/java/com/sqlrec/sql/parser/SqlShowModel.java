package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlShowModel extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW_MODELS", SqlKind.OTHER_DDL);

    public SqlShowModel(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW");
        writer.keyword("MODELS");
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER_DDL;
    }

    @Override
    public List<org.apache.calcite.sql.SqlNode> getOperandList() {
        return Collections.emptyList();
    }
}