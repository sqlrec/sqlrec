package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlShowCheckpoint extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW_CHECKPOINT", SqlKind.OTHER_DDL);

    private final SqlIdentifier modelName;

    public SqlShowCheckpoint(SqlParserPos pos, SqlIdentifier modelName) {
        super(pos);
        this.modelName = modelName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW");
        writer.keyword("CHECKPOINTS");
        modelName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER_DDL;
    }

    @Override
    public List<org.apache.calcite.sql.SqlNode> getOperandList() {
        return Collections.singletonList(modelName);
    }
}