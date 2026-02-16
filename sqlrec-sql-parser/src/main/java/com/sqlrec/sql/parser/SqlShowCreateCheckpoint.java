package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

public class SqlShowCreateCheckpoint extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW_CREATE_CHECKPOINT", SqlKind.OTHER_DDL);

    private final SqlIdentifier modelName;
    private final SqlIdentifier checkpointName;

    public SqlShowCreateCheckpoint(SqlParserPos pos, SqlIdentifier modelName, SqlIdentifier checkpointName) {
        super(pos);
        this.modelName = modelName;
        this.checkpointName = checkpointName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public SqlIdentifier getCheckpointName() {
        return checkpointName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DESCRIBE");
        writer.keyword("CHECKPOINT");
        modelName.unparse(writer, leftPrec, rightPrec);
        writer.print(".");
        checkpointName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER_DDL;
    }

    @Override
    public List<org.apache.calcite.sql.SqlNode> getOperandList() {
        return Arrays.asList(modelName, checkpointName);
    }
}