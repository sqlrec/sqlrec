package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

public class SqlShowCreateModel extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW_CREATE_MODEL", SqlKind.OTHER_DDL);

    private final SqlIdentifier modelName;
    private final SqlNode checkpoint;

    public SqlShowCreateModel(SqlParserPos pos, SqlIdentifier modelName, SqlNode checkpoint) {
        super(pos);
        this.modelName = modelName;
        this.checkpoint = checkpoint;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public SqlNode getCheckpoint() {
        return checkpoint;
    }

    public boolean hasCheckpoint() {
        return checkpoint != null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DESCRIBE");
        writer.keyword("MODEL");
        modelName.unparse(writer, leftPrec, rightPrec);
        if (checkpoint != null) {
            writer.keyword("CHECKPOINT");
            writer.keyword("=");
            checkpoint.unparse(writer, leftPrec, rightPrec);
        }
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER_DDL;
    }

    @Override
    public List<org.apache.calcite.sql.SqlNode> getOperandList() {
        if (checkpoint != null) {
            return Arrays.asList(modelName, checkpoint);
        }
        return Arrays.asList(modelName);
    }
}