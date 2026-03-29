package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlNode;

import java.util.Arrays;
import java.util.List;

public class SqlAlterModelDropCheckpoint extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ALTER_MODEL_DROP_CHECKPOINT", SqlKind.OTHER_DDL);

    private final SqlIdentifier modelName;
    private final SqlNode checkpointName;
    private final boolean ifExists;

    public SqlAlterModelDropCheckpoint(
            SqlParserPos pos,
            SqlIdentifier modelName,
            SqlNode checkpointName,
            boolean ifExists) {
        super(pos);
        this.modelName = modelName;
        this.checkpointName = checkpointName;
        this.ifExists = ifExists;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public SqlNode getCheckpointName() {
        return checkpointName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER");
        writer.keyword("MODEL");
        modelName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("DROP");
        if (ifExists) {
            writer.keyword("IF");
            writer.keyword("EXISTS");
        }
        writer.keyword("CHECKPOINT");
        writer.print("=");
        checkpointName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER_DDL;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(modelName, checkpointName);
    }
}
