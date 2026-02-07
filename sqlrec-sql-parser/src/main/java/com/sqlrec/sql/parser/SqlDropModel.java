package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlNode;
import java.util.List;
import java.util.Collections;

public class SqlDropModel extends SqlDrop {
    private final SqlIdentifier modelName;
    private final boolean ifExists;

    public SqlDropModel(
            SqlParserPos pos,
            SqlIdentifier modelName,
            boolean ifExists) {
        super(pos);
        this.modelName = modelName;
        this.ifExists = ifExists;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public void unparse(org.apache.calcite.sql.SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("MODEL");
        if (ifExists) {
            writer.keyword("IF");
            writer.keyword("EXISTS");
        }
        modelName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }
}
