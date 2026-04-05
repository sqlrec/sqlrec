package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlNode;
import java.util.List;
import java.util.Collections;

public class SqlDropSqlFunction extends SqlDrop {
    private final SqlIdentifier funcName;
    private final boolean ifExists;

    public SqlDropSqlFunction(
            SqlParserPos pos,
            SqlIdentifier funcName,
            boolean ifExists) {
        super(pos);
        this.funcName = funcName;
        this.ifExists = ifExists;
    }

    public SqlIdentifier getFuncName() {
        return funcName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public void unparse(org.apache.calcite.sql.SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("SQL");
        writer.keyword("FUNCTION");
        if (ifExists) {
            writer.keyword("IF");
            writer.keyword("EXISTS");
        }
        funcName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }
}
