package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlNode;
import java.util.List;
import java.util.Collections;

public class SqlDropService extends SqlDrop {
    private final SqlIdentifier serviceName;
    private final boolean ifExists;

    public SqlDropService(
            SqlParserPos pos,
            SqlIdentifier serviceName,
            boolean ifExists) {
        super(pos);
        this.serviceName = serviceName;
        this.ifExists = ifExists;
    }

    public SqlIdentifier getServiceName() {
        return serviceName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public void unparse(org.apache.calcite.sql.SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("SERVICE");
        if (ifExists) {
            writer.keyword("IF");
            writer.keyword("EXISTS");
        }
        serviceName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }
}
