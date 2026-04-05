package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlNode;
import java.util.List;
import java.util.Collections;

public class SqlDropApi extends SqlDrop {
    private final SqlIdentifier apiName;
    private final boolean ifExists;

    public SqlDropApi(
            SqlParserPos pos,
            SqlIdentifier apiName,
            boolean ifExists) {
        super(pos);
        this.apiName = apiName;
        this.ifExists = ifExists;
    }

    public SqlIdentifier getApiName() {
        return apiName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public void unparse(org.apache.calcite.sql.SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("API");
        if (ifExists) {
            writer.keyword("IF");
            writer.keyword("EXISTS");
        }
        apiName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }
}
