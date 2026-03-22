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

public class SqlShowCreateService extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW_CREATE_SERVICE", SqlKind.OTHER_DDL);

    private final SqlIdentifier serviceName;

    public SqlShowCreateService(SqlParserPos pos, SqlIdentifier serviceName) {
        super(pos);
        this.serviceName = serviceName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getServiceName() {
        return serviceName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DESCRIBE");
        writer.keyword("SERVICE");
        serviceName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER_DDL;
    }

    @Override
    public List<org.apache.calcite.sql.SqlNode> getOperandList() {
        return Arrays.asList(serviceName);
    }
}
