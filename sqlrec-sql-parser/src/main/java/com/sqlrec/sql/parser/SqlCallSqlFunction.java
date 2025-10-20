package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlCallSqlFunction extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CALL", SqlKind.OTHER);
    private SqlIdentifier funcName;
    private SqlGetVariable funcNameVariable;
    private List<SqlNode> inputTableList;
    private SqlIdentifier likeTableName;
    private boolean isAsync;

    public SqlCallSqlFunction(
            SqlParserPos pos,
            SqlIdentifier funcName,
            SqlGetVariable funcNameVariable,
            List<SqlNode> inputTableList,
            SqlIdentifier likeTableName,
            boolean isAsync
    ) {
        super(pos);
        this.funcName = funcName;
        this.funcNameVariable = funcNameVariable;
        this.inputTableList = inputTableList;
        this.likeTableName = likeTableName;
        this.isAsync = isAsync;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("call");
        if (funcNameVariable != null) {
            funcNameVariable.unparse(writer, leftPrec, rightPrec);
        } else {
            funcName.unparse(writer, leftPrec, rightPrec);
        }
        writer.literal("(");
        for (int i = 0; i < inputTableList.size(); i++) {
            if (i > 0) {
                writer.literal(", ");
            }
            inputTableList.get(i).unparse(writer, leftPrec, rightPrec);
        }
        writer.literal(")");
        if (likeTableName != null) {
            writer.keyword("like");
            likeTableName.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlIdentifier getFuncName() {
        return funcName;
    }

    public List<SqlNode> getInputTableList() {
        return inputTableList;
    }

    public SqlGetVariable getFuncNameVariable() {
        return funcNameVariable;
    }

    public SqlIdentifier getLikeTableName() {
        return likeTableName;
    }

    public boolean isAsync() {
        return isAsync;
    }
}
