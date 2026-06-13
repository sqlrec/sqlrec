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
    private SqlNode likeFunctionName;
    private boolean isAsync;
    private SqlNode partitionBy;
    private SqlNode partitionSize;

    public SqlCallSqlFunction(
            SqlParserPos pos,
            SqlIdentifier funcName,
            SqlGetVariable funcNameVariable,
            List<SqlNode> inputTableList,
            SqlIdentifier likeTableName,
            SqlNode likeFunctionName,
            boolean isAsync,
            SqlNode partitionBy,
            SqlNode partitionSize
    ) {
        super(pos);
        this.funcName = funcName;
        this.funcNameVariable = funcNameVariable;
        this.inputTableList = inputTableList;
        this.likeTableName = likeTableName;
        this.likeFunctionName = likeFunctionName;
        this.isAsync = isAsync;
        this.partitionBy = partitionBy;
        this.partitionSize = partitionSize;
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
        writer.keyword("CALL");
        if (funcNameVariable != null) {
            funcNameVariable.unparse(writer, leftPrec, rightPrec);
        } else {
            funcName.unparse(writer, leftPrec, rightPrec);
        }
        SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 0; i < inputTableList.size(); i++) {
            if (i > 0) {
                writer.literal(",");
            }
            inputTableList.get(i).unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
        if (likeTableName != null) {
            writer.keyword("LIKE");
            likeTableName.unparse(writer, leftPrec, rightPrec);
        }
        if (likeFunctionName != null) {
            writer.keyword("LIKE");
            writer.keyword("FUNCTION");
            likeFunctionName.unparse(writer, leftPrec, rightPrec);
        }
        if (partitionBy != null) {
            writer.keyword("PARTITION");
            writer.keyword("BY");
            partitionBy.unparse(writer, leftPrec, rightPrec);
        }
        if (partitionSize != null) {
            writer.keyword("SIZE");
            partitionSize.unparse(writer, leftPrec, rightPrec);
        }
        if (isAsync) {
            writer.keyword("ASYNC");
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

    public SqlNode getLikeFunctionName() {
        return likeFunctionName;
    }

    public boolean isAsync() {
        return isAsync;
    }

    public SqlNode getPartitionBy() {
        return partitionBy;
    }

    public SqlNode getPartitionSize() {
        return partitionSize;
    }
}
