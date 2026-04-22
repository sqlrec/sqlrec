package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlDefineInputTable extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DEFINE_INPUT_TABLE", SqlKind.OTHER);
    private SqlIdentifier tableName;
    private SqlIdentifier likeTable;
    private List<SqlIdentifier> columnList;
    private List<SqlTypeNameSpec> columnTypeList;

    public SqlDefineInputTable(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier likeTable) {
        super(pos);
        this.tableName = tableName;
        this.likeTable = likeTable;
        this.columnList = Collections.emptyList();
        this.columnTypeList = Collections.emptyList();
    }

    public SqlDefineInputTable(SqlParserPos pos, SqlIdentifier tableName, List<SqlIdentifier> columnList, List<SqlTypeNameSpec> columnTypeList) {
        super(pos);
        this.tableName = tableName;
        this.likeTable = null;
        this.columnList = columnList;
        this.columnTypeList = columnTypeList;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> operands = new ArrayList<>();
        operands.add(tableName);
        if (likeTable != null) {
            operands.add(likeTable);
        }
        operands.addAll(columnList);
        return operands;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("define");
        writer.keyword("input");
        writer.keyword("table");
        tableName.unparse(writer, leftPrec, rightPrec);
        if (likeTable != null) {
            writer.keyword("like");
            likeTable.unparse(writer, leftPrec, rightPrec);
        } else {
            writer.keyword("(");
            for (int i = 0; i < columnList.size(); i++) {
                if (i > 0) {
                    writer.literal(", ");
                }
                columnList.get(i).unparse(writer, leftPrec, rightPrec);
                writer.literal(" ");
                columnTypeList.get(i).unparse(writer, leftPrec, rightPrec);
            }
            writer.keyword(")");
        }
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public void setTableName(SqlIdentifier tableName) {
        this.tableName = tableName;
    }

    public SqlIdentifier getLikeTable() {
        return likeTable;
    }

    public void setLikeTable(SqlIdentifier likeTable) {
        this.likeTable = likeTable;
    }

    public List<SqlIdentifier> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<SqlIdentifier> columnList) {
        this.columnList = columnList;
    }

    public List<SqlTypeNameSpec> getColumnTypeList() {
        return columnTypeList;
    }

    public void setColumnTypeList(List<SqlTypeNameSpec> columnTypeList) {
        this.columnTypeList = columnTypeList;
    }
}
