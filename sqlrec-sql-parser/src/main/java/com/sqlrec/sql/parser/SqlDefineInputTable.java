package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlDefineInputTable extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DEFINE_INPUT_TABLE", SqlKind.OTHER);
    private SqlIdentifier tableName;
    private List<SqlIdentifier> columnList;
    private List<SqlTypeNameSpec> columnTypeList;

    public SqlDefineInputTable(SqlParserPos pos, SqlIdentifier tableName, List<SqlIdentifier> columnList, List<SqlTypeNameSpec> columnTypeList) {
        super(pos);
        this.tableName = tableName;
        this.columnList = columnList;
        this.columnTypeList = columnTypeList;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public void setTableName(SqlIdentifier tableName) {
        this.tableName = tableName;
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
