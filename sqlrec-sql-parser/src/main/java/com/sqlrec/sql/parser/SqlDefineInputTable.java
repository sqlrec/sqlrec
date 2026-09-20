package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SqlDefineInputTable extends SqlCall implements SqlRecStatement {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DEFINE_INPUT_TABLE", SqlKind.OTHER);
    private SqlIdentifier tableName;
    private SqlIdentifier likeTable;
    private List<SqlIdentifier> columnList;
    private List<SqlTypeNameSpec> columnTypeList;

    public SqlDefineInputTable(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier likeTable) {
        super(pos);
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.likeTable = Objects.requireNonNull(likeTable, "likeTable");
        this.columnList = Collections.emptyList();
        this.columnTypeList = Collections.emptyList();
    }

    public SqlDefineInputTable(SqlParserPos pos, SqlIdentifier tableName, List<SqlIdentifier> columnList, List<SqlTypeNameSpec> columnTypeList) {
        super(pos);
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.likeTable = null;
        this.columnList = List.copyOf(Objects.requireNonNull(columnList, "columnList"));
        this.columnTypeList = List.copyOf(Objects.requireNonNull(columnTypeList, "columnTypeList"));
        if (this.columnList.size() != this.columnTypeList.size()) {
            throw new IllegalArgumentException("column names and types must have the same size");
        }
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
        return List.copyOf(operands);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DEFINE");
        writer.keyword("INPUT");
        writer.keyword("TABLE");
        tableName.unparse(writer, leftPrec, rightPrec);
        if (likeTable != null) {
            writer.keyword("LIKE");
            likeTable.unparse(writer, leftPrec, rightPrec);
        } else {
            SqlUnparseUtils.unparseIndentedList(writer, columnList.size(), i -> {
                columnList.get(i).unparse(writer, leftPrec, rightPrec);
                columnTypeList.get(i).unparse(writer, leftPrec, rightPrec);
            });
        }
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    /** @deprecated SQL AST nodes should be treated as immutable. */
    @Deprecated(forRemoval = true)
    public void setTableName(SqlIdentifier tableName) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
    }

    public SqlIdentifier getLikeTable() {
        return likeTable;
    }

    /** @deprecated SQL AST nodes should be treated as immutable. */
    @Deprecated(forRemoval = true)
    public void setLikeTable(SqlIdentifier likeTable) {
        this.likeTable = likeTable;
    }

    public List<SqlIdentifier> getColumnList() {
        return columnList;
    }

    /** @deprecated SQL AST nodes should be treated as immutable. */
    @Deprecated(forRemoval = true)
    public void setColumnList(List<SqlIdentifier> columnList) {
        this.columnList = List.copyOf(Objects.requireNonNull(columnList, "columnList"));
    }

    public List<SqlTypeNameSpec> getColumnTypeList() {
        return columnTypeList;
    }

    /** @deprecated SQL AST nodes should be treated as immutable. */
    @Deprecated(forRemoval = true)
    public void setColumnTypeList(List<SqlTypeNameSpec> columnTypeList) {
        this.columnTypeList = List.copyOf(Objects.requireNonNull(columnTypeList, "columnTypeList"));
    }

}
