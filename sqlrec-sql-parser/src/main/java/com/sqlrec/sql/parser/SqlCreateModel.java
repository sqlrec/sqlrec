package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlCreateModel extends SqlCreate {
    private SqlIdentifier modelName;
    private SqlNodeList fieldList;
    private SqlNodeList propertyList;
    private boolean ifNotExists;

    public SqlCreateModel(
            SqlParserPos pos,
            SqlIdentifier modelName,
            SqlNodeList fieldList,
            SqlNodeList propertyList,
            boolean ifNotExists) {
        super(pos, false);
        this.modelName = modelName;
        this.fieldList = fieldList;
        this.propertyList = propertyList;
        this.ifNotExists = ifNotExists;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public SqlNodeList getFieldList() {
        return fieldList;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    public void setPropertyList(SqlNodeList propertyList) {
        this.propertyList = propertyList;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public void unparse(org.apache.calcite.sql.SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("MODEL");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        modelName.unparse(writer, leftPrec, rightPrec);
        if (fieldList != null && fieldList.size() > 0) {
            SqlUnparseUtils.unparseIndentedList(writer, fieldList, leftPrec, rightPrec);
            writer.setNeedWhitespace(true);
        }
        if (propertyList != null && propertyList.size() > 0) {
            writer.keyword("WITH");
            SqlUnparseUtils.unparseIndentedList(writer, propertyList, leftPrec, rightPrec);
        }
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }
}
