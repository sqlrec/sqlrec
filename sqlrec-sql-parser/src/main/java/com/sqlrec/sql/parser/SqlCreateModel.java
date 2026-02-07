package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlNode;
import java.util.List;
import java.util.Collections;

public class SqlCreateModel extends SqlCreate {
    private final SqlIdentifier modelName;
    private final SqlNodeList fieldList;
    private final SqlNodeList propertyList;
    private final boolean ifNotExists;

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

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public void unparse(org.apache.calcite.sql.SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("CREATE MODEL");
        if (ifNotExists) {
            writer.print(" IF NOT EXISTS");
        }
        writer.print(" ");
        modelName.unparse(writer, leftPrec, rightPrec);
        if (fieldList != null && fieldList.size() > 0) {
            writer.print(" (");
            for (int i = 0; i < fieldList.size(); i++) {
                if (i > 0) {
                    writer.print(", ");
                }
                fieldList.get(i).unparse(writer, leftPrec, rightPrec);
            }
            writer.print(")");
        }
        if (propertyList != null && propertyList.size() > 0) {
            writer.print(" WITH (");
            for (int i = 0; i < propertyList.size(); i++) {
                if (i > 0) {
                    writer.print(", ");
                }
                propertyList.get(i).unparse(writer, leftPrec, rightPrec);
            }
            writer.print(")");
        }
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }
}
