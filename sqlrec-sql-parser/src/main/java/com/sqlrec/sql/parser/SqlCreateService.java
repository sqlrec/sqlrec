package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlNode;
import java.util.List;
import java.util.ArrayList;

public class SqlCreateService extends SqlCreate {
    private final SqlIdentifier serviceName;
    private final SqlIdentifier modelName;
    private final SqlNode checkpoint;
    private final SqlNodeList propertyList;
    private final boolean ifNotExists;

    public SqlCreateService(
            SqlParserPos pos,
            SqlIdentifier serviceName,
            SqlIdentifier modelName,
            SqlNode checkpoint,
            SqlNodeList propertyList,
            boolean ifNotExists) {
        super(pos, false);
        this.serviceName = serviceName;
        this.modelName = modelName;
        this.checkpoint = checkpoint;
        this.propertyList = propertyList;
        this.ifNotExists = ifNotExists;
    }

    public SqlIdentifier getServiceName() {
        return serviceName;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public SqlNode getCheckpoint() {
        return checkpoint;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public void unparse(org.apache.calcite.sql.SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("CREATE SERVICE");
        if (ifNotExists) {
            writer.print(" IF NOT EXISTS");
        }
        writer.print(" ");
        serviceName.unparse(writer, leftPrec, rightPrec);
        writer.print(" ON MODEL");
        writer.print(" ");
        modelName.unparse(writer, leftPrec, rightPrec);
        if (checkpoint != null) {
            writer.print(" checkpoint=");
            checkpoint.unparse(writer, leftPrec, rightPrec);
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
        List<SqlNode> operands = new ArrayList<>();
        operands.add(serviceName);
        operands.add(modelName);
        if (checkpoint != null) {
            operands.add(checkpoint);
        }
        if (propertyList != null) {
            operands.addAll(propertyList);
        }
        return operands;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCreateService(
                pos,
                serviceName,
                modelName,
                checkpoint,
                propertyList,
                ifNotExists);
    }
}
