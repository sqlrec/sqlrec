package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlNode;
import java.util.List;
import java.util.ArrayList;

public class SqlTrainModel extends SqlCreate {
    private final SqlIdentifier modelName;
    private final SqlNode checkpoint;
    private final SqlIdentifier dataSource;
    private final SqlNode whereCondition;
    private final SqlNodeList propertyList;

    public SqlTrainModel(
            SqlParserPos pos,
            SqlIdentifier modelName,
            SqlNode checkpoint,
            SqlIdentifier dataSource,
            SqlNode whereCondition,
            SqlNodeList propertyList) {
        super(pos, false);
        this.modelName = modelName;
        this.checkpoint = checkpoint;
        this.dataSource = dataSource;
        this.whereCondition = whereCondition;
        this.propertyList = propertyList;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public SqlNode getCheckpoint() {
        return checkpoint;
    }

    public SqlIdentifier getDataSource() {
        return dataSource;
    }

    public SqlNode getWhereCondition() {
        return whereCondition;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    @Override
    public void unparse(org.apache.calcite.sql.SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("TRAIN MODEL");
        writer.print(" ");
        modelName.unparse(writer, leftPrec, rightPrec);
        if (checkpoint != null) {
            writer.print(" checkpoint=");
            checkpoint.unparse(writer, leftPrec, rightPrec);
        }
        writer.print(" ON");
        writer.print(" ");
        dataSource.unparse(writer, leftPrec, rightPrec);
        if (whereCondition != null) {
            writer.print(" WHERE");
            writer.print(" ");
            whereCondition.unparse(writer, leftPrec, rightPrec);
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
        operands.add(modelName);
        if (checkpoint != null) {
            operands.add(checkpoint);
        }
        operands.add(dataSource);
        if (whereCondition != null) {
            operands.add(whereCondition);
        }
        if (propertyList != null) {
            operands.addAll(propertyList);
        }
        return operands;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlTrainModel(
                pos,
                modelName,
                checkpoint,
                dataSource,
                whereCondition,
                propertyList);
    }
}
