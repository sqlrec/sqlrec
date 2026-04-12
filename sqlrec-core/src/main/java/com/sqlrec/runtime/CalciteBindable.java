package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.compiler.SqlTypeChecker;
import com.sqlrec.utils.NodeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlNode;

import java.util.*;

public class CalciteBindable extends BindableInterface {
    private Map<String, Object> parameters;
    private Bindable<Object[]> bindable;
    private RelNode bestExp;
    private SqlNode sqlNode;
    private Set<String> readTables;
    private Set<String> writeTables;

    private String logicalPlan;
    private String physicalPlan;
    private String javaExpression;

    public CalciteBindable(
            Map<String, Object> parameters,
            Bindable<Object[]> bindable,
            RelNode bestExp,
            SqlNode sqlNode,
            String logicalPlan,
            String physicalPlan,
            String javaExpression
    ) {
        this.parameters = parameters;
        this.bindable = bindable;
        this.bestExp = bestExp;
        this.sqlNode = sqlNode;
        this.logicalPlan = logicalPlan;
        this.physicalPlan = physicalPlan;
        this.javaExpression = javaExpression;

        List<String> readTables = NodeUtils.getTableFromSqlNode(sqlNode);
        this.readTables = new HashSet<>(readTables);

        List<String> writeTables = NodeUtils.getModifyTablesFromSqlNode(sqlNode);
        this.writeTables = new HashSet<>(writeTables);
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Enumerable rawData = bindable.bind(new SqlRecDataContextImpl(parameters, schema, context));

        List<Object[]> objArrayList = new ArrayList<>();
        for (Object obj : rawData) {
            if (obj instanceof Object[]) {
                objArrayList.add((Object[]) obj);
            } else {
                objArrayList.add(new Object[]{obj});
            }
        }
        return Linq4j.asEnumerable(objArrayList);
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return bestExp.getRowType().getFieldList();
    }

    @Override
    public boolean isParallelizable() {
        return true;
    }

    @Override
    public Set<String> getReadTables() {
        return readTables;
    }

    @Override
    public Set<String> getWriteTables() {
        return writeTables;
    }

    public RelNode getBestExp() {
        return bestExp;
    }

    public boolean isUnionSql() {
        return SqlTypeChecker.isUnionSql(sqlNode);
    }

    public String getLogicalPlan() {
        return logicalPlan;
    }

    public String getPhysicalPlan() {
        return physicalPlan;
    }

    public String getJavaExpression() {
        return javaExpression;
    }
}
