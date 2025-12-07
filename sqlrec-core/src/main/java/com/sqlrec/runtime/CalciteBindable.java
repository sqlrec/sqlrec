package com.sqlrec.runtime;

import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.compiler.SqlTypeChecker;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

import static org.apache.calcite.linq4j.Linq4j.DEFAULT_PROVIDER;

public class CalciteBindable extends BindableInterface {
    private Map<String, Object> parameters;
    private Bindable<Object[]> bindable;
    private RelNode bestExp;
    private SqlNode sqlNode;
    private Set<String> readTables;
    private Set<String> writeTables;

    public CalciteBindable(Map<String, Object> parameters, Bindable<Object[]> bindable, RelNode bestExp, SqlNode sqlNode) {
        this.parameters = parameters;
        this.bindable = bindable;
        this.bestExp = bestExp;
        this.sqlNode = sqlNode;

        List<String> readTables = SqlTypeChecker.getTableFromSqlNode(sqlNode);
        this.readTables = new HashSet<>(readTables);

        List<String> writeTables = SqlTypeChecker.getModifyTablesFromSqlNode(sqlNode);
        this.writeTables = new HashSet<>(writeTables);
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Enumerable rawData = bindable.bind(new DataContextImpl(parameters, schema));

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

    public static class DataContextImpl implements DataContext {
        private Map<String, Object> parameters;
        private CalciteSchema schema;

        public DataContextImpl(Map<String, Object> parameters, CalciteSchema schema) {
            this.parameters = parameters;
            this.schema = schema;
        }

        @Override
        public @Nullable SchemaPlus getRootSchema() {
            return schema.plus();
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
        }

        @Override
        public QueryProvider getQueryProvider() {
            return DEFAULT_PROVIDER;
        }

        @Override
        public @Nullable Object get(String name) {
            switch (name) {
                case "currentTimestamp":
                    return System.currentTimeMillis();
            }
            return parameters.get(name);
        }
    }
}
