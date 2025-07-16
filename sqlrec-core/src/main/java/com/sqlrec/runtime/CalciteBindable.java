package com.sqlrec.runtime;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.linq4j.Linq4j.DEFAULT_PROVIDER;

public class CalciteBindable implements BindableInterface {
    private Map<String, Object> parameters;
    private Bindable<Object[]> bindable;
    private RelNode bestExp;

    public CalciteBindable(Map<String, Object> parameters, Bindable<Object[]> bindable, RelNode bestExp) {
        this.parameters = parameters;
        this.bindable = bindable;
        this.bestExp = bestExp;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema) {
        return bindable.bind(new DataContext() {
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
                return null;
            }
        });
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return bestExp.getRowType().getFieldList();
    }
}
