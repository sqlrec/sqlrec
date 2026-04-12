package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.runtime.SqlRecDataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

import static org.apache.calcite.linq4j.Linq4j.DEFAULT_PROVIDER;

public class SqlRecDataContextImpl implements SqlRecDataContext {
    private Map<String, Object> parameters;
    private CalciteSchema schema;
    private ExecuteContext executeContext;

    public SqlRecDataContextImpl(Map<String, Object> parameters, CalciteSchema schema, ExecuteContext executeContext) {
        this.parameters = parameters;
        this.schema = schema;
        this.executeContext = executeContext;
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

    @Override
    public String getVariable(String key) {
        return executeContext.getVariable(key);
    }
}
