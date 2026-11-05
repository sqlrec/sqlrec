package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.runtime.SqlRecDataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Clock;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.calcite.linq4j.Linq4j.DEFAULT_PROVIDER;

public class SqlRecDataContextImpl implements SqlRecDataContext {
    private final Map<String, Object> parameters;
    private final CalciteSchema schema;
    private final ExecuteContext executeContext;
    private final Map<String, Object> builtInVariables;

    public SqlRecDataContextImpl(Map<String, Object> parameters, CalciteSchema schema, ExecuteContext executeContext) {
        this(parameters, schema, executeContext, Clock.systemDefaultZone());
    }

    SqlRecDataContextImpl(Map<String, Object> parameters, CalciteSchema schema,
                         ExecuteContext executeContext, Clock clock) {
        this.parameters = parameters;
        this.schema = schema;
        this.executeContext = executeContext;

        // Calcite represents local wall time as epoch milliseconds plus the zone offset.
        // Capture it once so CURRENT_* and LOCAL* stay consistent throughout a statement.
        long utcTimestamp = clock.millis();
        TimeZone timeZone = TimeZone.getTimeZone(clock.getZone());
        long localTimestamp = utcTimestamp + timeZone.getOffset(utcTimestamp);
        builtInVariables = Map.of(
                Variable.UTC_TIMESTAMP.camelName, utcTimestamp,
                Variable.CURRENT_TIMESTAMP.camelName, localTimestamp,
                Variable.LOCAL_TIMESTAMP.camelName, localTimestamp,
                Variable.TIME_ZONE.camelName, timeZone,
                Variable.LOCALE.camelName, Locale.ROOT,
                // There is no per-statement SQL user; follow Calcite's default identity.
                Variable.USER.camelName, "sa",
                Variable.SYSTEM_USER.camelName, System.getProperty("user.name", ""));
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
        Object builtIn = builtInVariables.get(name);
        return builtIn != null ? builtIn : parameters.get(name);
    }

    @Override
    public String getVariable(String key) {
        return executeContext.getVariable(key);
    }

    @Override
    public Map<String, String> getVariables() {
        return executeContext.getVariables();
    }
}
