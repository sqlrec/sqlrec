package com.sqlrec.common.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An immutable, parameterized SQL execution unit: the SQL text (built only from
 * dialect-quoted identifiers and {@code '?'} placeholders) plus the parameter values
 * bound to those placeholders, in order.
 *
 * <p>Instances can only be created inside this package (via {@link SqlUtils} and
 * {@link FilterUtils}). Callers outside the package can never construct a statement
 * whose SQL text contains an inlined value: literal values always travel through
 * {@link #getParameters()} and reach the database exclusively through
 * {@link PreparedStatement} parameter binding. SQL injection is therefore prevented
 * structurally, instead of depending on correct escaping at every call site.
 *
 * <p>A statement may also serve as a row template (e.g. upsert/delete): keep one
 * instance and call {@link #withParameters(List)} per row.
 */
public final class SqlStatement {

    private final String sql;
    private final List<Object> parameters;

    SqlStatement(String sql, List<?> parameters) {
        this.sql = Objects.requireNonNull(sql, "sql");
        this.parameters = Collections.unmodifiableList(
                new ArrayList<>(Objects.requireNonNull(parameters, "parameters")));
    }

    /** SQL text containing only quoted identifiers and {@code '?'} placeholders; never inlined values. */
    public String getSql() {
        return sql;
    }

    /** Values to bind to the {@code '?'} placeholders, in placeholder order (unmodifiable). */
    public List<Object> getParameters() {
        return parameters;
    }

    public int getParameterCount() {
        return parameters.size();
    }

    /**
     * The same SQL text with a different parameter set, e.g. re-binding a row-template
     * statement (upsert/delete) for each row. This instance is left unchanged.
     */
    public SqlStatement withParameters(List<?> parameters) {
        return new SqlStatement(sql, parameters);
    }

    /** Bind all parameters to the statement, in placeholder order. */
    public void bindTo(PreparedStatement stmt) throws SQLException {
        for (int i = 0; i < parameters.size(); i++) {
            Object param = parameters.get(i);
            if (param == null) {
                stmt.setNull(i + 1, Types.NULL);
            } else {
                stmt.setObject(i + 1, param);
            }
        }
    }

    /** Bind all parameters and add the statement to the batch. */
    public void addToBatch(PreparedStatement stmt) throws SQLException {
        bindTo(stmt);
        stmt.addBatch();
    }

    /**
     * Deliberately does not include parameter values: they are potentially sensitive
     * (user-supplied filter values, keys) and toString may end up in logs or error
     * messages.
     */
    @Override
    public String toString() {
        return "SqlStatement{sql='" + sql + "', parameterCount=" + parameters.size() + "}";
    }
}
