package com.sqlrec.connectors.jdbc.sql;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class JdbcStatementTest {

    private static JdbcStatement statement(String sql, Object... parameters) {
        return new JdbcStatement(sql, Arrays.asList(parameters));
    }

    @Test
    void testBindToBindsValuesInOrder() throws SQLException {
        PreparedStatement stmt = mock(PreparedStatement.class);
        JdbcStatement statement = statement("a = ? AND b = ? AND c = ?", "v1", BigDecimal.TEN, true);

        statement.bindTo(stmt);

        verify(stmt).setObject(1, "v1");
        verify(stmt).setObject(2, BigDecimal.TEN);
        verify(stmt).setObject(3, Boolean.TRUE);
        verify(stmt, never()).setNull(anyInt(), anyInt());
        verifyNoMoreInteractions(stmt);
    }

    @Test
    void testBindToBindsNullAsSetNull() throws SQLException {
        PreparedStatement stmt = mock(PreparedStatement.class);
        JdbcStatement statement = statement("a = ?", new Object[]{null});

        statement.bindTo(stmt);

        verify(stmt).setNull(1, Types.NULL);
        verifyNoMoreInteractions(stmt);
    }

    @Test
    void testBindToWithNoParametersIsNoOp() throws SQLException {
        PreparedStatement stmt = mock(PreparedStatement.class);
        JdbcStatement statement = statement("SELECT 1");

        statement.bindTo(stmt);

        verifyNoInteractions(stmt);
    }

    @Test
    void testAddToBatchBindsThenBatches() throws SQLException {
        PreparedStatement stmt = mock(PreparedStatement.class);
        JdbcStatement statement = statement("a = ?", "v1");

        statement.addToBatch(stmt);

        verify(stmt).setObject(1, "v1");
        verify(stmt).addBatch();
        verifyNoMoreInteractions(stmt);
    }

    @Test
    void testWithParametersCopiesAndIsolatesParameterList() {
        List<Object> params = new java.util.ArrayList<>(Collections.singletonList("v1"));
        JdbcStatement statement = statement("a = ?").withParameters(params);

        // mutating the source list afterwards must not affect the statement
        params.add("v2");
        assertEquals(Collections.singletonList("v1"), statement.getParameters());
    }

    @Test
    void testConstructorRejectsNullSql() {
        assertThrows(NullPointerException.class, () -> new JdbcStatement(null, Collections.emptyList()));
    }

    @Test
    void testConstructorRejectsNullParameters() {
        assertThrows(NullPointerException.class, () -> new JdbcStatement("SELECT 1", null));
    }

    @Test
    void testToStringDoesNotLeakParameterValues() {
        JdbcStatement statement = statement("name = ?", "secret-value'; DROP TABLE users; --");
        String text = statement.toString();

        assertTrue(text.contains("name = ?"));
        // parameter values (potentially sensitive) must not leak into logs/error messages
        assertFalse(text.contains("secret-value"));
        assertFalse(text.contains("DROP TABLE"));
    }
}

