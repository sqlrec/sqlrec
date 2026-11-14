package com.sqlrec.connectors.jdbc.utils;

import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.sqlrec.connectors.jdbc.handler.JdbcHandler;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JdbcDataSourceUtilsTest {
    @Test
    void sharesMatchingPoolsAndRecreatesThemAfterTheExistingCloseEntryPoint() throws Exception {
        JdbcConfig first = connectionConfig();
        first.connectionPoolSize = 1;
        first.connectionPoolMinIdle = 0;
        JdbcConfig same = connectionConfig();
        same.connectionPoolSize = 1;
        same.connectionPoolMinIdle = 0;
        JdbcConfig different = connectionConfig();
        different.connectionPoolSize = 2;
        different.connectionPoolMinIdle = 0;
        try {
            HikariDataSource shared = JdbcDataSourceUtils.getOrCreate(first);
            assertSame(shared, JdbcDataSourceUtils.getOrCreate(same));
            HikariDataSource separate = JdbcDataSourceUtils.getOrCreate(different);
            assertNotSame(shared, separate);
            assertEquals(1, shared.getMaximumPoolSize());
            assertEquals(2, separate.getMaximumPoolSize());

            JdbcHandler.closeAllDataSources();
            assertTrue(shared.isClosed());
            assertTrue(separate.isClosed());
            HikariDataSource replacement = JdbcDataSourceUtils.getOrCreate(first);
            assertNotSame(shared, replacement);
            try (var connection = replacement.getConnection()) {
                assertTrue(connection.isValid(1));
            }
        } finally {
            JdbcHandler.closeAllDataSources();
        }
    }

    @Test
    void credentialsContainingDelimitersDoNotSharePools() {
        JdbcConfig a = connectionConfig();
        JdbcConfig b = connectionConfig();
        a.username = "alice, admin";
        a.password = "secret";
        b.username = "alice";
        b.password = "admin, secret";
        assertNotEquals(JdbcDataSourceUtils.dataSourceKey(a), JdbcDataSourceUtils.dataSourceKey(b));
        a.username = null;
        b.username = "null";
        a.password = b.password;
        assertNotEquals(JdbcDataSourceUtils.dataSourceKey(a), JdbcDataSourceUtils.dataSourceKey(b));
    }

    @Test
    void propertyValuesCannotImpersonateAdditionalProperties() {
        JdbcConfig a = connectionConfig();
        JdbcConfig b = connectionConfig();
        a.jdbcProperties = Map.of("a", "b, c=d");
        b.jdbcProperties = Map.of("a", "b", "c", "d");
        assertNotEquals(JdbcDataSourceUtils.dataSourceKey(a), JdbcDataSourceUtils.dataSourceKey(b));
    }

    @Test
    void keysSnapshotPropertiesAndIgnoreTheirIterationOrder() {
        JdbcConfig a = connectionConfig();
        JdbcConfig b = connectionConfig();
        a.jdbcProperties = new HashMap<>(Map.of("a", "1", "b", "2"));
        b.jdbcProperties = new LinkedHashMap<>();
        b.jdbcProperties.put("b", "2");
        b.jdbcProperties.put("a", "1");
        List<Object> original = JdbcDataSourceUtils.dataSourceKey(a);
        assertThrows(UnsupportedOperationException.class, () -> original.set(0, "changed-url"));
        int hash = original.hashCode();
        assertEquals(original, JdbcDataSourceUtils.dataSourceKey(b));
        a.jdbcProperties.put("a", "changed");
        assertEquals(hash, original.hashCode());
        assertEquals(original, JdbcDataSourceUtils.dataSourceKey(b));
        assertNotEquals(original, JdbcDataSourceUtils.dataSourceKey(a));
    }

    @Test
    void differentConnectionAndPoolSettingsUseDifferentKeys() throws Exception {
        for (String field : new String[]{"url", "username", "password", "driver", "schema",
                "connectionPoolSize", "connectionPoolMinIdle", "connectionPoolIdleTimeout",
                "connectionPoolMaxLifetime", "connectionPoolConnectionTimeout", "connectionPoolValidationTimeout",
                "connectionPoolKeepaliveTime", "connectionPoolName"}) {
            JdbcConfig a = connectionConfig();
            JdbcConfig b = connectionConfig();
            java.lang.reflect.Field property = JdbcConfig.class.getField(field);
            Object value;
            if (property.getType() == Integer.class) value = Integer.valueOf(2);
            else if (property.getType() == Long.class) value = Long.valueOf(1234);
            else value = "different";
            property.set(b, value);
            assertNotEquals(JdbcDataSourceUtils.dataSourceKey(a), JdbcDataSourceUtils.dataSourceKey(b), field);
        }
    }

    private JdbcConfig connectionConfig() {
        JdbcConfig config = new JdbcConfig();
        config.url = "jdbc:h2:mem:pool-key";
        config.username = "alice";
        config.password = "secret";
        return config;
    }
}
