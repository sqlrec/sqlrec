package com.sqlrec.connectors.jdbc.calcite;

import com.sqlrec.connectors.jdbc.config.JdbcOptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class JdbcCalciteTableFactoryTest {

    @Test
    void testGetConnectorName() {
        JdbcCalciteTableFactory factory = new JdbcCalciteTableFactory();
        assertEquals(JdbcOptions.CONNECTOR_IDENTIFIER, factory.getConnectorName());
        assertEquals("jdbc", factory.getConnectorName());
    }

    @Test
    void testGetRulesReturnsEmptyList() {
        JdbcCalciteTableFactory factory = new JdbcCalciteTableFactory();
        assertNotNull(factory.getRules());
        assertTrue(factory.getRules().isEmpty());
    }
}
