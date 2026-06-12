package com.sqlrec.connectors.mongodb.calcite;

import com.sqlrec.connectors.mongodb.config.MongoOptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MongoCalciteTableFactoryTest {

    @Test
    void testGetConnectorName() {
        MongoCalciteTableFactory factory = new MongoCalciteTableFactory();
        assertEquals(MongoOptions.CONNECTOR_IDENTIFIER, factory.getConnectorName());
        assertEquals("mongodb", factory.getConnectorName());
    }

    @Test
    void testGetRulesReturnsEmptyList() {
        MongoCalciteTableFactory factory = new MongoCalciteTableFactory();
        assertNotNull(factory.getRules());
        assertTrue(factory.getRules().isEmpty());
    }
}
