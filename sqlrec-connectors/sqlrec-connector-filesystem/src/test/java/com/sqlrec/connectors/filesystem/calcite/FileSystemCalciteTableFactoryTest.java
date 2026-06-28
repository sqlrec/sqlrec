package com.sqlrec.connectors.filesystem.calcite;

import com.sqlrec.connectors.filesystem.config.FileSystemOptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FileSystemCalciteTableFactoryTest {

    @Test
    void testGetConnectorName() {
        FileSystemCalciteTableFactory factory = new FileSystemCalciteTableFactory();
        assertEquals(FileSystemOptions.CONNECTOR_IDENTIFIER, factory.getConnectorName());
        assertEquals("filesystem", factory.getConnectorName());
    }

    @Test
    void testGetRulesReturnsEmptyList() {
        FileSystemCalciteTableFactory factory = new FileSystemCalciteTableFactory();
        assertNotNull(factory.getRules());
        assertTrue(factory.getRules().isEmpty());
    }
}
