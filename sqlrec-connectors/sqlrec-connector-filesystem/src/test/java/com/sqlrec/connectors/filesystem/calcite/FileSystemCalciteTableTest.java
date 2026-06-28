package com.sqlrec.connectors.filesystem.calcite;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FileSystemCalciteTableTest {

    private FileSystemConfig config;
    private FileSystemCalciteTable table;

    @BeforeEach
    void setUp() {
        config = new FileSystemConfig();
        config.path = null;
        config.format = "csv";
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("age", "INTEGER")
        );
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;

        table = new FileSystemCalciteTable(config);
    }

    @Test
    void testGetRowType() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType rowType = table.getRowType(typeFactory);

        assertEquals(3, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("name", rowType.getFieldNames().get(1));
        assertEquals("age", rowType.getFieldNames().get(2));
    }

    @Test
    void testGetPrimaryKeyIndex() {
        assertEquals(0, table.getPrimaryKeyIndex());
    }

    @Test
    void testGetModifiableCollection() {
        assertNotNull(table.getModifiableCollection());
        assertTrue(table.getModifiableCollection() instanceof FileSystemCalciteTable.FileSystemCollection);
    }

    @Test
    void testScanEmptyTable() {
        List<Object[]> rows = table.scanImpl(Arrays.asList()).toList();
        assertTrue(rows.isEmpty());
    }
}
