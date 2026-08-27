package com.sqlrec.connectors.filesystem.calcite;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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

    @Test
    void testScanByNonPrimaryKeyEqualityFilter() {
        table.getModifiableCollection().add(new Object[]{1, "alice", 20});
        table.getModifiableCollection().add(new Object[]{2, "bob", 25});

        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType rowType = table.getRowType(typeFactory);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RexNode filter = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(rowType.getFieldList().get(1).getType(), 1),
                rexBuilder.makeLiteral("alice")
        );

        List<Object[]> rows = table.scanImpl(List.of(filter)).toList();
        assertEquals(1, rows.size());
        assertEquals("alice", rows.get(0)[1]);
    }
}
