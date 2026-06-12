package com.sqlrec.connectors.mongodb.calcite;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class MongoCalciteTableTest {

    private MongoConfig mongoConfig;

    @BeforeEach
    void setUp() {
        mongoConfig = new MongoConfig();
        mongoConfig.uri = "mongodb://localhost:27017";
        mongoConfig.database = "testdb";
        mongoConfig.collection = "users";
        mongoConfig.primaryKey = "id";
        mongoConfig.primaryKeyIndex = 0;
        mongoConfig.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("age", "INTEGER")
        );
    }

    @Test
    void testGetRowType() {
        MongoCalciteTable table = new MongoCalciteTable(mongoConfig);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType rowType = table.getRowType(typeFactory);

        assertEquals(3, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("name", rowType.getFieldNames().get(1));
        assertEquals("age", rowType.getFieldNames().get(2));
        assertEquals(SqlTypeName.INTEGER, rowType.getFieldList().get(0).getType().getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(1).getType().getSqlTypeName());
        assertEquals(SqlTypeName.INTEGER, rowType.getFieldList().get(2).getType().getSqlTypeName());
    }

    @Test
    void testGetPrimaryKeyIndex() {
        MongoCalciteTable table = new MongoCalciteTable(mongoConfig);
        assertEquals(0, table.getPrimaryKeyIndex());
    }

    @Test
    void testOnlyFilterByPrimaryKey() {
        MongoCalciteTable table = new MongoCalciteTable(mongoConfig);
        assertFalse(table.onlyFilterByPrimaryKey());
    }

    @Test
    void testGetElementType() {
        MongoCalciteTable table = new MongoCalciteTable(mongoConfig);
        assertEquals(Object[].class, table.getElementType());
    }

    @Test
    void testAsQueryableThrows() {
        MongoCalciteTable table = new MongoCalciteTable(mongoConfig);
        assertThrows(UnsupportedOperationException.class, () ->
                table.asQueryable(null, null, "test"));
    }

    @Test
    void testGetModifiableCollection() {
        MongoCalciteTable table = new MongoCalciteTable(mongoConfig);
        MongoCalciteTable.MongoCollection collection = (MongoCalciteTable.MongoCollection) table.getModifiableCollection();
        assertNotNull(collection);
    }
}
