package com.sqlrec.connectors.mongodb.calcite;

import com.mongodb.MongoException;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import com.sqlrec.connectors.mongodb.handler.MongoHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    void testGetModifiableCollection() {
        MongoCalciteTable table = new MongoCalciteTable(mongoConfig);
        MongoCalciteTable.MongoCollection collection = (MongoCalciteTable.MongoCollection) table.getModifiableCollection();
        assertNotNull(collection);
    }

    @Test
    void failedUpdateInvalidatesBothKeysOfEverySelectedRow() {
        var table = mock(MongoCalciteTable.class);
        when(table.getTableName()).thenReturn("users");
        var handler = mock(MongoHandler.class);
        var collection = new MongoCalciteTable.MongoCollection(table, handler);
        List<Object[]> oldRows = List.of(new Object[]{1, "Alice", 30}, new Object[]{2, "Bob", 25});
        List<Object[]> newRows = List.of(new Object[]{11, "Alice", 30}, new Object[]{12, "Bob", 25});
        var failure = new MongoException("ordered bulk update failed");
        when(handler.updateBatch(oldRows, newRows)).thenThrow(failure);

        assertSame(failure, assertThrows(MongoException.class, () -> collection.replaceAll(oldRows, newRows)));
        assertEquals(0, collection.size());
        oldRows.forEach(row -> verify(table).invalidateCache(row));
        newRows.forEach(row -> verify(table).invalidateCache(row));
    }
}
