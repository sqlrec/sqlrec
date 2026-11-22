package com.sqlrec.utils;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.connectors.redis.codec.JsonCodec;
import com.sqlrec.frontend.utils.ThriftUtils;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaThriftUtilsTest {

    @Test
    void localSqlTemporalResultsUseHiveTypesAndCalendarText() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema());
        var query = NormalSqlCompiler.getNormalSqlBindable(
                "SELECT DATE '2026-11-22' AS d, TIMESTAMP '1969-12-31 23:59:59.123' AS ts,"
                        + " CAST(NULL AS TIMESTAMP) AS missing",
                schema, Consts.DEFAULT_SCHEMA_NAME);
        var fields = query.getReturnDataFields();
        var rows = query.bind(schema, new ExecuteContextImpl(schema));
        var columns = ThriftUtils.convertObjectArrayToTRowSet(rows, fields).getColumns();
        var metadata = ThriftUtils.convertFieldsToTTableSchema(fields).getColumns();

        assertEquals(TTypeId.DATE_TYPE, metadata.get(0).getTypeDesc().getTypes().getFirst().getPrimitiveEntry().getType());
        assertEquals(TTypeId.TIMESTAMP_TYPE, metadata.get(1).getTypeDesc().getTypes().getFirst().getPrimitiveEntry().getType());
        assertEquals(List.of("2026-11-22"), columns.get(0).getStringVal().getValues());
        assertEquals(List.of("1969-12-31 23:59:59.123"), columns.get(1).getStringVal().getValues());
        assertArrayEquals(new byte[]{1}, columns.get(2).getStringVal().getNulls());
    }

    @Test
    void jdbcTemporalValuesAndTextPreserveFractionalSeconds() {
        var fields = List.of(DataTypeUtils.getRelDataTypeField("d", 0, SqlTypeName.DATE),
                DataTypeUtils.getRelDataTypeField("ts", 1, SqlTypeName.TIMESTAMP));
        List<Object[]> rows = List.of(
                new Object[]{Date.valueOf("2026-11-22"), Timestamp.valueOf("2026-11-22 10:00:00.123456789")},
                new Object[]{"2026-11-23", "2026-11-23T10:00:00"});
        var columns = ThriftUtils.convertObjectArrayToTRowSet(Linq4j.asEnumerable(rows), fields).getColumns();

        assertEquals(List.of("2026-11-22", "2026-11-23"), columns.get(0).getStringVal().getValues());
        assertEquals(List.of("2026-11-22 10:00:00.123456789", "2026-11-23 10:00:00"),
                columns.get(1).getStringVal().getValues());
    }

    @Test
    void redisJsonRoundTripPreservesJdbcTemporalResults() {
        var codec = new JsonCodec();
        codec.init(List.of(new FieldSchema("id", "BIGINT"), new FieldSchema("d", "DATE"),
                new FieldSchema("ts", "TIMESTAMP")), 0);
        Date date = Date.valueOf("2024-01-02");
        Timestamp timestamp = Timestamp.valueOf("2024-01-02 03:04:05.123456789");
        Object[] input = {1L, date, timestamp};
        byte[] encoded = codec.encode(input);
        Object[] decoded = codec.decode(encoded, "1");

        assertArrayEquals(new Object[]{1L, date, timestamp}, input);
        assertArrayEquals(new Object[]{1L, "2024-01-02", "2024-01-02T03:04:05.123456789"}, decoded);
        assertArrayEquals(encoded, codec.encode(decoded));

        var fields = List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.BIGINT),
                DataTypeUtils.getRelDataTypeField("d", 1, SqlTypeName.DATE),
                DataTypeUtils.getRelDataTypeField("ts", 2, SqlTypeName.TIMESTAMP));
        var columns = ThriftUtils.convertObjectArrayToTRowSet(Linq4j.singletonEnumerable(decoded), fields).getColumns();
        var metadata = ThriftUtils.convertFieldsToTTableSchema(fields).getColumns();

        assertEquals(TTypeId.DATE_TYPE, metadata.get(1).getTypeDesc().getTypes().getFirst().getPrimitiveEntry().getType());
        assertEquals(TTypeId.TIMESTAMP_TYPE, metadata.get(2).getTypeDesc().getTypes().getFirst().getPrimitiveEntry().getType());
        assertEquals(List.of("2024-01-02"), columns.get(1).getStringVal().getValues());
        assertEquals(List.of("2024-01-02 03:04:05.123456789"), columns.get(2).getStringVal().getValues());
    }

    @Test
    public void testThriftValuesPreserveNullBitmapAcrossBytes() {
        List<Object[]> rows = List.of(
                new Object[]{null}, new Object[]{1L}, new Object[]{2}, new Object[]{3},
                new Object[]{4}, new Object[]{5}, new Object[]{6}, new Object[]{null},
                new Object[]{null}, new Object[]{9});

        var fields = List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        var column = ThriftUtils.convertObjectArrayToTRowSet(Linq4j.asEnumerable(rows), fields)
                .getColumns().getFirst().getI32Val();

        assertArrayEquals(new byte[]{(byte) 0x81, 0x01}, column.getNulls());
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 0, 0, 9), column.getValues());
        assertNull(rows.get(0)[0]);
        assertEquals(1L, rows.get(1)[0]);
    }

    @Test
    @SilenceLoggers(ThriftUtils.class)
    public void testThriftValuesForMissingInputAndFailedCast() {
        var fields = DataTypeUtils.getStringTypeField("name");
        var empty = ThriftUtils.convertObjectArrayToTRowSet(null, fields)
                .getColumns().getFirst().getStringVal();
        assertArrayEquals(new byte[0], empty.getNulls());
        assertTrue(empty.getValues().isEmpty());

        var column = ThriftUtils.convertObjectArrayToTRowSet(
                Linq4j.asEnumerable(List.of(new Object[]{"value"}, new Object[]{42})), fields)
                .getColumns().getFirst().getStringVal();
        assertArrayEquals(new byte[]{2}, column.getNulls());
        assertEquals(List.of("value", ""), column.getValues());
    }

}
