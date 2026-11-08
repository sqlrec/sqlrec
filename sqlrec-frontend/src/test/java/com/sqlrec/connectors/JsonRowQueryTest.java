package com.sqlrec.connectors;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.connectors.filesystem.calcite.FileSystemCalciteTable;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonRowQueryTest {
    private final List<FieldSchema> fields = List.of(new FieldSchema("id", "BIGINT"),
            new FieldSchema("payload", "ROW<n BIGINT, detail ROW<score FLOAT>>"),
            new FieldSchema("entries", "MAP<VARCHAR, ROW<n BIGINT>>"));
    private final String json = "{\"id\":1,\"payload\":{\"n\":7,\"detail\":{\"score\":0.25}},"
            + "\"entries\":{\"first\":{\"n\":9}}}";

    @Test
    void convertedRestInputAndCachedQueryResultsReadRowFields() throws Exception {
        Map<String, Object> payload = Map.of("n", 7, "detail", Map.of("score", 0.25f));
        Map<String, Object> input = Map.of("id", 1, "payload", payload,
                "entries", Map.of("first", Map.of("n", 9)));
        var type = DataTypeUtils.getRelDataType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT), fields);
        CacheTable table = new CacheTable("items",
                RowTransformUtils.convertDataToEnumerable(List.of(input), type.getFieldList()), type.getFieldList());
        assertArrayEquals(new Object[]{7L, 0.25d}, query(table,
                "select t.payload.n, t.payload.detail.score from items t").get(0));
        CacheTable cached = new CacheTable("items", Linq4j.asEnumerable(query(table, "select * from items")),
                type.getFieldList());
        assertArrayEquals(new Object[]{7L, 0.25d}, query(cached,
                "select t.payload.n, t.payload.detail.score from items t").get(0));
        assertArrayEquals(new Object[]{9L}, query(cached,
                "select q.e.n from (select entries['first'] as e from items) q").get(0));
        assertEquals(Map.of("n", 7, "detail", Map.of("score", 0.25f)), payload);
    }

    @Test
    void filesystemScanAndPrimaryKeyLookupReadRowFields(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("rows.json");
        Files.writeString(file, "[" + json + ",{\"id\":2,\"payload\":null,\"entries\":null}]");
        FileSystemConfig config = new FileSystemConfig();
        config.path = file.toString();
        config.format = "json";
        config.fieldSchemas = fields;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        var table = new FileSystemCalciteTable(config);
        var rows = query(table, "select t.id, t.payload.n, t.payload.detail.score from items t order by id");
        assertEquals(2, rows.size());
        assertArrayEquals(new Object[]{1L, 7L, 0.25d}, rows.get(0));
        assertArrayEquals(new Object[]{2L, null, null}, rows.get(1));
        assertArrayEquals(new Object[]{7L, 0.25d}, query(table,
                "select t.payload.n, t.payload.detail.score from items t where id = 1").get(0));
        assertArrayEquals(new Object[]{9L}, query(table,
                "select q.e.n from (select entries['first'] as e from items where id = 1) q").get(0));
    }

    private static List<Object[]> query(Table table, String sql) throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("default", new AbstractSchema()).add("items", table);
        return NormalSqlCompiler.getNormalSqlBindable(sql, schema, "default")
                .bind(schema, new ExecuteContextImpl(schema)).toList();
    }
}
