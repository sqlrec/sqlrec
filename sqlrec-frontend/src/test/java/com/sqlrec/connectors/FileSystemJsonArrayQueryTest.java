package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.connectors.filesystem.calcite.FileSystemCalciteTable;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FileSystemJsonArrayQueryTest {
    @Test
    void readsTypedJsonArraysThroughSqlScanAndPrimaryKeyLookup(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("arrays.json");
        Files.writeString(file, """
                [
                  {"id":1.0,"xs":[1.0,"2",null],"scores":[0.25,null],"matrix":[[0.5,null],null,[]],
                   "flags":[true,null],"tags":["red",7,{"x":1}]},
                  {"id":2e0,"xs":[3e0],"scores":[0.875],"matrix":[[0.75]],"flags":[false],"tags":[]},
                  {"id":3,"xs":[],"scores":[],"matrix":[],"flags":null,"tags":null},
                  {"id":4,"xs":null}
                ]
                """);
        FileSystemConfig config = new FileSystemConfig();
        config.path = file.toString();
        config.format = "json";
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = List.of(new FieldSchema("id", "BIGINT NOT NULL"),
                new FieldSchema("xs", "ARRAY<INTEGER>"), new FieldSchema("scores", "ARRAY<FLOAT>"),
                new FieldSchema("matrix", "ARRAY<ARRAY<DOUBLE>>"), new FieldSchema("flags", "ARRAY<BOOLEAN>"),
                new FieldSchema("tags", "ARRAY<VARCHAR>"));
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema()).add("items", new FileSystemCalciteTable(config));

        List<Object[]> sorted = query(schema,
                "select id, xs[1], cardinality(xs), scores[1], matrix[1][1], flags[1], tags[1] "
                        + "from items order by scores[1] desc nulls last, id");
        assertEquals(4, sorted.size());
        assertArrayEquals(new Object[]{2L, 3, 1, 0.875d, 0.75d, false, null}, sorted.get(0));
        assertArrayEquals(new Object[]{1L, 1, 3, 0.25d, 0.5d, true, "red"}, sorted.get(1));
        assertArrayEquals(new Object[]{3L, null, 0, null, null, null, null}, sorted.get(2));
        assertArrayEquals(new Object[]{4L, null, null, null, null, null, null}, sorted.get(3));

        List<Object[]> lookup = query(schema,
                "select xs[2], xs[3], scores[2], matrix[1][2], matrix[2], cardinality(matrix[3]), tags[3] "
                        + "from items where id = 1");
        assertEquals(1, lookup.size());
        assertArrayEquals(new Object[]{2, null, null, null, null, 0, "{\"x\":1}"}, lookup.get(0));
    }

    private static List<Object[]> query(CalciteSchema schema, String sql) throws Exception {
        var bindable = NormalSqlCompiler.getNormalSqlBindable(sql, schema, Consts.DEFAULT_SCHEMA_NAME);
        return bindable.bind(schema, new ExecuteContextImpl(schema)).toList();
    }
}
