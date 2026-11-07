package com.sqlrec.runtime;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.compiler.NormalSqlCompiler;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ArrayTypeConversionTest {
    @Test
    void javaFunctionArraysSupportSubscriptsAndSorting() throws Exception {
        CalciteSchema schema = schema();
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        JavaFunctionBindable function = new JavaFunctionBindable("arrays", new ArrayFunction(),
                List.of(), fields(), schema);
        CacheTable table = new CacheTable("arrays", function.bind(schema, context), fields());
        schema.add("arrays", table);
        checkSql(schema, context);
    }

    @Test
    void connectorArraysSupportSubscriptsAndSorting() throws Exception {
        CalciteSchema schema = schema();
        List<Object[]> rows = inputRows();
        RowTransformUtils.convertRowTypes(rows, fields());
        schema.add("arrays", new CacheTable("arrays", Linq4j.asEnumerable(rows), fields()));
        checkSql(schema, new ExecuteContextImpl(schema));
    }

    private static CalciteSchema schema() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("default", new AbstractSchema());
        return schema;
    }

    private static void checkSql(CalciteSchema schema, ExecuteContextImpl context) throws Exception {
        BindableInterface query = NormalSqlCompiler.getNormalSqlBindable(
                "select id, ints[1], longs[1], scores[1], reals[1], nested[1][1] "
                        + "from arrays order by scores[1] desc nulls last", schema, "default");
        List<Object[]> rows = query.bind(schema, context).toList();
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[]{2L, 2, 2L, 0.875d, 0.875f, 0.875d}, rows.get(0));
        assertArrayEquals(new Object[]{1L, 1, 1L, 0.25d, 0.25f, 0.25d}, rows.get(1));
        assertArrayEquals(new Object[]{3L, null, null, null, null, null}, rows.get(2));
    }

    private static List<RelDataTypeField> fields() {
        return List.of(DataTypeUtils.getRelDataTypeField("id", 0, "BIGINT"),
                DataTypeUtils.getRelDataTypeField("ints", 1, "ARRAY<INTEGER>"),
                DataTypeUtils.getRelDataTypeField("longs", 2, "ARRAY<BIGINT>"),
                DataTypeUtils.getRelDataTypeField("scores", 3, "ARRAY<FLOAT>"),
                DataTypeUtils.getRelDataTypeField("reals", 4, "ARRAY<REAL>"),
                DataTypeUtils.getRelDataTypeField("nested", 5, "ARRAY<ARRAY<FLOAT>>"));
    }

    private static List<Object[]> inputRows() {
        return Arrays.asList(
                new Object[]{1L, List.of(1L), List.of(1), List.of(0.25f), List.of(0.25d), List.of(List.of(0.25f))},
                new Object[]{2L, List.of(2L), List.of(2), List.of(0.875f), List.of(0.875d), List.of(List.of(0.875f))},
                new Object[]{3L, Arrays.asList((Object) null), null, Arrays.asList((Object) null), null, null});
    }

    public static class ArrayFunction {
        public CacheTable evaluate() {
            return new CacheTable("output", Linq4j.asEnumerable(inputRows()), fields());
        }
    }
}
