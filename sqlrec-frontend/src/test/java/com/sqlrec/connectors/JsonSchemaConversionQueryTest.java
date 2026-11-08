package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.udf.table.JsonToTableFunction;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonSchemaConversionQueryTest {
    @Test
    void convertedAndInvalidInferredValuesCanBeUsedInSql() throws Exception {
        var table = new JsonToTableFunction().evaluate("""
                [{"n":1,"flag":false,"xs":[1]},
                 {"n":"2.5","flag":"true","xs":["3","bad"]},
                 {"n":"bad","flag":"bad","xs":[]}]
                """);
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema()).add("items", table);
        var query = NormalSqlCompiler.getNormalSqlBindable(
                "select n, n + 1, flag, xs[1], xs[2] from items order by n desc nulls last", schema, "default");
        var rows = query.bind(schema, new ExecuteContextImpl(schema)).toList();
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[]{2.5d, 3.5d, true, 3.0d, null}, rows.get(0));
        assertArrayEquals(new Object[]{1.0d, 2.0d, false, 1.0d, null}, rows.get(1));
        assertArrayEquals(new Object[]{null, null, false, null, null}, rows.get(2));
    }
}
