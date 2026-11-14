package com.sqlrec.udf;

import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScalarFunctionOverloadTest {
    @Test
    void resolvesOverloadsByArgumentType() throws Exception {
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        CalciteSchema schema = root.add("default", new AbstractSchema());
        UdfManager.addFunction(schema, "typed", TypedFunction.class.getName());
        String sql = "SELECT typed('value'), typed(7)";
        var rows = NormalSqlCompiler.getNormalSqlBindable(sql, root, "default")
                .bind(root, new ExecuteContextImpl(root)).toList();
        assertArrayEquals(new Object[]{"string:value", "integer:7"}, rows.get(0));
    }

    @Test
    void rejectsUnsupportedArgumentCount() throws Exception {
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        CalciteSchema schema = root.add("default", new AbstractSchema());
        UdfManager.addFunction(schema, "get", "com.sqlrec.udf.scalar.GetFunction");
        for (String sql : new String[]{"SELECT `get`()", "SELECT `get`('a', 'b', 'c')"}) {
            assertThrows(CalciteContextException.class,
                    () -> NormalSqlCompiler.getNormalSqlBindable(sql, root, "default"));
        }
    }

    public static class TypedFunction {
        public String evaluate(String value) {
            return "string:" + value;
        }

        public String evaluate(Integer value) {
            return "integer:" + value;
        }
    }
}
