package com.sqlrec;

import com.sqlrec.common.udf.table.ShuffleFunction;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.TableFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestTableFunction {
    @Test
    public void testTableFunction() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        HmsSchema.setGlobalSchema(schema);
        TableFunctionUtils.registerTableFunction("default", "shuffle", ShuffleFunction.class);

        List<String> sqlList = Arrays.asList(
                "cache table t1 as select * from myTable",
                "call shuffle(t1)",
                "cache table t2 as shuffle(t1)",
                "select * from t2"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema);
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            }
        }
    }
}
