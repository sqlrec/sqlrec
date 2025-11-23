package com.sqlrec;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.Const;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestUdfSupport {
    @Test
    public void testUdfSupport() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Const.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        HmsSchema.setGlobalSchema(schema);

        SchemaUtils.addFunction(
                schema.getSubSchema(Const.DEFAULT_SCHEMA_NAME, false),
                "uuid",
                "com.sqlrec.common.udf.scalar.UuidFunction"
        );
        SchemaUtils.addFunction(
                schema.getSubSchema(Const.DEFAULT_SCHEMA_NAME, false),
                "l2_norm",
                "com.sqlrec.common.udf.scalar.L2NormFunction"
        );

        List<String> sqlList = Arrays.asList(
                "select uuid()",
                "select l2_norm(array_float_type) from myTable",
                "select l2_norm(array_double_type) from myTable",
                "select SIN(0.1)",
                "select count(1) from myTable",
                "select sum(int_type) from myTable",
                "select min(int_type) from myTable",
                "select max(int_type) from myTable",
                "select UPPER(varchar_type) from myTable",
                "select CHAR_LENGTH(varchar_type) from myTable",
                "select SUBSTRING(varchar_type from 1 for 2) from myTable",
                "select varchar_type || '1' from myTable",
                "select CARDINALITY(array_int_type) from myTable",
                "select CARDINALITY(array_varchar_type) from myTable",
                "select CARDINALITY(array_float_type) from myTable",
                "select CARDINALITY(array_double_type) from myTable"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = new CompileManager().compileSql(flinkSqlNode, schema, Const.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema, new ExecuteContextImpl());
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            }
        }
    }
}
