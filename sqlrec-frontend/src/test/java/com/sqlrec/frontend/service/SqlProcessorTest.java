package com.sqlrec.frontend.service;


import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.frontend.common.SqlProcessResult;
import com.sqlrec.frontend.common.SqlProcessor;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.utils.TableFunctionUtils;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SqlProcessorTest {
    @Test
    public void testSqlProcessor() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });

        HmsSchema.setGlobalSchema(schema);
        TableFunctionUtils.registerTableFunction("default", "fun1", Integer.TYPE);  // avoid find function in hms

        testSqlFunctionCompile(schema);

        List<String> sqlList = Arrays.asList(
                "create sql function fun2",
                "define input table input1(id int, name string)",
                "cache table t1 as SELECT NAME, count(*) as cnt FROM input1 where ID > 1 group by NAME",
                "return t1",
                "cache table t1 as SELECT * FROM myTable",
                "select * from t1",
                "cache table t2 as SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME",
                "select * from t2",
                "call fun1(t1)",
                "cache table t3 as call fun1(t1)",
                "select * from t3"
        );

        SqlProcessor processor = new SqlProcessor();
        for (String sql : sqlList) {
            System.out.println("\n\n" + sql);
            SqlProcessResult rowSet = processor.tryExecuteSql(sql);
            System.out.println(rowSet);
            assert rowSet != null;
            assert rowSet.exception == null;
            if (rowSet.enumerable != null) {
                List<Object[]> results = rowSet.enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            }
        }
    }

    public static void testSqlFunctionCompile(CalciteSchema schema) throws Exception {
        List<String> sqlList = Arrays.asList(
                "create sql function fun1",
                "define input table input1(id int, name string)",
                "cache table t1 as SELECT NAME, count(*) as cnt FROM input1 where ID > 1 group by NAME",
                "return t1"
        );
        CompileManager.compileSqlFunction("fun1", sqlList);
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {

        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, "Alice"},
                    {2, "Bob"},
                    {3, "Charlie"}
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", SqlTypeName.INTEGER)
                    .add("NAME", SqlTypeName.VARCHAR, 20)
                    .build();
        }
    }
}