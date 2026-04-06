package com.sqlrec;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestWindowDiversifyFunction {
    @Test
    public void testTableFunction() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });
        HmsSchema.setGlobalSchema(schema);

        List<String> sqlList = Arrays.asList(
                "cache table t1 as select * from myTable",
                "cache table t2 as call window_diversify(t1, 'varchar_type', '2', '1', '10')",
                "select * from t2",
                "cache table t3 as call window_diversify(t1, 'array_varchar_type', '2', '1', '10')",
                "select * from t3",
                "cache table t4 as call window_diversify(t1, 'array_varchar_type', '3', '1', '10')",
                "select * from t4",
                "select id, UPPER(varchar_type) from t4",
                "select id, UPPER(array_varchar_type[1]) from t4"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = new CompileManager().compileSql(flinkSqlNode, schema, Consts.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema, executeContext);
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
                if (results.size() == 3) {
                    assert results.get(0)[0].equals(1);
                    assert results.get(1)[0].equals(3);
                    assert results.get(2)[0].equals(2);
                }
            } else {
                System.out.println("sql return null");
            }
        }
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {

        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, "a", Arrays.asList("a", "b")},
                    {2, "a", Arrays.asList("a", "e")},
                    {3, "b", Arrays.asList("d", "c")},
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", SqlTypeName.INTEGER)
                    .add("varchar_type", SqlTypeName.VARCHAR)
                    .add("array_varchar_type", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
                    .build();
        }
    }
}
