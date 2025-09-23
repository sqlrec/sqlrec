package com.sqlrec;

import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
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
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestNormalSqlCompiler {
    @Test
    public void testNormalSqlCompiler() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });

        String sql = "SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME";

        BindableInterface bindable = NormalSqlCompiler.getNormalSqlBindable(sql, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

        Enumerable enumerable = bindable.bind(schema);
        List<Object[]> results = enumerable.toList();
        for (Object[] result : results) {
            System.out.println(java.util.Arrays.toString(result));
        }
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

        @Override
        public SqlRecTableType getSqlRecTableType() {
            return SqlRecTableType.MEMORY;
        }
    }
}
