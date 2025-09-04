package com.sqlrec.frontend.service;

import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.common.schema.SqlRecTable;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestTypeSupport {
    @Test
    public void testTypeSupport() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });
        HmsSchema.setGlobalSchema(schema);

        List<String> sqlList = Arrays.asList(
                "select * from myTable",
                "select int_type + 1 from myTable",
                "select bigint_type + 1 from myTable",
                "select double_type + 1 from myTable",
                "select float_type + 1 from myTable",
                "select varchar_type + '1' from myTable",
                "select boolean_type from myTable",
                "select array_int_type from myTable",
                "select array_varchar_type from myTable",
                "select array_float_type from myTable",
                "select array_double_type from myTable",
                "select array_int_type[1] from myTable",
                "select array_varchar_type[1] from myTable",
                "select array_float_type[1] from myTable",
                "select array_double_type[1] from myTable"
        );

        SqlProcessor processor = new SqlProcessor();
        for (String sql : sqlList) {
            System.out.println("\n\n" + sql);
            SqlProcessResult rowSet = processor.tryExecuteSql(sql);
            System.out.println(rowSet);
            assert rowSet != null;
            assert rowSet.exception == null;
            Utils.convertFieldsToTTableSchema(rowSet.fields);
            Utils.convertObjectArrayToTRowSet(rowSet.enumerable, rowSet.fields);
            if (rowSet.enumerable != null) {
                List<Object[]> results = rowSet.enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            }
        }
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {

        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, 1L, 1.0d, 1.0d, "1", true, "1", Arrays.asList(1, 2, 3), Arrays.asList("a", "b", "c"), Arrays.asList(1.0d, 2.0d, 3.0d), Arrays.asList(1.0d, 2.0d, 3.0d)},
                    {2, 2L, 2.0d, 2.0d, "2", false, "2", Arrays.asList(4, 5, 6), Arrays.asList("d", "e", "f"), Arrays.asList(4.0d, 5.0d, 6.0d), Arrays.asList(4.0d, 5.0d, 6.0d)},
                    {3, 3L, 3.0d, 3.0d, "3", true, "3", Arrays.asList(7, 8, 9), Arrays.asList("g", "h", "i"), Arrays.asList(7.0d, 8.0d, 9.0d), Arrays.asList(7.0d, 8.0d, 9.0d)},
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("int_type", SqlTypeName.INTEGER)
                    .add("bigint_type", SqlTypeName.BIGINT)
                    .add("double_type", SqlTypeName.DOUBLE)
                    .add("float_type", SqlTypeName.FLOAT)
                    .add("char_type", SqlTypeName.CHAR)
                    .add("boolean_type", SqlTypeName.BOOLEAN)
                    .add("varchar_type", SqlTypeName.VARCHAR)
                    .add("array_int_type", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1))
                    .add("array_varchar_type", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
                    .add("array_float_type", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.FLOAT), -1))
                    .add("array_double_type", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), -1))
                    .build();
        }

        @Override
        public SqlRecTableType getSqlRecTableType() {
            return SqlRecTableType.MEMORY;
        }
    }
}
