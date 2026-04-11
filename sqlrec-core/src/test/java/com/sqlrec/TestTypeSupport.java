package com.sqlrec;

import com.sqlrec.common.config.Consts;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.utils.SqlTestCase;
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

public class TestTypeSupport {
    @Test
    public void testTypeSupport() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });

        HmsSchema.setGlobalSchema(schema);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "select * from myTable",
                        Arrays.asList(
                                new Object[]{1, 1L, 1.0d, 1.0d, "1", true, "abc", Arrays.asList(1, 2, 3), Arrays.asList("a", "b", "c"), Arrays.asList(1.0d, 2.0d, 3.0d), Arrays.asList(1.0d, 2.0d, 3.0d)},
                                new Object[]{2, 2L, 2.0d, 2.0d, "2", false, "bcd", Arrays.asList(4, 5, 6), Arrays.asList("d", "e", "f"), Arrays.asList(4.0d, 5.0d, 6.0d), Arrays.asList(4.0d, 5.0d, 6.0d)},
                                new Object[]{3, 3L, 3.0d, 3.0d, "3", true, "cde", Arrays.asList(7, 8, 9), Arrays.asList("g", "h", "i"), Arrays.asList(7.0d, 8.0d, 9.0d), Arrays.asList(7.0d, 8.0d, 9.0d)}
                        )
                ),
                new SqlTestCase(
                        "select int_type + 1 from myTable",
                        Arrays.asList(
                                new Object[]{2},
                                new Object[]{3},
                                new Object[]{4}
                        )
                ),
                new SqlTestCase(
                        "select bigint_type + 1 from myTable",
                        Arrays.asList(
                                new Object[]{2L},
                                new Object[]{3L},
                                new Object[]{4L}
                        )
                ),
                new SqlTestCase(
                        "select double_type + 1 from myTable",
                        Arrays.asList(
                                new Object[]{2.0d},
                                new Object[]{3.0d},
                                new Object[]{4.0d}
                        )
                ),
                new SqlTestCase(
                        "select float_type + 1 from myTable",
                        Arrays.asList(
                                new Object[]{2.0d},
                                new Object[]{3.0d},
                                new Object[]{4.0d}
                        )
                ),
                new SqlTestCase(
                        "select UPPER(varchar_type) from myTable",
                        Arrays.asList(
                                new Object[]{"ABC"},
                                new Object[]{"BCD"},
                                new Object[]{"CDE"}
                        )
                ),
                new SqlTestCase(
                        "select boolean_type from myTable",
                        Arrays.asList(
                                new Object[]{true},
                                new Object[]{false},
                                new Object[]{true}
                        )
                ),
                new SqlTestCase(
                        "select array_int_type from myTable",
                        Arrays.asList(
                                new Object[]{Arrays.asList(1, 2, 3)},
                                new Object[]{Arrays.asList(4, 5, 6)},
                                new Object[]{Arrays.asList(7, 8, 9)}
                        )
                ),
                new SqlTestCase(
                        "select array_varchar_type from myTable",
                        Arrays.asList(
                                new Object[]{Arrays.asList("a", "b", "c")},
                                new Object[]{Arrays.asList("d", "e", "f")},
                                new Object[]{Arrays.asList("g", "h", "i")}
                        )
                ),
                new SqlTestCase(
                        "select array_float_type from myTable",
                        Arrays.asList(
                                new Object[]{Arrays.asList(1.0d, 2.0d, 3.0d)},
                                new Object[]{Arrays.asList(4.0d, 5.0d, 6.0d)},
                                new Object[]{Arrays.asList(7.0d, 8.0d, 9.0d)}
                        )
                ),
                new SqlTestCase(
                        "select array_double_type from myTable",
                        Arrays.asList(
                                new Object[]{Arrays.asList(1.0d, 2.0d, 3.0d)},
                                new Object[]{Arrays.asList(4.0d, 5.0d, 6.0d)},
                                new Object[]{Arrays.asList(7.0d, 8.0d, 9.0d)}
                        )
                ),
                new SqlTestCase(
                        "select array_int_type[1] from myTable",
                        Arrays.asList(
                                new Object[]{1},
                                new Object[]{4},
                                new Object[]{7}
                        )
                ),
                new SqlTestCase(
                        "select array_varchar_type[1] from myTable",
                        Arrays.asList(
                                new Object[]{"a"},
                                new Object[]{"d"},
                                new Object[]{"g"}
                        )
                ),
                new SqlTestCase(
                        "select array_float_type[1] from myTable",
                        Arrays.asList(
                                new Object[]{1.0d},
                                new Object[]{4.0d},
                                new Object[]{7.0d}
                        )
                ),
                new SqlTestCase(
                        "select array_double_type[1] from myTable",
                        Arrays.asList(
                                new Object[]{1.0d},
                                new Object[]{4.0d},
                                new Object[]{7.0d}
                        )
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, new ExecuteContextImpl());
        }
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {

        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, 1L, 1.0d, 1.0d, "1", true, "abc", Arrays.asList(1, 2, 3), Arrays.asList("a", "b", "c"), Arrays.asList(1.0d, 2.0d, 3.0d), Arrays.asList(1.0d, 2.0d, 3.0d)},
                    {2, 2L, 2.0d, 2.0d, "2", false, "bcd", Arrays.asList(4, 5, 6), Arrays.asList("d", "e", "f"), Arrays.asList(4.0d, 5.0d, 6.0d), Arrays.asList(4.0d, 5.0d, 6.0d)},
                    {3, 3L, 3.0d, 3.0d, "3", true, "cde", Arrays.asList(7, 8, 9), Arrays.asList("g", "h", "i"), Arrays.asList(7.0d, 8.0d, 9.0d), Arrays.asList(7.0d, 8.0d, 9.0d)},
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
    }
}
