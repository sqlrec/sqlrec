package com.sqlrec;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
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

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t1 as select * from myTable",
                        Arrays.<Object[]>asList(
                                new Object[]{"t1", 3L}
                        )
                ),
                new SqlTestCase(
                        "cache table t2 as call window_diversify(t1, 'varchar_type', '2', '1', '10')",
                        Arrays.<Object[]>asList(
                                new Object[]{"t2", 3L}
                        )
                ),
                new SqlTestCase(
                        "select * from t2",
                        Arrays.asList(
                                new Object[]{1, "a", Arrays.asList("a", "b")},
                                new Object[]{3, "b", Arrays.asList("d", "c")},
                                new Object[]{2, "a", Arrays.asList("a", "e")}
                        )
                ),
                new SqlTestCase(
                        "cache table t3 as call window_diversify(t1, 'array_varchar_type', '2', '1', '10')",
                        Arrays.<Object[]>asList(
                                new Object[]{"t3", 3L}
                        )
                ),
                new SqlTestCase(
                        "select * from t3",
                        Arrays.asList(
                                new Object[]{1, "a", Arrays.asList("a", "b")},
                                new Object[]{3, "b", Arrays.asList("d", "c")},
                                new Object[]{2, "a", Arrays.asList("a", "e")}
                        )
                ),
                new SqlTestCase(
                        "cache table t4 as call window_diversify(t1, 'array_varchar_type', '3', '1', '10')",
                        Arrays.<Object[]>asList(
                                new Object[]{"t4", 3L}
                        )
                ),
                new SqlTestCase(
                        "select * from t4",
                        Arrays.asList(
                                new Object[]{1, "a", Arrays.asList("a", "b")},
                                new Object[]{3, "b", Arrays.asList("d", "c")},
                                new Object[]{2, "a", Arrays.asList("a", "e")}
                        )
                ),
                new SqlTestCase(
                        "select id, UPPER(varchar_type) from t4",
                        Arrays.asList(
                                new Object[]{1, "A"},
                                new Object[]{3, "B"},
                                new Object[]{2, "A"}
                        )
                ),
                new SqlTestCase(
                        "select id, UPPER(array_varchar_type[1]) from t4",
                        Arrays.asList(
                                new Object[]{1, "A"},
                                new Object[]{3, "D"},
                                new Object[]{2, "A"}
                        )
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
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
