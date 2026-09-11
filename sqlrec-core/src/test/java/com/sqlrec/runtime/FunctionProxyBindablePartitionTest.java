package com.sqlrec.runtime;

import com.sqlrec.utils.TypeSupportTest;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.*;

public class FunctionProxyBindablePartitionTest {

    @Test
    public void testPartitionBy() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TypeSupportTest.MyTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "partition_echo_fun", TestPartitionEchoFun.class);
        JavaFunctionUtils.registerTableFunction(
                "default", "partition_sometimes_fail_fun", PartitionSometimesFailFun.class);
        new CompileManager().compileSqlFunction(
                "partition_sql_sometimes_fail",
                Arrays.asList(
                        "create sql function partition_sql_sometimes_fail",
                        "define input table input_table(int_type integer)",
                        "assert select count(*) = 0 from input_table where int_type = 2",
                        "return input_table"
                )
        );
        executeContext.setVariable("partition_size", "2");
        executeContext.setVariable("partition_size_override", "1");
        executeContext.setVariable("zero_partition_size", "0");

        List<SqlTestCase> sqlList = Arrays.asList(
                // prepare source data
                new SqlTestCase(
                        "cache table t1 as select * from myTable",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
                ),
                new SqlTestCase(
                        "cache table t1_int as select int_type from myTable",
                        Arrays.<Object[]>asList(new Object[]{"t1_int", 3L})
                ),

                // call with partition by, each partition returns its row count
                new SqlTestCase(
                        "cache table r1 as call partition_echo_fun(t1) partition by t1 size 2",
                        Arrays.<Object[]>asList(new Object[]{"r1", 2L})
                ),
                new SqlTestCase(
                        "select * from r1",
                        Arrays.asList(
                                new Object[]{2},
                                new Object[]{1}
                        )
                ),

                // partition by with size larger than data -> single partition
                new SqlTestCase(
                        "cache table r2 as call partition_echo_fun(t1) partition by t1 size 100",
                        Arrays.<Object[]>asList(new Object[]{"r2", 1L})
                ),
                new SqlTestCase(
                        "select * from r2",
                        Collections.singletonList(new Object[]{3})
                ),

                // partition by with size 1 -> each row is a partition
                new SqlTestCase(
                        "cache table r3 as call partition_echo_fun(t1) partition by t1 size 1",
                        Arrays.<Object[]>asList(new Object[]{"r3", 3L})
                ),
                new SqlTestCase(
                        "select * from r3",
                        Arrays.asList(
                                new Object[]{1},
                                new Object[]{1},
                                new Object[]{1}
                        )
                ),

                // call with partition by + async (returns null, cache table should still work)
                new SqlTestCase("call partition_echo_fun(t1) partition by t1 size 2 async"),

                // call without partition (normal call)
                new SqlTestCase(
                        "cache table r4 as call partition_echo_fun(t1)",
                        Arrays.<Object[]>asList(new Object[]{"r4", 1L})
                ),
                new SqlTestCase(
                        "select * from r4",
                        Collections.singletonList(new Object[]{3})
                ),

                // partition by with like table
                new SqlTestCase(
                        "cache table r5 as call partition_echo_fun(t1) like r4 partition by t1 size 2",
                        Arrays.<Object[]>asList(new Object[]{"r5", 2L})
                ),
                new SqlTestCase(
                        "select * from r5",
                        Arrays.asList(
                                new Object[]{2},
                                new Object[]{1}
                        )
                ),

                // table identity is case-insensitive during validation and partition replacement
                new SqlTestCase(
                        "cache table MixedCaseTable as select * from myTable",
                        Arrays.<Object[]>asList(new Object[]{"MixedCaseTable", 3L})
                ),
                new SqlTestCase(
                        "cache table r6 as call partition_echo_fun(mixedcasetable) " +
                                "partition by MIXEDCASETABLE size 2",
                        Arrays.<Object[]>asList(new Object[]{"r6", 2L})
                ),
                new SqlTestCase(
                        "select * from r6",
                        Arrays.asList(
                                new Object[]{2},
                                new Object[]{1}
                        )
                ),

                // partition size can be resolved dynamically from the execution context
                new SqlTestCase(
                        "cache table r7 as call partition_echo_fun(t1) " +
                                "partition by t1 size get('partition_size')",
                        Arrays.<Object[]>asList(new Object[]{"r7", 2L})
                ),
                new SqlTestCase(
                        "select * from r7",
                        Arrays.asList(new Object[]{2}, new Object[]{1})
                ),
                new SqlTestCase(
                        "cache table r8 as call partition_echo_fun(t1) " +
                                "partition by t1 size get_or_default('unknown_partition_size', '2')",
                        Arrays.<Object[]>asList(new Object[]{"r8", 2L})
                ),
                new SqlTestCase(
                        "select * from r8",
                        Arrays.asList(new Object[]{2}, new Object[]{1})
                ),
                // A present variable takes precedence over get_or_default's fallback.
                new SqlTestCase(
                        "cache table r8_override as call partition_echo_fun(t1) " +
                                "partition by t1 size get_or_default('partition_size_override', '2')",
                        Arrays.<Object[]>asList(new Object[]{"r8_override", 3L})
                ),
                new SqlTestCase(
                        "select * from r8_override",
                        Arrays.asList(new Object[]{1}, new Object[]{1}, new Object[]{1})
                ),

                // Keep the legacy non-positive SIZE behavior: execute the whole table once.
                new SqlTestCase(
                        "cache table r8_zero_literal as call partition_echo_fun(t1) partition by t1 size 0",
                        Arrays.<Object[]>asList(new Object[]{"r8_zero_literal", 1L})
                ),
                new SqlTestCase(
                        "select * from r8_zero_literal",
                        Collections.singletonList(new Object[]{3})
                ),
                new SqlTestCase(
                        "cache table r8_zero_dynamic as call partition_echo_fun(t1) " +
                                "partition by t1 size get('zero_partition_size')",
                        Arrays.<Object[]>asList(new Object[]{"r8_zero_dynamic", 1L})
                ),
                new SqlTestCase(
                        "select * from r8_zero_dynamic",
                        Collections.singletonList(new Object[]{3})
                ),

                // A positive SIZE over an empty input produces no partitions and no output rows.
                new SqlTestCase("cache table empty_input as select * from myTable where int_type > 10"),
                new SqlTestCase(
                        "cache table r8_empty as call partition_echo_fun(empty_input) " +
                                "partition by empty_input size 1",
                        Arrays.<Object[]>asList(new Object[]{"r8_empty", 0L})
                ),
                new SqlTestCase("select * from r8_empty", Collections.emptyList()),

                // missing or non-numeric dynamic sizes fail at execution time
                new SqlTestCase(
                        "call partition_echo_fun(t1) partition by t1 size get('missing_partition_size')",
                        null,
                        new RuntimeException()
                ),
                new SqlTestCase(
                        "call partition_echo_fun(t1) " +
                                "partition by t1 size get_or_default('missing_partition_size', 'invalid')",
                        null,
                        new RuntimeException()
                ),
                new SqlTestCase(
                        "call partition_echo_fun(t1) " +
                                "partition by t1 size get_or_default('missing_partition_size', '2147483648')",
                        null,
                        new RuntimeException()
                ),

                // partition by table not in input -> should fail
                new SqlTestCase(
                        "call partition_echo_fun(t1) partition by t2 size 1",
                        null,
                        new RuntimeException()
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }

        // The default is strict: one failed partition makes the whole call fail even
        // when other partitions could succeed.
        new SqlTestCase(
                "cache table r9_strict as call partition_sometimes_fail_fun(t1) partition by t1 size 1",
                null,
                new RuntimeException()
        ).test(schema, executeContext);

        // When enabled dynamically, only the failed partition is discarded.
        executeContext.setVariable("IGNORE_PARTITION_EXCEPTION", "true");
        new SqlTestCase(
                "cache table r9 as call partition_sometimes_fail_fun(t1) partition by t1 size 1",
                Arrays.<Object[]>asList(new Object[]{"r9", 2L})
        ).test(schema, executeContext);
        new SqlTestCase(
                "select * from r9",
                Arrays.asList(new Object[]{1}, new Object[]{3})
        ).test(schema, executeContext);

        // SQL functions create nested execution contexts; failed partitions must remain
        // isolated there as well.
        new SqlTestCase(
                "cache table r10 as call partition_sql_sometimes_fail(t1_int) " +
                        "partition by t1_int size 1",
                Arrays.<Object[]>asList(new Object[]{"r10", 2L})
        ).test(schema, executeContext);
        new SqlTestCase(
                "select * from r10",
                Arrays.asList(new Object[]{1}, new Object[]{3})
        ).test(schema, executeContext);
    }

    public static class TestPartitionEchoFun {
        public CacheTable evaluate(CacheTable input) {
            List<Object[]> rows = new ArrayList<>();
            input.scan(null).forEach(rows::add);
            List<Object[]> result = new ArrayList<>();
            result.add(new Object[]{rows.size()});
            return new CacheTable("output", Linq4j.asEnumerable(result), createIntField("count"));
        }
    }

    public static class PartitionSometimesFailFun {
        public CacheTable evaluate(CacheTable input) {
            List<Object[]> rows = new ArrayList<>();
            input.scan(null).forEach(rows::add);
            int value = (Integer) rows.get(0)[0];
            if (value == 2) {
                throw new RuntimeException("partition 2 fails");
            }
            return new CacheTable(
                    "output",
                    Linq4j.asEnumerable(Collections.singletonList(new Object[]{value})),
                    createIntField("value")
            );
        }
    }

    private static List<RelDataTypeField> createIntField(String name) {
        return Collections.singletonList(
                new RelDataTypeFieldImpl(name, 0, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER))
        );
    }
}
