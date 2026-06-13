package com.sqlrec.runtime;

import com.sqlrec.TestTypeSupport;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
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
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "partition_echo_fun", TestPartitionEchoFun.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // prepare source data
                new SqlTestCase(
                        "cache table t1 as select * from myTable",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
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

    private static List<RelDataTypeField> createIntField(String name) {
        return Collections.singletonList(
                new RelDataTypeFieldImpl(name, 0, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER))
        );
    }
}
