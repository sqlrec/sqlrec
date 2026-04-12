package com.sqlrec;

import com.sqlrec.common.config.Consts;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.udf.UdfManager;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestUdfSupport {
    @Test
    public void testUdfSupport() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        HmsSchema.setGlobalSchema(schema);

        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "uuid",
                "com.sqlrec.udf.scalar.UuidFunction"
        );
        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "l2_norm",
                "com.sqlrec.udf.scalar.L2NormFunction"
        );
        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "get",
                "com.sqlrec.udf.scalar.GetFunction"
        );

        ExecuteContextImpl executeContext = new ExecuteContextImpl();
        executeContext.setVariable("test_var", "hello_world");

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase("select uuid()"),
                new SqlTestCase(
                        "select l2_norm(array_float_type) from myTable",
                        Arrays.asList(
                                new Object[]{Arrays.asList(1.0 / Math.sqrt(14.0d), 2.0 / Math.sqrt(14.0d), 3.0 / Math.sqrt(14.0d))},
                                new Object[]{Arrays.asList(4.0 / Math.sqrt(77.0d), 5.0 / Math.sqrt(77.0d), 6.0 / Math.sqrt(77.0d))},
                                new Object[]{Arrays.asList(7.0 / Math.sqrt(194.0d), 8.0 / Math.sqrt(194.0d), 9.0 / Math.sqrt(194.0d))}
                        )
                ),
                new SqlTestCase(
                        "select l2_norm(array_double_type) from myTable",
                        Arrays.asList(
                                new Object[]{Arrays.asList(1.0 / Math.sqrt(14.0d), 2.0 / Math.sqrt(14.0d), 3.0 / Math.sqrt(14.0d))},
                                new Object[]{Arrays.asList(4.0 / Math.sqrt(77.0d), 5.0 / Math.sqrt(77.0d), 6.0 / Math.sqrt(77.0d))},
                                new Object[]{Arrays.asList(7.0 / Math.sqrt(194.0d), 8.0 / Math.sqrt(194.0d), 9.0 / Math.sqrt(194.0d))}
                        )
                ),
                new SqlTestCase(
                        "select SIN(0.1)",
                        Arrays.<Object[]>asList(
                                new Object[]{Math.sin(0.1)}
                        )
                ),
                new SqlTestCase(
                        "select count(1) from myTable",
                        Arrays.<Object[]>asList(
                                new Object[]{3L}
                        )
                ),
                new SqlTestCase(
                        "select sum(int_type) from myTable",
                        Arrays.<Object[]>asList(
                                new Object[]{6}
                        )
                ),
                new SqlTestCase(
                        "select min(int_type) from myTable",
                        Arrays.<Object[]>asList(
                                new Object[]{1}
                        )
                ),
                new SqlTestCase(
                        "select max(int_type) from myTable",
                        Arrays.<Object[]>asList(
                                new Object[]{3}
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
                        "select CHAR_LENGTH(varchar_type) from myTable",
                        Arrays.asList(
                                new Object[]{3},
                                new Object[]{3},
                                new Object[]{3}
                        )
                ),
                new SqlTestCase(
                        "select SUBSTRING(varchar_type from 1 for 2) from myTable",
                        Arrays.asList(
                                new Object[]{"ab"},
                                new Object[]{"bc"},
                                new Object[]{"cd"}
                        )
                ),
                new SqlTestCase(
                        "select varchar_type || '1' from myTable",
                        Arrays.asList(
                                new Object[]{"abc1"},
                                new Object[]{"bcd1"},
                                new Object[]{"cde1"}
                        )
                ),
                new SqlTestCase(
                        "select CARDINALITY(array_int_type) from myTable",
                        Arrays.asList(
                                new Object[]{3},
                                new Object[]{3},
                                new Object[]{3}
                        )
                ),
                new SqlTestCase(
                        "select CARDINALITY(array_varchar_type) from myTable",
                        Arrays.asList(
                                new Object[]{3},
                                new Object[]{3},
                                new Object[]{3}
                        )
                ),
                new SqlTestCase(
                        "select CARDINALITY(array_float_type) from myTable",
                        Arrays.asList(
                                new Object[]{3},
                                new Object[]{3},
                                new Object[]{3}
                        )
                ),
                new SqlTestCase(
                        "select CARDINALITY(array_double_type) from myTable",
                        Arrays.asList(
                                new Object[]{3},
                                new Object[]{3},
                                new Object[]{3}
                        )
                ),
                new SqlTestCase("select CURRENT_TIMESTAMP"),
                new SqlTestCase("select CURRENT_TIMESTAMP(1)"),
                new SqlTestCase("select cast(CURRENT_TIMESTAMP as BIGINT) as req_time"),
                new SqlTestCase(
                        "select `get`('test_var')",
                        Arrays.<Object[]>asList(
                                new Object[]{"hello_world"}
                        )
                ),
                new SqlTestCase("set 'num_var' = '12345'"),
                new SqlTestCase(
                        "select `get`('num_var')",
                        Arrays.<Object[]>asList(
                                new Object[]{"12345"}
                        )
                ),
                new SqlTestCase(
                        "select cast(`get`('num_var') as bigint)",
                        Arrays.<Object[]>asList(
                                new Object[]{12345L}
                        )
                ),
                new SqlTestCase(
                        "select `get`('nonexistent_var')",
                        Arrays.<Object[]>asList(
                                new Object[]{null}
                        )
                ),
                new SqlTestCase(
                        "select `get`('test_var') || '!'",
                        Arrays.<Object[]>asList(
                                new Object[]{"hello_world!"}
                        )
                ),
                new SqlTestCase("cache table tmp as select 'test_var' as name"),
                new SqlTestCase(
                        "select `get`(name) || '!' from tmp",
                        Arrays.<Object[]>asList(
                                new Object[]{"hello_world!"}
                        )
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }
}
