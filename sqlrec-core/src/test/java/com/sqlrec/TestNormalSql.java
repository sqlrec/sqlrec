package com.sqlrec;

import com.sqlrec.common.config.Consts;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestNormalSql {
    @Test
    public void testNormalSqlCompiler() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "select * from myTable",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                ),
                new SqlTestCase(
                        "SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME",
                        Arrays.asList(
                                new Object[]{"Bob", 1L},
                                new Object[]{"Charlie", 1L}
                        )
                ),
                new SqlTestCase(
                        "cache table t0 as select 1 as a",
                        Arrays.<Object[]>asList(
                                new Object[]{"t0", 1L}
                        )
                ),
                new SqlTestCase(
                        "select * from t0",
                        Arrays.<Object[]>asList(
                                new Object[]{1}
                        )
                ),
                new SqlTestCase(
                        "cache table t1 as SELECT * FROM myTable",
                        Arrays.<Object[]>asList(
                                new Object[]{"t1", 3L}
                        )
                ),
                new SqlTestCase(
                        "select * from t1",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                ),
                new SqlTestCase(
                        "cache table t2 as SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME",
                        Arrays.<Object[]>asList(
                                new Object[]{"t2", 2L}
                        )
                ),
                new SqlTestCase(
                        "select * from t2",
                        Arrays.asList(
                                new Object[]{"Bob", 1L},
                                new Object[]{"Charlie", 1L}
                        )
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testPlanValidationWithExpectedStrings() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });

        String expectedLogicalPlan = """
                LogicalProject(ID=[$0], NAME=[$1])
                  LogicalTableScan(table=[[default, myTable]])""";
        String expectedPhysicalPlan = """
                EnumerableTableScan(table=[[default, myTable]])""";
        String expectedJavaExpression = """
                public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext root) {
                  return org.apache.calcite.schema.Schemas.enumerable((org.apache.calcite.schema.ScannableTable) root.getRootSchema().getSubSchema("default").getTable("myTable"), root);
                }
                public Class getElementType() {
                  return java.lang.Object[].class;
                }""";

        List<Object[]> expectedResults = new ArrayList<>();
        expectedResults.add(new Object[]{1, "Alice"});
        expectedResults.add(new Object[]{2, "Bob"});
        expectedResults.add(new Object[]{3, "Charlie"});

        SqlTestCase planValidationCase = new SqlTestCase(
                "select * from myTable",
                expectedResults,
                expectedLogicalPlan,
                expectedPhysicalPlan,
                expectedJavaExpression
        );
        planValidationCase.test(schema);
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
