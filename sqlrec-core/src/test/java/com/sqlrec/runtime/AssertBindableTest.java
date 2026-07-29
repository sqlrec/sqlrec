package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.utils.SqlTestCase;
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

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AssertBindableTest {

    @Test
    public void testAssertPassTrue() throws Exception {
        CalciteSchema schema = buildSchema();

        // single boolean true -> assertion passes, no result returned
        SqlTestCase passCase = new SqlTestCase(
                "assert select count(*) > 0 from myTable", null);
        passCase.test(schema);

        // multiple boolean columns, all true -> passes
        SqlTestCase multiColCase = new SqlTestCase(
                "assert select count(*) > 0, count(*) >= 3 from myTable", null);
        multiColCase.test(schema);
    }

    @Test
    public void testAssertPassWithWhere() throws Exception {
        CalciteSchema schema = buildSchema();

        SqlTestCase passCase = new SqlTestCase(
                "assert select count(*) > 0 from myTable where id > 1", null);
        passCase.test(schema);
    }

    @Test
    public void testAssertFailWhenFalse() throws Exception {
        CalciteSchema schema = buildSchema();

        // myTable has 3 rows; count(*) > 100 is false -> assert throws
        SqlTestCase failCase = new SqlTestCase(
                "assert select count(*) > 100 from myTable",
                null,
                new RuntimeException("returned false"));
        failCase.test(schema);
    }

    @Test
    public void testAssertFailWhenNoRows() throws Exception {
        CalciteSchema schema = buildSchema();

        // WHERE filters out all rows -> empty result -> assert throws "no rows"
        SqlTestCase failCase = new SqlTestCase(
                "assert select count(*) > 0 from myTable where id > 1000",
                null,
                new RuntimeException("no rows"));
        failCase.test(schema);
    }

    @Test
    public void testAssertFailWhenNullBoolean() throws Exception {
        CalciteSchema schema = buildSchema();

        // count(*) returns 0 for empty set, 0 > 0 is false -> throws
        SqlTestCase failCase = new SqlTestCase(
                "assert select count(*) > 0 from myTable where id > 1000",
                null,
                new RuntimeException("assert failed"));
        failCase.test(schema);
    }

    @Test
    public void testAssertBindableNotParallelizable() throws Exception {
        CalciteSchema schema = buildSchema();

        com.sqlrec.compiler.CompileManager cm = new com.sqlrec.compiler.CompileManager();
        org.apache.calcite.sql.SqlNode node =
                com.sqlrec.compiler.CompileManager.parseFlinkSql(
                        "assert select count(*) > 0 from myTable");
        BindableInterface bindable = cm.compileSql(
                node, schema, Consts.DEFAULT_SCHEMA_NAME,
                "assert select count(*) > 0 from myTable");

        assertTrue(bindable instanceof AssertBindable,
                "expected AssertBindable but got " + bindable.getClass());
        AssertBindable assertBindable = (AssertBindable) bindable;

        assertTrue(!assertBindable.isParallelizable(),
                "assert operator must not be parallelizable");
        assertNotNull(assertBindable.getSelectBindable());
    }

    // --- schema setup (mirrors TestNormalSql.MyTable) ---

    private CalciteSchema buildSchema() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });
        return schema;
    }

    public static class MyTable extends com.sqlrec.common.schema.SqlRecTable implements ScannableTable {
        @Override
        public @Nullable Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
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
