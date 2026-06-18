package com.sqlrec;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.*;

public class TestJavaFunctionSchemaCheck {

    /**
     * Function that returns a CacheTable with schema (name VARCHAR, age INTEGER).
     */
    public static class TestSchemaMatchFun {
        public CacheTable evaluate() {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"Alice", 30});
            List<RelDataTypeField> fields = Arrays.asList(
                    DataTypeUtils.getRelDataTypeField("name", 0, SqlTypeName.VARCHAR),
                    DataTypeUtils.getRelDataTypeField("age", 1, SqlTypeName.INTEGER)
            );
            return new CacheTable("output", Linq4j.asEnumerable(data), fields);
        }
    }

    /**
     * Function that returns a CacheTable with schema (username VARCHAR, age INTEGER),
     * which has a different field name from what "like" expects.
     */
    public static class TestFieldNameMismatchFun {
        public CacheTable evaluate() {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"Alice", 30});
            List<RelDataTypeField> fields = Arrays.asList(
                    DataTypeUtils.getRelDataTypeField("username", 0, SqlTypeName.VARCHAR),
                    DataTypeUtils.getRelDataTypeField("age", 1, SqlTypeName.INTEGER)
            );
            return new CacheTable("output", Linq4j.asEnumerable(data), fields);
        }
    }

    /**
     * Function that returns a CacheTable with schema (name VARCHAR, age VARCHAR),
     * which has a different field type from what "like" expects.
     */
    public static class TestFieldTypeMismatchFun {
        public CacheTable evaluate() {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"Alice", "30"});
            List<RelDataTypeField> fields = Arrays.asList(
                    DataTypeUtils.getRelDataTypeField("name", 0, SqlTypeName.VARCHAR),
                    DataTypeUtils.getRelDataTypeField("age", 1, SqlTypeName.VARCHAR)
            );
            return new CacheTable("output", Linq4j.asEnumerable(data), fields);
        }
    }

    /**
     * Function that returns a CacheTable with only one field (name VARCHAR),
     * fewer than what "like" expects.
     */
    public static class TestFieldCountMismatchFun {
        public CacheTable evaluate() {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"Alice"});
            List<RelDataTypeField> fields = Collections.singletonList(
                    DataTypeUtils.getRelDataTypeField("name", 0, SqlTypeName.VARCHAR)
            );
            return new CacheTable("output", Linq4j.asEnumerable(data), fields);
        }
    }

    @Test
    public void testSchemaMatch() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "schema_match_fun", TestSchemaMatchFun.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // Create a reference table with schema (name VARCHAR, age INTEGER)
                new SqlTestCase(
                        "cache table t1 as select 'Alice' as name, 30 as age",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                // Function returns matching schema - should succeed
                new SqlTestCase(
                        "cache table r1 as call schema_match_fun() like t1",
                        Arrays.<Object[]>asList(new Object[]{"r1", 1L})
                ),
                new SqlTestCase(
                        "select * from r1",
                        Collections.singletonList(new Object[]{"Alice", 30})
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    @Test
    public void testSchemaFieldNameMismatch() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "field_name_mismatch_fun", TestFieldNameMismatchFun.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // Create a reference table with schema (name VARCHAR, age INTEGER)
                new SqlTestCase(
                        "cache table t1 as select 'Alice' as name, 30 as age",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                // Function returns schema (username VARCHAR, age INTEGER) - field name mismatch
                new SqlTestCase(
                        "cache table r1 as call field_name_mismatch_fun() like t1",
                        null,
                        new RuntimeException()
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    @Test
    public void testSchemaFieldTypeMismatch() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "field_type_mismatch_fun", TestFieldTypeMismatchFun.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // Create a reference table with schema (name VARCHAR, age INTEGER)
                new SqlTestCase(
                        "cache table t1 as select 'Alice' as name, 30 as age",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                // Function returns schema (name VARCHAR, age VARCHAR) - field type mismatch
                new SqlTestCase(
                        "cache table r1 as call field_type_mismatch_fun() like t1",
                        null,
                        new RuntimeException()
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    @Test
    public void testSchemaFieldCountMismatch() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "field_count_mismatch_fun", TestFieldCountMismatchFun.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // Create a reference table with schema (name VARCHAR, age INTEGER)
                new SqlTestCase(
                        "cache table t1 as select 'Alice' as name, 30 as age",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                // Function returns schema (name VARCHAR) - fewer fields than expected
                new SqlTestCase(
                        "cache table r1 as call field_count_mismatch_fun() like t1",
                        null,
                        new RuntimeException()
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }
}
