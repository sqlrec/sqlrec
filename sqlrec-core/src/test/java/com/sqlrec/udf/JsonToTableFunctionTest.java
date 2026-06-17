package com.sqlrec.udf;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.udf.table.JsonToTableFunction;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JsonToTableFunctionTest {

    @Test
    public void testJsonObject() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.emptyMap();
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);
        JavaFunctionUtils.registerTableFunction("default", "json_to_table", JsonToTableFunction.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // single json object -> 1 row
                new SqlTestCase(
                        "cache table t1 as call json_to_table('{\"name\":\"Alice\",\"age\":30,\"score\":95.5}')",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                new SqlTestCase(
                        "select name, age, score from t1",
                        Collections.singletonList(new Object[]{"Alice", 30.0d, 95.5d})
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    @Test
    public void testJsonArray() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.emptyMap();
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);
        JavaFunctionUtils.registerTableFunction("default", "json_to_table", JsonToTableFunction.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // json array -> multiple rows
                new SqlTestCase(
                        "cache table t1 as call json_to_table('[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]')",
                        Arrays.<Object[]>asList(new Object[]{"t1", 2L})
                ),
                new SqlTestCase(
                        "select name, age from t1",
                        Arrays.asList(
                                new Object[]{"Alice", 30.0d},
                                new Object[]{"Bob", 25.0d}
                        )
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    @Test
    public void testWithArrayField() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.emptyMap();
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);
        JavaFunctionUtils.registerTableFunction("default", "json_to_table", JsonToTableFunction.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // array field with numbers -> ARRAY<DOUBLE>
                new SqlTestCase(
                        "cache table t1 as call json_to_table('{\"name\":\"Alice\",\"scores\":[90.0,85.5,92.3]}')",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                new SqlTestCase(
                        "select name, scores from t1",
                        Collections.singletonList(new Object[]{"Alice", Arrays.asList(90.0d, 85.5d, 92.3d)})
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    @Test
    public void testWithBooleanField() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.emptyMap();
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);
        JavaFunctionUtils.registerTableFunction("default", "json_to_table", JsonToTableFunction.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t1 as call json_to_table('{\"name\":\"Alice\",\"active\":true,\"score\":88.5}')",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                new SqlTestCase(
                        "select name, active, score from t1",
                        Collections.singletonList(new Object[]{"Alice", true, 88.5d})
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    @Test
    public void testWithNullAndMissingFields() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.emptyMap();
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);
        JavaFunctionUtils.registerTableFunction("default", "json_to_table", JsonToTableFunction.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // different objects with different keys -> missing keys are null
                new SqlTestCase(
                        "cache table t1 as call json_to_table('[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"score\":95.0}]')",
                        Arrays.<Object[]>asList(new Object[]{"t1", 2L})
                ),
                new SqlTestCase(
                        "select name, age, score from t1",
                        Arrays.asList(
                                new Object[]{"Alice", 30.0d, null},
                                new Object[]{"Bob", null, 95.0d}
                        )
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    @Test
    public void testWithStringArrayField() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.emptyMap();
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);
        JavaFunctionUtils.registerTableFunction("default", "json_to_table", JsonToTableFunction.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // string array -> ARRAY<VARCHAR>
                new SqlTestCase(
                        "cache table t1 as call json_to_table('{\"name\":\"Alice\",\"tags\":[\"sports\",\"music\"]}')",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                new SqlTestCase(
                        "select name, tags from t1",
                        Collections.singletonList(new Object[]{"Alice", Arrays.asList("sports", "music")})
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }
}
