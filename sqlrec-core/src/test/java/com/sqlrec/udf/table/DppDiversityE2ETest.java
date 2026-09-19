package com.sqlrec.udf.table;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
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

public class DppDiversityE2ETest {
    @Test
    public void testDppDiversityThroughSqlCall() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("candidates", new CandidateTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        List<SqlTestCase> sqlCases = Arrays.asList(
                new SqlTestCase(
                        "cache table input as select * from candidates",
                        Arrays.<Object[]>asList(new Object[]{"input", 3L})
                ),
                new SqlTestCase(
                        "cache table dpp_output as call dpp_diversity(input, 'embedding', 'score', '0', '2')",
                        Arrays.<Object[]>asList(new Object[]{"dpp_output", 2L})
                ),
                new SqlTestCase(
                        "select * from dpp_output",
                        Arrays.asList(
                                new Object[]{1, 0.1, Arrays.asList(1.0, 0.0)},
                                new Object[]{3, 0.01, Arrays.asList(-1.0, 0.0)}
                        )
                )
        );

        for (SqlTestCase sqlCase : sqlCases) {
            sqlCase.test(schema, executeContext);
        }
    }

    public static class CandidateTable extends SqlRecTable implements ScannableTable {
        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, 0.1, Arrays.asList(1.0, 0.0)},
                    {2, 1000.0, Arrays.asList(0.0, 1.0)},
                    {3, 0.01, Arrays.asList(-1.0, 0.0)},
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", SqlTypeName.INTEGER)
                    .add("score", SqlTypeName.DOUBLE)
                    .add("embedding", typeFactory.createArrayType(
                            typeFactory.createSqlType(SqlTypeName.DOUBLE), -1))
                    .build();
        }
    }
}
