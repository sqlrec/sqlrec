package com.sqlrec.executor;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlExecutorLocalMetadataTest {
    @Test
    void rejectsSqlFunctionDdlInLocalMetadataMode(@TempDir Path sqlDir) {
        String previous = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        try {
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(sqlDir.toString());
            CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
            SqlExecutor executor = new SqlExecutor();

            assertThrows(UnsupportedOperationException.class,
                    () -> executor.executeSqlAsync("CREATE SQL FUNCTION test_function"));
            assertThrows(UnsupportedOperationException.class,
                    () -> executor.executeSqlAsync("CREATE OR REPLACE SQL FUNCTION test_function"));
        } finally {
            CalciteSchemaFactory.setGlobalSchema(null);
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(previous);
        }
    }
}
