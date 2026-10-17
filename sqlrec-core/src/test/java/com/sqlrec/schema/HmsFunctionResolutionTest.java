package com.sqlrec.schema;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.udf.scalar.IpFunction;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.ScalarFunction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HmsFunctionResolutionTest {
    @Test
    void sameImplementationIsRegisteredOnceAndExecutesArrayArguments() throws Exception {
        CalciteSchema root = schema("ip", IpFunction.class.getName());
        assertSingleIp(root, 11.0);
    }

    @Test
    void mixedCaseNameAlsoOverridesBuiltin() throws Exception {
        CalciteSchema root = schema("IP", OverrideIp.class.getName());
        assertSingleIp(root, 42.0);
    }

    @Test
    void differentImplementationInHmsTakesPrecedence() throws Exception {
        CalciteSchema root = schema("ip", OverrideIp.class.getName());
        assertSingleIp(root, 42.0);
    }

    @Test
    @SilenceLoggers(HmsSchema.class)
    void failedHmsDefinitionFallsBackToBuiltin() throws Exception {
        assertSingleIp(schema("ip", "example.MissingIp"), 11.0);
    }

    @Test
    void classWithoutEvaluateFallsBackToBuiltin() throws Exception {
        assertSingleIp(schema("ip", String.class.getName()), 11.0);
    }

    @Test
    void vectorRecallCompilesAndExecutesWithHmsAndBuiltinIp() throws Exception {
        CalciteSchema root = schema("ip", IpFunction.class.getName());
        ExecuteContextImpl context = new ExecuteContextImpl(root);
        CompileManager compiler = new CompileManager();
        for (String sql : List.of(
                "CACHE TABLE user_embedding AS SELECT "
                        + "ARRAY[CAST(1 AS FLOAT), CAST(2 AS FLOAT)] AS user_tower_emb",
                "CACHE TABLE item_embedding AS SELECT 10 AS id, "
                        + "ARRAY[CAST(3 AS DOUBLE), CAST(4 AS DOUBLE)] AS embedding "
                        + "UNION ALL SELECT 20 AS id, "
                        + "ARRAY[CAST(0 AS DOUBLE), CAST(1 AS DOUBLE)] AS embedding",
                "CACHE TABLE vector_recall AS SELECT item_embedding.id AS movie_id "
                        + "FROM user_embedding JOIN item_embedding ON 1=1 "
                        + "ORDER BY ip(user_embedding.user_tower_emb, item_embedding.embedding) LIMIT 300")) {
            compiler.compileSql(CompileManager.parseSql(sql), root, "default", sql).bind(root, context);
        }
        List<Object[]> rows = SchemaUtils.getCacheTable("vector_recall", root).scan(null).toList();
        assertEquals(List.of(20, 10), rows.stream().map(row -> row[0]).toList());
    }

    private static CalciteSchema schema(String name, String className) throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var definition = new org.apache.hadoop.hive.metastore.api.Function();
        definition.setFunctionName(name);
        definition.setClassName(className);
        when(metadata.getFunctions("default")).thenReturn(List.of(definition));
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        root.add("default", new HmsSchema("default", metadata));
        return root;
    }

    private static void assertSingleIp(CalciteSchema root, double expected) throws Exception {
        var functions = root.getSubSchema("default", false).getFunctions("ip", false);
        assertEquals(1, functions.size());
        assertInstanceOf(ScalarFunction.class, functions.iterator().next());
        String sql = "SELECT ip(ARRAY[CAST(1 AS FLOAT), CAST(2 AS FLOAT)], "
                + "ARRAY[CAST(3 AS DOUBLE), CAST(4 AS DOUBLE)]) AS score";
        List<Object[]> rows = NormalSqlCompiler.getNormalSqlBindable(sql, root, "default")
                .bind(root, new ExecuteContextImpl(root)).toList();
        assertEquals(1, rows.size());
        assertEquals(expected, rows.get(0)[0]);
    }

    public static class OverrideIp {
        public Double evaluate(Object first, Object second) {
            return 42.0;
        }
    }
}
