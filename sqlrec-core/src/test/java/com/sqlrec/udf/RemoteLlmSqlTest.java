package com.sqlrec.udf;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.udf.config.FunctionConfigs;
import com.sun.net.httpserver.HttpServer;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RemoteLlmSqlTest {
    private HttpServer server;
    private CalciteSchema schema;
    private String url;
    private final AtomicInteger requests = new AtomicInteger();

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            requests.incrementAndGet();
            String path = exchange.getRequestURI().getPath();
            String response = path.equals("/error") ? "{\"error\":{\"message\":\"unavailable\"}}"
                    : path.equals("/ark") ? "{\"data\":{\"embedding\":[3,4]}}"
                    : path.equals("/chat") ? "{\"choices\":[{\"message\":{\"content\":\"summary\"}}]}"
                    : "{\"data\":[{\"embedding\":[3,4]}]}";
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(path.equals("/error") ? 503 : 200, bytes.length);
            try {
                exchange.getResponseBody().write(bytes);
            } finally {
                exchange.close();
            }
        });
        server.start();
        url = "http://127.0.0.1:" + server.getAddress().getPort();
        schema = CalciteSchema.createRootSchema(false);
        CalciteSchema child = schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema());
        for (String name : Arrays.asList("llm_embed_text", "llm_complete", "ark_embed_image", "ip", "l2_norm")) {
            UdfManager.addFunction(child, name, FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.get(name));
        }
        CalciteSchemaFactory.setGlobalSchema(schema);
    }

    @AfterEach
    void cleanup() {
        server.stop(0);
        CalciteSchemaFactory.setGlobalSchema(null);
    }

    @Test
    void constantArgumentsDoNotCallRemoteDuringCompilationAndReusedPlansCallAtRuntime() throws Exception {
        for (String function : Arrays.asList("llm_embed_text", "llm_complete", "ark_embed_image")) {
            String path = function.equals("llm_complete") ? "/chat" : function.equals("ark_embed_image") ? "/ark" : "/embed";
            String input = function.equals("ark_embed_image") ? "https://example.com/image.png" : "text";
            int before = requests.get();
            BindableInterface plan = compile("select " + function + "('" + url + path
                    + "', 'key', 'model', '" + input + "', '{}') as output_value");
            assertEquals(before, requests.get(), "Compiling a constant remote UDF must not send a request");
            assertNotNull(plan.getLogicalPlan());
            assertNotNull(plan.getPhysicalPlan());
            if (!function.equals("llm_complete")) {
                assertEquals(SqlTypeName.ARRAY, plan.getReturnDataFields().get(0).getType().getSqlTypeName());
                assertEquals(SqlTypeName.DOUBLE, plan.getReturnDataFields().get(0).getType().getComponentType().getSqlTypeName());
            }
            for (int run = 1; run <= 2; run++) {
                Object actual = plan.bind(schema, new ExecuteContextImpl(schema)).toList().get(0)[0];
                assertEquals(function.equals("llm_complete") ? "summary" : Arrays.asList(3.0, 4.0), actual);
                assertEquals(before + run, requests.get());
            }
        }
    }

    @Test
    void retrievingPlansDoesNotCallRemote() throws Exception {
        BindableInterface plan = compile("select llm_embed_text('" + url + "/embed', 'key', 'model', 'text', '{}')");
        assertNotNull(plan.getLogicalPlan());
        assertNotNull(plan.getPhysicalPlan());
        assertNotNull(plan.getJavaExpression());
        assertEquals(0, requests.get());
    }

    @Test
    void vectorsComposeWithExistingFunctionsAndSqlArrayOperators() throws Exception {
        String expression = "llm_embed_text('" + url + "/embed', 'key', 'model', 'text', '{}')";
        assertEquals(2, execute("select cardinality(" + expression + ")").get(0)[0]);
        assertEquals(3.0, execute("select " + expression + "[1]").get(0)[0]);
        assertEquals(Arrays.asList(0.6, 0.8), execute("select l2_norm(" + expression + ")").get(0)[0]);
        assertEquals(25.0, execute("select ip(" + expression + ", ARRAY[3.0, 4.0])").get(0)[0]);
    }

    @Test
    void inputColumnsAreEvaluatedPerRowAndNullSkipsRequests() throws Exception {
        List<Object[]> rows = execute("select llm_complete('" + url + "/chat', 'key', 'model', txt, '{}') "
                + "from (values ('one'), ('two'), (cast(null as varchar))) as t(txt)");
        assertEquals(3, rows.size());
        assertEquals("summary", rows.get(0)[0]);
        assertEquals("summary", rows.get(1)[0]);
        assertNull(rows.get(2)[0]);
        assertEquals(2, requests.get());
    }

    @Test
    void remoteFailuresPropagateThroughSqlExecution() throws Exception {
        for (String function : Arrays.asList("llm_embed_text", "llm_complete", "ark_embed_image")) {
            String input = function.equals("ark_embed_image") ? "https://example.com/image.png" : "text";
            BindableInterface plan = compile("select " + function + "('" + url
                    + "/error', 'key', 'model', '" + input + "', '{}')");
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> plan.bind(schema, new ExecuteContextImpl(schema)).toList());
            Throwable cause = failure;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertEquals("LLM HTTP request failed (status 503)", cause.getMessage());
        }
        assertEquals(3, requests.get());
    }

    @Test
    void hiveNonDeterminismReachesCalciteOperators() {
        for (String name : Arrays.asList("llm_embed_text", "llm_complete", "ark_embed_image", "l2_norm")) {
            List<SqlOperator> operators = new ArrayList<>();
            NormalSqlCompiler.getCatalogReader(schema, Consts.DEFAULT_SCHEMA_NAME).lookupOperatorOverloads(
                    new SqlIdentifier(name, SqlParserPos.ZERO), SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    SqlSyntax.FUNCTION, operators, SqlNameMatchers.withCaseSensitive(false));
            assertEquals(1, operators.size());
            assertEquals(name.equals("l2_norm"), operators.get(0).isDeterministic());
        }
    }

    private BindableInterface compile(String sql) throws Exception {
        return new CompileManager().compileSql(CompileManager.parseSql(sql), schema, Consts.DEFAULT_SCHEMA_NAME, sql);
    }

    private List<Object[]> execute(String sql) throws Exception {
        return compile(sql).bind(schema, new ExecuteContextImpl(schema)).toList();
    }
}
