package com.sqlrec.udf.table;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.apache.calcite.linq4j.Linq4j;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CallServiceUserItemsTest {
    @Test
    void sendsUserItemColumnsAndMergesPredictionsIntoEveryItemRow() {
        AtomicInteger requests = new AtomicInteger();
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(chain -> {
            requests.incrementAndGet();
            Buffer body = new Buffer();
            chain.request().body().writeTo(body);
            assertEquals("{\"SHARED\":[\"context\"],\"user\":[null],"
                    + "\"item_id\":[1,2],\"price\":[99.9,199.9]}", body.readUtf8());
            return new Response.Builder().request(chain.request()).protocol(Protocol.HTTP_1_1)
                    .code(200).message("OK")
                    .body(ResponseBody.create("{\"score\":[0.1,0.2]}", MediaType.parse("application/json")))
                    .build();
        }).build();
        CacheTable user = table("user", List.of("USER", "shared"),
                Collections.singletonList(new Object[]{null, "context"}));
        CacheTable items = table("items", List.of("shared", "item_id", "price"),
                Arrays.asList(new Object[]{"ignored", 1, 99.9}, new Object[]{"ignored", 2, 199.9}));

        CacheTable output = new CallServiceFunction(client).evaluate(context(), "rank", user, items);

        List<Object[]> rows = output.scan(null).toList();
        assertEquals(1, requests.get());
        assertEquals(2, rows.size());
        assertArrayEquals(new Object[]{"ignored", 1, 99.9, 0.1}, rows.get(0));
        assertArrayEquals(new Object[]{"ignored", 2, 199.9, 0.2}, rows.get(1));
        assertEquals("score", output.getDataFields().get(3).getName());
    }

    @Test
    void skipsHttpForEmptyItemsAndRejectsInvalidUserRowCounts() {
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(chain -> {
            throw new AssertionError("empty items or invalid users must not send a request");
        }).build();
        CallServiceFunction function = new CallServiceFunction(client);
        CacheTable items = table("items", List.of("item_id"), List.of());
        CacheTable user = table("user", List.of("user"),
                Collections.singletonList(new Object[]{"one"}));

        CacheTable output = function.evaluate(context(), "rank", user, items);
        assertEquals(List.of(), output.scan(null).toList());
        assertEquals("score", output.getDataFields().get(1).getName());
        for (List<Object[]> users : Arrays.asList(Collections.<Object[]>emptyList(),
                Arrays.asList(new Object[]{"one"}, new Object[]{"two"}))) {
            assertEquals("User table must have exactly one row", assertThrows(RuntimeException.class,
                    () -> function.evaluate(context(), "rank", table("user", List.of("user"), users), items))
                    .getMessage());
        }
    }

    private static ReadonlyContext context() {
        ModelConf model = new ModelConf();
        model.setInputFields(List.of(new FieldSchema("item_id", "INT"),
                new FieldSchema("SHARED", "STRING"), new FieldSchema("user", "STRING"),
                new FieldSchema("price", "DOUBLE"), new FieldSchema("missing", "STRING")));
        ServiceConf service = new ServiceConf();
        service.setModelConfig(model);
        service.setUrl("http://prediction");
        ModelController controller = mock(ModelController.class);
        when(controller.getOutputFields(model)).thenReturn(List.of(new FieldSchema("score", "DOUBLE")));
        ReadonlyContext context = mock(ReadonlyContext.class);
        when(context.getServiceConfig("rank")).thenReturn(service);
        when(context.getModelController(model)).thenReturn(controller);
        return context;
    }

    private static CacheTable table(String name, List<String> fields, List<Object[]> rows) {
        return new CacheTable(name, Linq4j.asEnumerable(rows), DataTypeUtils.getStringTypeFieldList(fields));
    }
}
