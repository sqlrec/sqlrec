package com.sqlrec.utils;

import io.opentelemetry.context.Context;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TraceUtilsTest {

    @Test
    public void testUnwrapContextNullReturnsRoot() {
        Context result = TraceUtils.unwrapContext(null);

        assertEquals(Context.root(), result);
    }

    @Test
    public void testUnwrapContextNonContextReturnsRoot() {
        Context result = TraceUtils.unwrapContext("not a context");

        assertEquals(Context.root(), result);
    }

    @Test
    public void testUnwrapContextActualContext() {
        Context ctx = Context.root();

        Context result = TraceUtils.unwrapContext(ctx);

        assertSame(ctx, result);
    }
}
