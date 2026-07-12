package com.sqlrec.udf;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.runtime.SqlRecDataContext;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlRecDataContextImpl;
import com.sqlrec.udf.scalar.GetFunction;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class GetFunctionTest {
    private ExecuteContext executeContext;
    private SqlRecDataContext dataContext;

    @BeforeEach
    public void setUp() {
        executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        dataContext = new SqlRecDataContextImpl(Collections.emptyMap(), schema, executeContext);
    }

    @Test
    public void testGetExistingVariable() {
        executeContext.setVariable("key1", "value1");
        assertEquals("value1", GetFunction.evaluate(dataContext, "key1"));
    }

    @Test
    public void testGetNonExistingVariable() {
        assertNull(GetFunction.evaluate(dataContext, "nonexistent"));
    }
}
