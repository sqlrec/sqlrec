package com.sqlrec.udf;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.runtime.SqlRecDataContext;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlRecDataContextImpl;
import com.sqlrec.udf.scalar.GetOrDefaultFunction;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class GetOrDefaultFunctionTest {
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
        assertEquals("value1", GetOrDefaultFunction.evaluate(dataContext, "key1", "default"));
    }

    @Test
    public void testGetNonExistingVariableReturnsDefault() {
        assertEquals("default", GetOrDefaultFunction.evaluate(dataContext, "nonexistent", "default"));
    }

    @Test
    public void testNullDefaultValue() {
        assertNull(GetOrDefaultFunction.evaluate(dataContext, "nonexistent", null));
    }
}
