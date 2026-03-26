package com.sqlrec.model;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.sql.parser.SqlCreateModel;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ModelEntityConverterTest {

    @Test
    public void testConvertToModel() throws Exception {
        String createModelSql = "create model `test_model` (id bigint, name string) with ('param'='value')";
        SqlNode sqlNode = CompileManager.parseFlinkSql(createModelSql);

        assertTrue(sqlNode instanceof SqlCreateModel);
        SqlCreateModel sqlCreateModel = (SqlCreateModel) sqlNode;

        ModelConfig model = ModelEntityConverter.convertToModel(sqlCreateModel);

        assertNotNull(model);
        assertEquals("test_model", model.modelName);
        assertNotNull(model.inputFields);
        assertEquals(2, model.inputFields.size());
        assertEquals("id", model.inputFields.get(0).name);
        assertEquals("BIGINT", model.inputFields.get(0).type);
        assertEquals("name", model.inputFields.get(1).name);
        assertEquals("STRING", model.inputFields.get(1).type);
        assertNotNull(model.params);
        assertEquals(1, model.params.size());
        assertEquals("value", model.params.get("param"));
    }
}
