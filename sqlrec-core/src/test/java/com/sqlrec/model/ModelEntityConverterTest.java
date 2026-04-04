package com.sqlrec.model;

import com.sqlrec.common.config.ModelConfigs;
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
        assertEquals("test_model", model.getModelName());
        assertNotNull(model.getInputFields());
        assertEquals(2, model.getInputFields().size());
        assertEquals("id", model.getInputFields().get(0).getName());
        assertEquals("BIGINT", model.getInputFields().get(0).getType());
        assertEquals("name", model.getInputFields().get(1).getName());
        assertEquals("STRING", model.getInputFields().get(1).getType());
        assertNotNull(model.getParams());
        assertEquals("value", model.getParams().get("param"));
    }

    @Test
    public void testConvertToModelAddsModelPath() throws Exception {
        String createModelSql = "create model `test_model` (id bigint, name string) with ('param'='value')";
        ModelConfig model = ModelEntityConverter.convertToModel(createModelSql);

        assertNotNull(model);
        assertTrue(model.getParams().containsKey(ModelConfigs.MODEL_PATH.getKey()), "model_path should be added to params");
        assertNotNull(model.getParams().get(ModelConfigs.MODEL_PATH.getKey()), "model_path value should not be null");
        assertTrue(model.getParams().size() == 2, "params should contain original param and model_path");
        assertTrue(model.getDdl().contains(ModelConfigs.MODEL_PATH.getKey()), "DDL should contain model_path parameter");
    }
}
