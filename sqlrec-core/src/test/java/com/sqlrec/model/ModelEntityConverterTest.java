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
        assertEquals("test_model", model.modelName);
        assertNotNull(model.inputFields);
        assertEquals(2, model.inputFields.size());
        assertEquals("id", model.inputFields.get(0).name);
        assertEquals("BIGINT", model.inputFields.get(0).type);
        assertEquals("name", model.inputFields.get(1).name);
        assertEquals("STRING", model.inputFields.get(1).type);
        assertNotNull(model.params);
        assertEquals("value", model.params.get("param"));
    }

    @Test
    public void testConvertToModelAddsModelPath() throws Exception {
        String createModelSql = "create model `test_model` (id bigint, name string) with ('param'='value')";
        ModelConfig model = ModelEntityConverter.convertToModel(createModelSql);

        assertNotNull(model);
        assertTrue(model.params.containsKey(ModelConfigs.MODEL_PATH.getKey()), "model_path should be added to params");
        assertNotNull(model.params.get(ModelConfigs.MODEL_PATH.getKey()), "model_path value should not be null");
        assertTrue(model.params.size() == 2, "params should contain original param and model_path");
        assertTrue(model.ddl.contains(ModelConfigs.MODEL_PATH.getKey()), "DDL should contain model_path parameter");
    }
}
