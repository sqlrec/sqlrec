package com.sqlrec.model;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelTrainConf;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlTrainModel;
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
        assertNotNull(model.fieldSchemas);
        assertEquals(2, model.fieldSchemas.size());
        assertEquals("id", model.fieldSchemas.get(0).name);
        assertEquals("BIGINT", model.fieldSchemas.get(0).type);
        assertEquals("name", model.fieldSchemas.get(1).name);
        assertEquals("STRING", model.fieldSchemas.get(1).type);
        assertNotNull(model.params);
        assertEquals(1, model.params.size());
        assertEquals("value", model.params.get("param"));
    }

    @Test
    public void testConvertToModelTrainConf() throws Exception {
        String trainModelSql = "train model test_model checkpoint='checkpoint_path' on data_db.test_table where dt>='2023-01-01' and dt < '2023-02-01' WITH ( 'param1' = 'value1', 'param2' = 'value2' )";
        SqlNode sqlNode = CompileManager.parseFlinkSql(trainModelSql);

        assertTrue(sqlNode instanceof SqlTrainModel);
        SqlTrainModel sqlTrainModel = (SqlTrainModel) sqlNode;

        ModelTrainConf modelTrainConf = ModelEntityConverter.convertToModelTrainConf(sqlTrainModel);

        assertNotNull(modelTrainConf);
        assertEquals("test_model", modelTrainConf.name);
        assertNotNull(modelTrainConf.params);
        assertEquals(2, modelTrainConf.params.size());
        assertEquals("value1", modelTrainConf.params.get("param1"));
        assertEquals("value2", modelTrainConf.params.get("param2"));
    }
}
