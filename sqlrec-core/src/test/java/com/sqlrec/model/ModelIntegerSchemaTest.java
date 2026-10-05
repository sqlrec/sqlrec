package com.sqlrec.model;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.model.gbdt.CatBoostModel;
import com.sqlrec.model.tzrec.PipelineConfigUtils;
import com.sqlrec.model.tzrec.WideAndDeepModel;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlRecSqlParser;
import com.sqlrec.utils.SchemaUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ModelIntegerSchemaTest {
    @Test
    void sqlIntSurvivesParsingAndProducesDirectTzrecBuckets() throws Exception {
        ModelConf model = parse("tzrec.wide_and_deep");
        assertEquals("INTEGER", model.getInputFields().get(0).getType());
        assertNull(new WideAndDeepModel().checkModel(model));
        String features = PipelineConfigUtils.generateFeatureConfigs(model);
        assertTrue(features.contains("num_buckets: 100"), features);
        assertFalse(features.contains("hash_bucket_size"), features);
    }

    @Test
    void sqlIntSurvivesParsingAndIsAcceptedByCatBoost() throws Exception {
        assertNull(new CatBoostModel().checkModel(parse("gbdt.catboost")));
    }

    private ModelConf parse(String framework) throws Exception {
        SqlCreateModel statement = (SqlCreateModel) SqlRecSqlParser.parse("""
                CREATE MODEL model_int (uid INT, label INT)
                WITH ('model'='%s', 'label_columns'='label', 'num_buckets'='100')
                """.formatted(framework));
        ModelConf model = new ModelConf();
        model.setInputFields(SchemaUtils.convertFieldList(statement.getFieldList()));
        model.setParams(SchemaUtils.convertPropertyList(statement.getPropertyList()));
        return model;
    }
}
