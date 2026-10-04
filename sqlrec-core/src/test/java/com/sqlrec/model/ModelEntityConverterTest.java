package com.sqlrec.model;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.model.tzrec.PipelineConfigUtils;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.sql.parser.SqlCreateModel;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Model/service names are normalized (lower case, back-quotes stripped) when
 * extracted from DDL, so the stored name never keeps quoting or casing.
 */
class ModelEntityConverterTest {

    @Test
    void multiTargetModelUsesExistingSqlSyntaxAndRegistersOrderedOutputs() throws Exception {
        ModelConf model = ModelManager.getAndCheckModel((SqlCreateModel) CompileManager.parseSql(
                "create model multi_rank (uid bigint, price double, click int, `like` int, watch_time double) "
                + "with ('model'='tzrec.mmoe', 'label_columns'='click,like,watch_time', "
                + "'task.like.weight'='2', 'task.watch_time.type'='regression', 'task.watch_time.weight'='0.1')"));
        assertEquals(List.of("probs_click", "probs_like", "y_watch_time"), ModelControllerFactory.getRequiredModelController(model)
                .getOutputFields(model).stream().map(field -> field.getName()).collect(java.util.stream.Collectors.toList()));
        String features = PipelineConfigUtils.generateFeatureConfigs(model);
        assertTrue(features.contains("feature_name: \"price\""));
        org.junit.jupiter.api.Assertions.assertFalse(features.contains("feature_name: \"click\""));
    }

    @Test
    void tzrecAcceptsIntegerAndArrayTypesRenderedByTheSqlParser() throws Exception {
        for (String architecture : List.of("tzrec.wide_and_deep", "tzrec.deepfm", "tzrec.dssm")) {
            ModelConf model = ModelManager.getAndCheckModel((SqlCreateModel) CompileManager.parseSql(
                    "create model array_features (uid int, genres array<string>, vector array<float>) "
                    + "with ('model'='" + architecture + "', 'label_columns'='rating', "
                    + "'column.vector.value_dim'='2', 'item_features'='genres,vector')"));
            // The parser inserts spaces inside ARRAY<...>; these are ordinary supported types.
            assertEquals(3, model.getInputFields().size());
            assertTrue(PipelineConfigUtils.generateFeatureConfigs(model).contains("raw_feature {"));
        }
    }

    @Test
    void convertToModelNormalizesModelName() throws Exception {
        SqlNode node = CompileManager.parseSql("create model MyModel (uid int)");

        ModelConf modelConf = ModelEntityConverter.convertToModel((SqlCreateModel) node);

        assertEquals("mymodel", modelConf.getModelName());
    }

    @Test
    void convertToModelStripsBackQuotesAndNormalizes() throws Exception {
        SqlNode node = CompileManager.parseSql("create model `MyModel` (uid int)");

        ModelConf modelConf = ModelEntityConverter.convertToModel((SqlCreateModel) node);

        assertEquals("mymodel", modelConf.getModelName());
    }
}
