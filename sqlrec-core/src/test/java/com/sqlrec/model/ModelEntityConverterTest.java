package com.sqlrec.model;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Model;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.model.tzrec.PipelineConfigUtils;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlCreateService;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Model names are normalized; new Service names must already be valid lowercase
 * Kubernetes Service DNS labels. Quoting is removed when extracting identifiers.
 */
class ModelEntityConverterTest {

    @Test
    void serviceIdIsTheExactSqlNameAndInvalidQuotedNamesAreRejected() throws Exception {
        var service = ModelEntityConverter.convertToServiceConf((SqlCreateService) CompileManager.parseSql(
                "CREATE SERVICE `rank-service` ON MODEL `rank`"));
        assertEquals("rank-service", service.getServiceName());
        assertEquals("rank-service", service.getId());
        for (String name : List.of("Rank-Service", "RANK", "rank_service", "rank.service", " rank-service", "rank-service ")) {
            var sql = (SqlCreateService) CompileManager.parseSql("CREATE SERVICE `" + name + "` ON MODEL `rank`");
            assertThrows(IllegalArgumentException.class, () -> ModelEntityConverter.convertToServiceConf(sql));
        }
        var unquoted = (SqlCreateService) CompileManager.parseSql("CREATE SERVICE RANKING ON MODEL `rank`");
        assertThrows(IllegalArgumentException.class, () -> ModelEntityConverter.convertToServiceConf(unquoted));
    }

    @Test
    void trainingSqlRejectsTraversalButSupportsNestedCheckpointNames() throws Exception {
        MetadataAccess db = mock(MetadataAccess.class);
        Model model = new Model();
        model.setDdl("CREATE MODEL `rank` (x FLOAT) WITH ('MODEL_PATH'='/models/rank')");
        when(db.getModel("rank")).thenReturn(model);
        try (var metadata = mockStatic(MetadataAccessFactory.class)) {
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            for (String name : List.of("../other/v1", ".", "v1/..")) {
                SqlTrainModel sql = (SqlTrainModel) CompileManager.parseSql(
                        "TRAIN MODEL `rank` CHECKPOINT='" + name + "'");
                assertThrows(IllegalArgumentException.class,
                        () -> ModelEntityConverter.convertToModelTrainConf(sql, "default"));
            }
            assertEquals("/models/rank/v1_export/item",
                    ModelEntityConverter.getModelCheckpointPath("rank", "v1_export/item"));
        }
    }

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
