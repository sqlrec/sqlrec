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
