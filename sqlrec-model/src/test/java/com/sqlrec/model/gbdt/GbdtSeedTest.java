package com.sqlrec.model.gbdt;
import com.google.gson.JsonParser;
import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class GbdtSeedTest {
    @Test
    void explicitSeedUsesOperationOverrideAndPreservesLegacyConfigWhenUnset() {
        ModelConf model = new ModelConf();
        model.setInputFields(List.of(new FieldSchema("feature", "FLOAT"), new FieldSchema("label", "INT")));
        model.setParams(Map.of("label_columns", "label", "random_seed", "123"));
        ModelTrainConf train = new ModelTrainConf();
        train.setTrainDataPaths(List.of("/data/train.parquet"));
        train.setModelDir("/model");
        train.setParams(Map.of("random_seed", "789"));
        for (PipelineConfigUtils.ModelType type : PipelineConfigUtils.ModelType.values()) {
            var params = JsonParser.parseString(PipelineConfigUtils.generateTrainConfig(type, model, train)).getAsJsonObject().getAsJsonObject("params");
            assertEquals(789, params.get("random_seed").getAsInt());
            train.setParams(Map.of("random_seed", "-1"));
            assertThrows(IllegalArgumentException.class, () -> PipelineConfigUtils.generateTrainConfig(type, model, train));
            train.setParams(Map.of("random_seed", "789"));
        }
    }
}
