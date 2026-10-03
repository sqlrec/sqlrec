package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigUtilsTest {
    @Test
    void featureOverridesKeepDefaultValidationOrderAndNumericFeaturesSkipIt() {
        ModelConf model = new ModelConf();
        model.setInputFields(List.of(new FieldSchema("category", "string")));
        Map<String, String> params = new LinkedHashMap<>(Map.of(
                "num_buckets", "bad-buckets", "embedding_dim", "bad-embedding",
                "column.category.bucket_size", "11", "column.category.embedding_dim", "12"));
        model.setParams(params);

        assertTrue(assertThrows(NumberFormatException.class,
                () -> PipelineConfigUtils.generateFeatureConfigs(model)).getMessage().contains("bad-buckets"));
        params.put("num_buckets", "10");
        params.put("column.category.bucket_size", "bad-override");
        assertTrue(assertThrows(NumberFormatException.class,
                () -> PipelineConfigUtils.generateFeatureConfigs(model)).getMessage().contains("bad-embedding"));
        params.put("embedding_dim", "16");
        assertTrue(assertThrows(NumberFormatException.class,
                () -> PipelineConfigUtils.generateFeatureConfigs(model)).getMessage().contains("bad-override"));

        model.setInputFields(List.of(new FieldSchema("score", "FLOAT")));
        assertEquals("", PipelineConfigUtils.generateFeatureConfigs(model));
    }
}
