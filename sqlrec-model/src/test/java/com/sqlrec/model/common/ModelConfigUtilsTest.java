package com.sqlrec.model.common;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ModelConfigUtilsTest {

    @Test
    void mergesParametersWithOverridesTakingPrecedence() {
        Map<String, String> merged = ModelConfigUtils.mergeParams(
                Map.of("model", "base", "shared", "base"),
                Map.of("shared", "override", "service", "value")
        );

        assertEquals("base", merged.get("model"));
        assertEquals("override", merged.get("shared"));
        assertEquals("value", merged.get("service"));
    }

    @Test
    void prettyJsonKeepsUnicodeAndIncludesTrailingNewline() {
        JsonObject config = new JsonObject();
        config.addProperty("prompt", "推荐<商品>");

        assertEquals("{\n  \"prompt\": \"推荐<商品>\"\n}\n", ModelConfigUtils.toPrettyJson(config));
    }
}
