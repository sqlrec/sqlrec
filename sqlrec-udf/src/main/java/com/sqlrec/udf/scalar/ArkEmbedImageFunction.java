package com.sqlrec.udf.scalar;

import com.sqlrec.udf.llm.LlmClient;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.List;

@UDFType(deterministic = false)
public class ArkEmbedImageFunction extends RemoteLlmFunction {
    public List<Double> evaluate(String endpoint, String apiKey, String model, String image, String optionsJson) {
        return LlmClient.embedImage(endpoint, apiKey, model, image, optionsJson);
    }

    @Override
    protected boolean returnsVector() {
        return true;
    }

    @Override
    protected Object invoke(String endpoint, String apiKey, String model, String input, String options) {
        return evaluate(endpoint, apiKey, model, input, options);
    }
}
