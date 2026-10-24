package com.sqlrec.udf.scalar;

import com.sqlrec.udf.llm.LlmClient;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.List;

@UDFType(deterministic = false)
public class LlmEmbedTextFunction extends RemoteLlmFunction {
    public List<Double> evaluate(String endpoint, String apiKey, String model, String text, String optionsJson) {
        return LlmClient.embedText(endpoint, apiKey, model, text, optionsJson);
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
