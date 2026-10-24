package com.sqlrec.udf.scalar;

import com.sqlrec.udf.llm.LlmClient;
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(deterministic = false)
public class LlmCompleteFunction extends RemoteLlmFunction {
    public String evaluate(String endpoint, String apiKey, String model, String prompt, String optionsJson) {
        return LlmClient.complete(endpoint, apiKey, model, prompt, optionsJson);
    }

    @Override
    protected boolean returnsVector() {
        return false;
    }

    @Override
    protected Object invoke(String endpoint, String apiKey, String model, String input, String options) {
        return evaluate(endpoint, apiKey, model, input, options);
    }
}
