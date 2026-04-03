package com.sqlrec.udf.scalar;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.List;

public class IpFunction extends ScalarFunction {
    public Object eval(Object emb1, Object emb2) {
        if (emb1 == null || emb2 == null) {
            return null;
        }

        if (!(emb1 instanceof List) || !(emb2 instanceof List)) {
            throw new IllegalArgumentException("emb1 and emb2 must be list");
        }

        List<Object> emb1Array = (List<Object>) emb1;
        List<Object> emb2Array = (List<Object>) emb2;

        if (emb1Array.size() != emb2Array.size()) {
            throw new IllegalArgumentException("emb1 and emb2 must have same length");
        }

        float ip = 0.0f;
        for (int i = 0; i < emb1Array.size(); i++) {
            if (!(emb1Array.get(i) instanceof Number) || !(emb2Array.get(i) instanceof Number)) {
                throw new IllegalArgumentException("emb1 and emb2 must be array of number");
            }
            ip += ((Number) emb1Array.get(i)).floatValue() * ((Number) emb2Array.get(i)).floatValue();
        }
        return ip;
    }
}
