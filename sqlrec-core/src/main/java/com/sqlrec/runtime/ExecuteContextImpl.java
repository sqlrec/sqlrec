package com.sqlrec.runtime;

import com.sqlrec.common.schema.ExecuteContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExecuteContextImpl implements ExecuteContext {
    private Map<String, String> variableMap = new ConcurrentHashMap<>();

    public String getVariable(String key) {
        return variableMap.get(key);
    }

    public void setVariable(String key, String value) {
        if (value == null) {
            variableMap.remove(key);
        } else {
            variableMap.put(key, value);
        }
    }
}
