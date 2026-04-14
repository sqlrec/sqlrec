package com.sqlrec.common.runtime;

import java.util.Map;

public interface ExecuteContext {
    String getVariable(String key);

    void setVariable(String key, String value);

    Map<String, String> getVariables();
}
