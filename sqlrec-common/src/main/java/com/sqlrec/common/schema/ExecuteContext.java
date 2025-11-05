package com.sqlrec.common.schema;

public interface ExecuteContext {
    String getVariable(String key);

    void setVariable(String key, String value);
}
