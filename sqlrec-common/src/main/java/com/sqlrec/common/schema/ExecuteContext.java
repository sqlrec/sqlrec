package com.sqlrec.common.schema;

import java.util.List;

public interface ExecuteContext {
    String getVariable(String key);

    void setVariable(String key, String value);

    void addFunNameToStack(String funName);

    void popFunNameFromStack();

    List<String> getFunNameStack();

    ExecuteContext clone();
}
