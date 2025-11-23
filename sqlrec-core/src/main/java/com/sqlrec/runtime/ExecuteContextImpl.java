package com.sqlrec.runtime;

import com.sqlrec.common.schema.ExecuteContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExecuteContextImpl implements ExecuteContext {
    private Map<String, String> variableMap;
    private List<String> funNameStack;

    public ExecuteContextImpl() {
        variableMap = new ConcurrentHashMap<>();
        funNameStack = new java.util.ArrayList<>();
    }

    public ExecuteContextImpl(ExecuteContextImpl parentContext) {
        variableMap = parentContext.variableMap;
        funNameStack = new java.util.ArrayList<>(parentContext.funNameStack);
    }

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

    @Override
    public void addFunNameToStack(String funName) {
        funName = funName.toUpperCase();
        if (funNameStack.contains(funName)) {
            throw new RuntimeException("Circular dependency detected: " + funName +
                    " in stack: " + String.join("->", funNameStack));
        }
        funNameStack.add(funName);
    }

    @Override
    public void popFunNameFromStack() {
        funNameStack.removeLast();
    }

    @Override
    public List<String> getFunNameStack() {
        return funNameStack;
    }

    @Override
    public ExecuteContextImpl clone() {
        return new ExecuteContextImpl(this);
    }
}
