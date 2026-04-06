package com.sqlrec.runtime;

import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.model.ModelControllerFactory;
import com.sqlrec.model.ServiceManager;

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

    public void addFunNameToStack(String funName) {
        funName = funName.toUpperCase();
        if (funNameStack.contains(funName)) {
            throw new RuntimeException("Circular dependency detected: " + funName +
                    " in stack: " + String.join("->", funNameStack));
        }
        funNameStack.add(funName);
    }

    public void popFunNameFromStack() {
        funNameStack.removeLast();
    }

    public List<String> getFunNameStack() {
        return funNameStack;
    }

    @Override
    public ServiceConfig getServiceConfig(String serviceName) {
        return ServiceManager.getServiceConfig(serviceName);
    }

    @Override
    public ModelController getModelController(ModelConfig modelConfig) {
        return ModelControllerFactory.getModelController(modelConfig);
    }

    @Override
    public ExecuteContextImpl clone() {
        return new ExecuteContextImpl(this);
    }
}
