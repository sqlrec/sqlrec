package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.model.ModelControllerFactory;
import com.sqlrec.model.ServiceManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ExecuteContextImpl implements ExecuteContext {
    private Map<String, String> variableMap;
    private Map<String, String> metricsTagMap;
    private List<String> funNameStack;
    private Object traceContext;

    private final ExecuteContextImpl parent;
    private volatile boolean cancelled = false;

    public ExecuteContextImpl() {
        variableMap = new ConcurrentHashMap<>();
        variableMap.put(Consts.LOG_ID, UUID.randomUUID().toString());
        metricsTagMap = new ConcurrentHashMap<>();
        funNameStack = new ArrayList<>();
        parent = null;
    }

    public ExecuteContextImpl(ExecuteContextImpl parentContext) {
        variableMap = parentContext.variableMap;
        metricsTagMap = parentContext.metricsTagMap;
        funNameStack = new ArrayList<>(parentContext.funNameStack);
        traceContext = parentContext.traceContext;
        parent = parentContext;
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
    public Map<String, String> getVariables() {
        return variableMap;
    }

    @Override
    public void setMetricsTag(String key, String value) {
        metricsTagMap.put(key, value);
    }

    @Override
    public Map<String, String> getMetricsTags() {
        return metricsTagMap;
    }

    @Override
    public String getLogId() {
        return variableMap.get(Consts.LOG_ID);
    }

    @Override
    public void setTraceContext(Object context) {
        this.traceContext = context;
    }

    @Override
    public Object getTraceContext() {
        return this.traceContext;
    }

    @Override
    public ServiceConf getServiceConfig(String serviceName) {
        return ServiceManager.getServiceConfig(serviceName);
    }

    @Override
    public ModelController getModelController(ModelConf modelConfig) {
        return ModelControllerFactory.getModelController(modelConfig);
    }

    public void addFunNameToStack(String funName) {
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
    public void cancel() {
        this.cancelled = true;
    }

    @Override
    public boolean isCancelled() {
        ExecuteContextImpl current = this;
        while (current != null) {
            if (current.cancelled) {
                return true;
            }
            current = current.parent;
        }
        return false;
    }

    @Override
    public ExecuteContextImpl clone() {
        return new ExecuteContextImpl(this);
    }
}
