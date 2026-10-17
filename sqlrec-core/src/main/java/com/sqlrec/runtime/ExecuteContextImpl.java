package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.model.ModelControllerFactory;
import com.sqlrec.model.ServiceManager;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ExecuteContextImpl implements ExecuteContext {
    private Map<String, String> variableMap;
    private List<String> funNameStack;
    private Object traceContext;
    private FunctionReturnState functionReturnState;

    private final ExecuteContextImpl parent;
    private volatile boolean cancelled = false;
    private final CalciteSchema executionSchema;

    public ExecuteContextImpl(CalciteSchema schema) {
        executionSchema = Objects.requireNonNull(schema, "schema");
        variableMap = new ConcurrentHashMap<>();
        variableMap.put(Consts.LOG_ID, UUID.randomUUID().toString());
        funNameStack = new ArrayList<>();
        functionReturnState = null;
        parent = null;
    }

    public ExecuteContextImpl(ExecuteContextImpl parentContext) {
        this(parentContext, parentContext.executionSchema);
    }

    private ExecuteContextImpl(ExecuteContextImpl parentContext, CalciteSchema schema) {
        variableMap = parentContext.variableMap;
        funNameStack = new ArrayList<>(parentContext.funNameStack);
        traceContext = parentContext.traceContext;
        functionReturnState = parentContext.functionReturnState;
        executionSchema = Objects.requireNonNull(schema, "schema");
        parent = parentContext;
    }

    private ExecuteContextImpl(ExecuteContextImpl parentContext, boolean newFunctionFrame) {
        this(parentContext);
        if (newFunctionFrame) {
            functionReturnState = new FunctionReturnState();
        }
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

    public ExecuteContextImpl createFunctionContext() {
        return new ExecuteContextImpl(this, true);
    }

    public ExecuteContextImpl createIsolatedReturnContext() {
        return new ExecuteContextImpl(this, true);
    }

    public void commitFunctionReturnFrom(ExecuteContextImpl isolatedContext) {
        if (isolatedContext == null || isolatedContext.functionReturnState == null) {
            throw new IllegalArgumentException("isolated return context is not available");
        }
        if (isolatedContext.functionReturnState == functionReturnState) {
            throw new IllegalArgumentException("return context is not isolated");
        }
        if (isolatedContext.functionReturnState.hasReturned()) {
            returnFromFunction(isolatedContext.functionReturnState.getResult());
        }
    }

    public void returnFromFunction(Enumerable<Object[]> result) {
        if (functionReturnState == null) {
            throw new RuntimeException("return statement must execute inside a SQL function");
        }
        functionReturnState.complete(result);
    }

    public boolean hasReturnedFromFunction() {
        return functionReturnState != null && functionReturnState.hasReturned();
    }

    public Enumerable<Object[]> getFunctionReturnResult() {
        if (functionReturnState == null) {
            throw new IllegalStateException("SQL function return state is not available");
        }
        return functionReturnState.getResult();
    }

    @Override
    public void cancel() {
        // Publication checks cancellation under the same schema monitor.
        // Isolated function/partition schemas do not block one another.
        synchronized (executionSchema) {
            cancelled = true;
        }
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

    /** Enter an isolated function/partition schema while retaining ancestor cancellation. */
    ExecuteContextImpl clone(CalciteSchema schema) {
        return new ExecuteContextImpl(this, schema);
    }

    private static class FunctionReturnState {
        private volatile boolean returned;
        private Enumerable<Object[]> result;

        private synchronized void complete(Enumerable<Object[]> result) {
            if (returned) {
                throw new IllegalStateException("SQL function has already returned");
            }
            this.result = result;
            returned = true;
        }

        private boolean hasReturned() {
            return returned;
        }

        private Enumerable<Object[]> getResult() {
            if (!returned) {
                throw new IllegalStateException("SQL function has not returned");
            }
            return result;
        }
    }
}
