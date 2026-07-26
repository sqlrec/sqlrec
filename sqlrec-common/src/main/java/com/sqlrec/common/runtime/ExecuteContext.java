package com.sqlrec.common.runtime;

public interface ExecuteContext extends ReadonlyContext {
    void setVariable(String key, String value);

    void setMetricsTag(String key, String value);

    void setTraceContext(Object context);
}
