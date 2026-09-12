package com.sqlrec.common.runtime;

import org.apache.calcite.DataContext;

import java.util.Map;

public interface SqlRecDataContext extends DataContext {
    String getVariable(String key);

    Map<String, String> getVariables();
}
