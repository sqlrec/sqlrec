package com.sqlrec.common.runtime;

import org.apache.calcite.DataContext;

public interface SqlRecDataContext extends DataContext {
    String getVariable(String key);
}
