package com.sqlrec.common.schema;

public interface TableFunction {
    public CacheTable eval(CacheTable input);
}
