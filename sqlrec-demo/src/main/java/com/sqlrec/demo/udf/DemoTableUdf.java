package com.sqlrec.demo.udf;

import com.sqlrec.common.schema.CacheTable;

public class DemoTableUdf {
    public CacheTable evaluate(CacheTable input) {
        return input;
    }
}
