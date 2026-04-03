package com.sqlrec.udf.scalar;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.UUID;

public class UuidFunction extends ScalarFunction {
    public String eval() {
        return UUID.randomUUID().toString();
    }
}
