package com.sqlrec.udf.scalar;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.UUID;

public class UuidFunction extends UDF {
    public String evaluate() {
        return UUID.randomUUID().toString();
    }
}
