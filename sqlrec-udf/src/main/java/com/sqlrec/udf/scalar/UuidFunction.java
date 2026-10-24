package com.sqlrec.udf.scalar;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.UUID;

@UDFType(deterministic = false)
public class UuidFunction extends UDF {
    public String evaluate() {
        return UUID.randomUUID().toString();
    }
}
