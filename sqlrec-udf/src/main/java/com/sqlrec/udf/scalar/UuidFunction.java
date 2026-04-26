package com.sqlrec.udf.scalar;

import java.util.UUID;

public class UuidFunction {
    public String eval() {
        return UUID.randomUUID().toString();
    }
}
