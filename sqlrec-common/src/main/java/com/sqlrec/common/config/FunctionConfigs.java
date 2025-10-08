package com.sqlrec.common.config;

import com.sqlrec.common.udf.scalar.IpFunction;
import com.sqlrec.common.udf.scalar.L2NormFunction;
import com.sqlrec.common.udf.scalar.UuidFunction;

import java.util.HashMap;
import java.util.Map;

public class FunctionConfigs {
    public static final Map<String, String> DEFAULT_SCALAR_FUNCTION_CONFIGS = new HashMap<String, String>() {{
        put("ip", IpFunction.class.getName());
        put("l2_norm", L2NormFunction.class.getName());
        put("uuid", UuidFunction.class.getName());
    }};
}
