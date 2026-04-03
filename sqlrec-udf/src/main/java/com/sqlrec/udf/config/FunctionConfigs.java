package com.sqlrec.udf.config;

import com.sqlrec.udf.scalar.IpFunction;
import com.sqlrec.udf.scalar.L2NormFunction;
import com.sqlrec.udf.scalar.UuidFunction;
import com.sqlrec.udf.table.*;

import java.util.HashMap;
import java.util.Map;

public class FunctionConfigs {
    public static final Map<String, String> DEFAULT_SCALAR_FUNCTION_CONFIGS = new HashMap<String, String>() {{
        put("ip", IpFunction.class.getName());
        put("l2_norm", L2NormFunction.class.getName());
        put("uuid", UuidFunction.class.getName());
    }};

    public static final Map<String, String> DEFAULT_JAVA_FUNCTION_CONFIGS = new HashMap<String, String>() {{
        put("add_col", AddColFunction.class.getName());
        put("shuffle", ShuffleFunction.class.getName());
        put("window_diversify", WindowDiversify.class.getName());
        put("dedup", DedupFunction.class.getName());
        put("call_service", CallServiceFunction.class.getName());
        put("call_service_with_qv", CallServiceWithQVFunction.class.getName());
    }};
}
