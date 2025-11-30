package com.sqlrec.common.config;

import com.sqlrec.common.udf.scalar.IpFunction;
import com.sqlrec.common.udf.scalar.L2NormFunction;
import com.sqlrec.common.udf.scalar.UuidFunction;
import com.sqlrec.common.udf.table.AddColFunction;
import com.sqlrec.common.udf.table.DedupFunction;
import com.sqlrec.common.udf.table.ShuffleFunction;
import com.sqlrec.common.udf.table.WindowDiversify;

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
    }};
}
