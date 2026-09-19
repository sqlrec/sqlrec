package com.sqlrec.udf.config;

import com.sqlrec.udf.scalar.ArrayContainsAllFunction;
import com.sqlrec.udf.scalar.ArrayContainsAnyFunction;
import com.sqlrec.udf.scalar.ArrayContainsFunction;
import com.sqlrec.udf.scalar.GetFunction;
import com.sqlrec.udf.scalar.GetOrDefaultFunction;
import com.sqlrec.udf.scalar.IpFunction;
import com.sqlrec.udf.scalar.L2NormFunction;
import com.sqlrec.udf.scalar.RandomVecFunction;
import com.sqlrec.udf.scalar.UuidFunction;
import com.sqlrec.udf.table.*;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FunctionConfigs {
    public static final Map<String, String> DEFAULT_SCALAR_FUNCTION_CONFIGS =
            createScalarFunctionConfigs();
    public static final Map<String, String> DEFAULT_JAVA_FUNCTION_CONFIGS =
            createJavaFunctionConfigs();

    private FunctionConfigs() {
    }

    private static Map<String, String> createScalarFunctionConfigs() {
        Map<String, String> configs = new LinkedHashMap<>();
        configs.put("ip", IpFunction.class.getName());
        configs.put("l2_norm", L2NormFunction.class.getName());
        configs.put("random_vec", RandomVecFunction.class.getName());
        configs.put("uuid", UuidFunction.class.getName());
        configs.put("get", GetFunction.class.getName());
        configs.put("get_or_default", GetOrDefaultFunction.class.getName());
        configs.put("array_contains", ArrayContainsFunction.class.getName());
        configs.put("array_contains_all", ArrayContainsAllFunction.class.getName());
        configs.put("array_contains_any", ArrayContainsAnyFunction.class.getName());
        return Collections.unmodifiableMap(configs);
    }

    private static Map<String, String> createJavaFunctionConfigs() {
        Map<String, String> configs = new LinkedHashMap<>();
        configs.put("add_col", AddColFunction.class.getName());
        configs.put("shuffle", ShuffleFunction.class.getName());
        configs.put("window_diversify", WindowDiversify.class.getName());
        configs.put("dedup", DedupFunction.class.getName());
        configs.put("call_service", CallServiceFunction.class.getName());
        configs.put("call_sqlrec_api", CallSqlRecApiFunction.class.getName());
        configs.put("truncate_table", TruncateTableFunction.class.getName());
        configs.put("get_variables", GetVariablesFunction.class.getName());
        configs.put("set_variables", SetVariablesFunction.class.getName());
        configs.put("feature_coverage_metrics", FeatureCoverageMetricsFunction.class.getName());
        configs.put("weighted_merge", WeightedMergeFunction.class.getName());
        configs.put("dpp_diversity", DppDiversity.class.getName());
        configs.put("rule_diversity", RuleDiversity.class.getName());
        configs.put("tag_to_vec", TagToVecFunction.class.getName());
        configs.put("json_to_table", JsonToTableFunction.class.getName());
        configs.put("get_growthbook_features", GetGrowthbookFeaturesFunction.class.getName());
        configs.put("sleep", SleepFunction.class.getName());
        return Collections.unmodifiableMap(configs);
    }
}
