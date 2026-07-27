package com.sqlrec.common.rest;

import java.util.List;
import java.util.Map;

/**
 * Request payload contract for sqlrec REST APIs.
 *
 * <p>{@code data} holds input tables keyed by table name (each table is a list of row maps);
 * {@code sqls} holds SQL statements for the {@code /sql/v1} endpoint;
 * {@code params} and {@code metricTags} carry execution variables and metric tags.
 *
 * <p>This class is part of the external interface protocol shared between the
 * frontend REST layer and remote callers (e.g. {@code call_sqlrec_api} UDF).
 */
public class RequestData {
    private Map<String, List<Map<String, Object>>> data;
    private List<String> sqls;
    private Map<String, String> params;
    private Map<String, String> metricTags;

    public Map<String, List<Map<String, Object>>> getData() {
        return data;
    }

    public void setData(Map<String, List<Map<String, Object>>> data) {
        this.data = data;
    }

    public List<String> getSqls() {
        return sqls;
    }

    public void setSqls(List<String> sqls) {
        this.sqls = sqls;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public Map<String, String> getMetricTags() {
        return metricTags;
    }

    public void setMetricTags(Map<String, String> metricTags) {
        this.metricTags = metricTags;
    }
}
