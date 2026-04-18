package com.sqlrec.frontend.RestService;

import java.util.List;
import java.util.Map;

public class RequestData {
    private Map<String, List<Map<String, Object>>> inputs;
    private List<String> sqls;
    private Map<String, String> params;
    private Map<String, String> metricTags;

    public Map<String, List<Map<String, Object>>> getInputs() {
        return inputs;
    }

    public void setInputs(Map<String, List<Map<String, Object>>> inputs) {
        this.inputs = inputs;
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
