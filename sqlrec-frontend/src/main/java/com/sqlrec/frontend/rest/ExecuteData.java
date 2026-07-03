package com.sqlrec.frontend.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecuteData {
    private String msg;
    private List<Map<String, Object>> data;
    private Map<String, String> params;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = new HashMap<>(params);
    }
}
