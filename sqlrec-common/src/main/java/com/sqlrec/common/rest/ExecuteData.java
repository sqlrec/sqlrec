package com.sqlrec.common.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response payload contract for a single sqlrec REST API execution.
 *
 * <p>{@code msg} carries a status/error message;
 * {@code data} holds the result rows (each row is a column-name to value map);
 * {@code params} echoes the execution variables after the call.
 *
 * <p>This class is part of the external interface protocol shared between the
 * frontend REST layer and remote callers (e.g. {@code call_sqlrec_api} UDF).
 */
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
