package com.sqlrec.frontend.RestService;

import java.util.List;
import java.util.Map;

public class ExecuteData {
    private String msg;
    private List<Map<String, Object>> data;

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
}
