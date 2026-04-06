package com.sqlrec.frontend.RestService;

import java.util.List;

public class ExecuteDataList {
    private String msg;
    private List<ExecuteData> data;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<ExecuteData> getData() {
        return data;
    }

    public void setData(List<ExecuteData> data) {
        this.data = data;
    }
}
