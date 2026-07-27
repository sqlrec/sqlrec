package com.sqlrec.common.rest;

import java.util.List;

/**
 * Response payload contract for the {@code /sql/v1} endpoint, wrapping the
 * {@link ExecuteData} result of each executed SQL statement.
 *
 * <p>This class is part of the external interface protocol shared between the
 * frontend REST layer and remote callers.
 */
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
