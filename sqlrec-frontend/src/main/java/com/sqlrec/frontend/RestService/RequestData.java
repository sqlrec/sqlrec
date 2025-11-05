package com.sqlrec.frontend.RestService;

import java.util.List;
import java.util.Map;

public class RequestData {
    public Map<String, List<Map<String, Object>>> inputs;
    public List<String> sqls;
    public Map<String, String> params;
}
