package com.sqlrec.common.model;

import java.util.Map;

public class ServiceConfig {
    public String id;
    public String serviceName;
    public String modelName;
    public ModelConfig modelConfig;
    public String checkpointName;
    public String modelCheckpointDir;
    public Map<String, String> params;
    public String url;
}
