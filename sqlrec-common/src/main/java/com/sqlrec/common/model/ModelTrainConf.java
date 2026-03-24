package com.sqlrec.common.model;

import java.util.List;
import java.util.Map;

public class ModelTrainConf {
    public String id;
    public String modelName;
    public String checkpointName;
    public String modelDir;
    public String baseModelDir;
    public List<String> trainDataPaths;
    public Map<String, String> params;
}
