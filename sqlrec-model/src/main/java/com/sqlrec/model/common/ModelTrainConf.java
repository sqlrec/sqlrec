package com.sqlrec.model.common;

import java.util.List;
import java.util.Map;

public class ModelTrainConf {
    public String name;
    public String modelDir;
    public String baseModelDir;
    public List<String> trainDataPaths;
    public Map<String, String> params;
}
