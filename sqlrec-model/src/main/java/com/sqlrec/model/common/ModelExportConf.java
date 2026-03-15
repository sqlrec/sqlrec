package com.sqlrec.model.common;

import java.util.List;
import java.util.Map;

public class ModelExportConf {
    public String id;
    public String modelName;
    public String checkpointName;
    public String baseModelDir;
    public List<String> trainDataPaths;
    public Map<String, String> params;
}
