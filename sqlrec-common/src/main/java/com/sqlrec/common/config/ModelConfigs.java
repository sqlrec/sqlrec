package com.sqlrec.common.config;

public class ModelConfigs {
    public static final ConfigOption<String> MODEL = new ConfigOption<>(
            "model",
            null, "Model name", null, String.class);

    public static final ConfigOption<String> MODEL_BASE_PATH = new ConfigOption<>(
            "MODEL_BASE_PATH",
            "/user/sqlrec/models", "Model base path", null, String.class);

    public static final ConfigOption<String> JAVA_HOME = new ConfigOption<>(
            "JAVA_HOME",
            null, "Java home directory", null, String.class);

    public static final ConfigOption<String> HADOOP_HOME = new ConfigOption<>(
            "HADOOP_HOME",
            null, "Hadoop home directory", null, String.class);

    public static final ConfigOption<String> CLASSPATH = new ConfigOption<>(
            "CLASSPATH",
            null, "Classpath", null, String.class);

    public static final ConfigOption<String> HADOOP_CONF_DIR = new ConfigOption<>(
            "HADOOP_CONF_DIR",
            null, "Hadoop configuration directory", null, String.class);

    public static final ConfigOption<String> CLIENT_DIR = new ConfigOption<>(
            "CLIENT_DIR",
            null, "Client directory", null, String.class);

    public static final ConfigOption<String> CLIENT_PV_NAME = new ConfigOption<>(
            "CLIENT_PV_NAME",
            null, "Client PV name", null, String.class);

    public static final ConfigOption<String> CLIENT_PVC_NAME = new ConfigOption<>(
            "CLIENT_PVC_NAME",
            null, "Client PVC name", null, String.class);

    public static final ConfigOption<String> NAMESPACE = new ConfigOption<>(
            "NAMESPACE",
            null, "Namespace", null, String.class);
}
