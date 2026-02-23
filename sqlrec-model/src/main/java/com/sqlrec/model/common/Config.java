package com.sqlrec.model.common;

import com.sqlrec.common.config.ConfigOption;

public class Config {
    public static final ConfigOption<String> MODEL = new ConfigOption<>(
            "model",
            null, "Model name", null, String.class);
}
