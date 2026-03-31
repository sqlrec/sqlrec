package com.sqlrec.model.external;

import com.sqlrec.common.config.ConfigOption;

public class Config {
    public static final ConfigOption<String> URL = new ConfigOption<>(
            "url",
            null, "External model service URL", null, String.class);
    public static final ConfigOption<String> OUTPUT_COLUMNS = new ConfigOption<>(
            "output_columns",
            null, "Output columns in format: name1:type1,name2:type2", null, String.class);
}
