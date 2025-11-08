package com.sqlrec.connectors.milvus.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Map;

public class MilvusOptions {
    public static final String CONNECTOR_IDENTIFIER = "milvus";

    public static final ConfigOption<String> URL = new ConfigOption<>(
            "url",
            null,
            "Milvus server URL",
            null,
            String.class
    );
    public static final ConfigOption<String> TOKEN = new ConfigOption<>(
            "token",
            null,
            "Milvus server token",
            null,
            String.class
    );
    public static final ConfigOption<String> DATABASE = new ConfigOption<>(
            "database",
            "default",
            "Milvus database name",
            null,
            String.class
    );
    public static final ConfigOption<String> COLLECTION = new ConfigOption<>(
            "collection",
            null,
            "Milvus collection name",
            null,
            String.class
    );


    public static MilvusConfig getMilvusConfig(Map<String, String> options) {
        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.url = URL.getValue(options);
        milvusConfig.token = TOKEN.getValue(options);
        milvusConfig.database = DATABASE.getValue(options);
        milvusConfig.collection = COLLECTION.getValue(options);

        return milvusConfig;
    }
}
