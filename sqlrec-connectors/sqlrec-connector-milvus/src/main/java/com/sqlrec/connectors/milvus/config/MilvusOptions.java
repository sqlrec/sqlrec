package com.sqlrec.connectors.milvus.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Map;

public class MilvusOptions {
    public static final String CONNECTOR_IDENTIFIER = "milvus";

    public static final ConfigOption<String> URL = new ConfigOption<>(
            "url",
            null,
            "Milvus server URL");
    public static final ConfigOption<String> TOKEN = new ConfigOption<>(
            "token",
            null,
            "Milvus server token");
    public static final ConfigOption<String> DATABASE = new ConfigOption<>(
            "database",
            "default",
            "Milvus database name");
    public static final ConfigOption<String> COLLECTION = new ConfigOption<>(
            "collection",
            null,
            "Milvus collection name");

    public static MilvusConfig getMilvusConfig(Map<String, String> options) {
        MilvusConfig milvusConfig = new MilvusConfig();

        if (!options.containsKey(URL.getKey())) {
            throw new RuntimeException("Milvus URL is not set");
        }
        milvusConfig.url = options.get(URL.getKey());

        if (!options.containsKey(TOKEN.getKey())) {
            throw new RuntimeException("Milvus token is not set");
        }
        milvusConfig.token = options.get(TOKEN.getKey());

        if (!options.containsKey(DATABASE.getKey())) {
            milvusConfig.database = DATABASE.getDefaultValue();
        } else {
            milvusConfig.database = options.get(DATABASE.getKey());
        }

        if (!options.containsKey(COLLECTION.getKey())) {
            throw new RuntimeException("Milvus collection is not set");
        }
        milvusConfig.collection = options.get(COLLECTION.getKey());

        return milvusConfig;
    }
}
