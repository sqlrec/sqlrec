package com.sqlrec.common.utils;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.commons.lang3.StringUtils;

public class ExecEnv {
    public static boolean isFileSystemMeta() {
        return StringUtils.isNotEmpty(SqlRecConfigs.SQL_SCHEMA_DIR.getValue());
    }
}
