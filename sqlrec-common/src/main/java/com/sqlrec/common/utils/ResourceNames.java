package com.sqlrec.common.utils;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.Locale;

/**
 * Single entry for sqlrec resource name normalization (sql function, api, model,
 * service, checkpoint). Resource names are case-insensitive and are stored and
 * compared in lower case.
 * <p>
 * Normalization must happen at system boundaries (SQL DDL parsing, REST path
 * parameters, {@code MetadataAccess} facade); downstream code can assume names
 * are already normalized.
 */
public final class ResourceNames {

    private ResourceNames() {
    }

    public static String normalize(String name) {
        return name == null ? null : name.trim().toLowerCase(Locale.ROOT);
    }

    public static String of(SqlIdentifier identifier) {
        return identifier == null ? null : normalize(identifier.getSimple());
    }
}
