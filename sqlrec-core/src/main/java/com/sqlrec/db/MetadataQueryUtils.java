package com.sqlrec.db;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.executor.SqlProcessResult;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.sql.parser.dql.SqlShowFunctions;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.functions.SqlLikeUtils;
import org.apache.flink.table.module.CoreModule;

import java.util.*;

/** Shared, storage-independent metadata names and query results. */
public final class MetadataQueryUtils {
    private MetadataQueryUtils() {}

    public static SqlProcessResult describeTable(ResolvedSchema schema) {
        List<String> names = new ArrayList<>(List.of("name", "type", "null", "key", "extras", "watermark"));
        List<String> types = new ArrayList<>(List.of("STRING", "STRING", "BOOLEAN", "STRING", "STRING", "STRING"));
        boolean comments = schema.getColumns().stream().anyMatch(column -> column.getComment().isPresent());
        if (comments) {
            names.add("comment");
            types.add("STRING");
        }
        UniqueConstraint primaryKey = schema.getPrimaryKey().orElse(null);
        String keyDescription = primaryKey == null ? null : "PRI(" + String.join(", ", primaryKey.getColumns()) + ")";
        List<Object[]> rows = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            String key = primaryKey != null && primaryKey.getColumns().contains(column.getName()) ? keyDescription : null;
            List<Object> values = new ArrayList<>(Arrays.asList(column.getName(),
                    column.getDataType().getLogicalType().copy(true).asSummaryString(),
                    column.getDataType().getLogicalType().isNullable(), key, column.explainExtras().orElse(null), null));
            if (comments) values.add(column.getComment().orElse(null));
            rows.add(values.toArray());
        }
        return result(names, types, rows);
    }

    public static SqlProcessResult showFunctions(SqlShowFunctions show, Collection<String> userFunctions) {
        Set<String> functions = new TreeSet<>(userFunctions);
        if (!show.requireUser()) functions.addAll(CoreModule.INSTANCE.listFunctions());
        return SqlProcessResult.stringList(functions.stream().filter(function -> matchesLike(function, show)).toList(),
                "function name");
    }

    private static boolean matchesLike(String function, SqlShowFunctions show) {
        if (!show.isWithLike()) return true;
        boolean matches = "ILIKE".equals(show.getLikeType())
                ? SqlLikeUtils.ilike(function, show.getLikeSqlPattern(), "\\")
                : SqlLikeUtils.like(function, show.getLikeSqlPattern(), "\\");
        return show.isNotLike() ? !matches : matches;
    }

    public static ObjectPath objectPath(String[] parts, String database) {
        return switch (parts.length) {
            case 1 -> new ObjectPath(database, parts[0]);
            case 2 -> new ObjectPath(parts[0], parts[1]);
            case 3 -> { requireCatalog(parts[0]); yield new ObjectPath(parts[1], parts[2]); }
            default -> throw new IllegalArgumentException("Invalid object name: " + String.join(".", parts));
        };
    }

    public static String databaseName(String[] parts, String database) {
        if (parts.length == 0) return database;
        if (parts.length == 1) return parts[0];
        if (parts.length == 2) { requireCatalog(parts[0]); return parts[1]; }
        throw new IllegalArgumentException("Invalid database name");
    }

    private static void requireCatalog(String name) {
        if (!Consts.HIVE_CATALOG_NAME.equals(name)) {
            throw new UnsupportedOperationException("catalog is not configured: " + name);
        }
    }

    private static SqlProcessResult result(List<String> names, List<String> types, List<Object[]> rows) {
        List<RelDataTypeField> fields = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) fields.add(DataTypeUtils.getRelDataTypeField(names.get(i), i, types.get(i)));
        return SqlProcessResult.of(Linq4j.asEnumerable(rows), fields);
    }
}
