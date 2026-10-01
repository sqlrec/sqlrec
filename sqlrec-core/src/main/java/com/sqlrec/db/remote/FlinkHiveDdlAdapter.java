package com.sqlrec.db.remote;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.executor.SqlProcessResult;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.ddl.*;
import org.apache.flink.sql.parser.ddl.resource.SqlResource;
import org.apache.flink.sql.parser.ddl.resource.SqlResourceType;
import org.apache.flink.sql.parser.dql.*;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.ShowCreateUtil;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.functions.SqlLikeUtils;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.thrift.transport.TTransportException;

import java.util.*;

/** Flink 1.19 SQL metadata adapter. Official HiveCatalog owns HMS serialization and writes.
 * No TableEnvironment, planner, executor or Flink job is created. */
public final class FlinkHiveDdlAdapter {
    private static CatalogManager catalogs;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(FlinkHiveDdlAdapter::closeCatalog, "sqlrec-hive-ddl"));
    }

    private FlinkHiveDdlAdapter() {}

    public static synchronized void executeDdl(String sql, String database) throws Exception {
        boolean submitting = false;
        try {
            SqlNode node = CompileManager.parseSql(sql);
            requirePersistentDdl(node);
            CatalogManager manager = catalogs();
            HiveCatalog catalog = hiveCatalog(manager);
            if (requiresCurrentDatabase(node) && !catalog.databaseExists(database)) {
                throw new IllegalArgumentException("Database does not exist: " + database);
            }
            // Prepare the definition before marking a write as submitted. Transport failures while
            // reading metadata can be retried, but a failed write may already have reached HMS.
            CatalogWrite write;
            if (node instanceof SqlCreateDatabase create) {
                String name = databaseName(create.fullDatabaseName(), database);
                var definition = new CatalogDatabaseImpl(FlinkTableDdl.properties(create.getPropertyList()),
                        create.getComment().map(FlinkHiveDdlAdapter::literal).orElse(null));
                write = () -> catalog.createDatabase(name, definition, create.isIfNotExists());
            } else if (node instanceof SqlAlterDatabase alter) {
                String name = databaseName(alter.fullDatabaseName(), database);
                CatalogDatabase previous = catalog.getDatabase(name);
                Map<String, String> properties = new HashMap<>(previous.getProperties());
                properties.putAll(FlinkTableDdl.properties(alter.getPropertyList()));
                var definition = new CatalogDatabaseImpl(properties, previous.getComment());
                write = () -> catalog.alterDatabase(name, definition, false);
            } else if (node instanceof SqlDropDatabase drop) {
                String name = databaseName(drop.fullDatabaseName(), database);
                if (name.equals(database) && catalog.databaseExists(name)) {
                    throw new ValidationException("Cannot drop the current database; USE another database first: " + name);
                }
                write = () -> catalog.dropDatabase(name, drop.getIfExists(), drop.isCascade());
            } else if (node instanceof SqlCreateTable create) {
                ObjectPath path = objectPath(create.fullTableName(), database);
                ResolvedCatalogTable definition = FlinkTableDdl.create(create, catalog, manager.getDataTypeFactory(), database);
                write = () -> catalog.createTable(path, definition, create.isIfNotExists());
            } else if (node instanceof SqlDropTable drop) {
                ObjectPath path = objectPath(drop.fullTableName(), database);
                write = () -> catalog.dropTable(path, drop.getIfExists());
            } else if (node instanceof SqlAlterTable alter) {
                ObjectPath path = objectPath(alter.fullTableName(), database);
                if (alter.ifTableExists() && !catalog.tableExists(path)) return;
                if (alter instanceof SqlAlterTableRename rename) {
                    ObjectPath target = objectPath(rename.fullNewTableName(), path.getDatabaseName());
                    if (!path.getDatabaseName().equals(target.getDatabaseName())) {
                        throw new UnsupportedOperationException("Renaming a table across databases is not supported");
                    }
                    write = () -> catalog.renameTable(path, target.getObjectName(), alter.ifTableExists());
                } else if (alter instanceof SqlAddPartitions add) {
                    write = () -> {
                        for (int i = 0; i < add.getPartSpecs().size(); i++) {
                            catalog.createPartition(path, new CatalogPartitionSpec(add.getPartitionKVs(i)),
                                    new CatalogPartitionImpl(FlinkTableDdl.properties(add.getPartProps().get(i)), null),
                                    add.ifPartitionNotExists());
                        }
                    };
                } else if (alter instanceof SqlDropPartitions drop) {
                    write = () -> {
                        for (int i = 0; i < drop.getPartSpecs().size(); i++) {
                            catalog.dropPartition(path, new CatalogPartitionSpec(drop.getPartitionKVs(i)), drop.ifExists());
                        }
                    };
                } else if (alter instanceof SqlAlterTableOptions options && alter.getPartitionSpec() != null) {
                    var spec = new CatalogPartitionSpec(alter.getPartitionKVs());
                    CatalogPartition previous = catalog.getPartition(path, spec);
                    Map<String, String> properties = new HashMap<>(previous.getProperties());
                    properties.putAll(FlinkTableDdl.properties(options.getPropertyList()));
                    var definition = new CatalogPartitionImpl(properties, previous.getComment());
                    write = () -> catalog.alterPartition(path, spec, definition, false);
                } else {
                    CatalogBaseTable previous = catalog.getTable(path);
                    ResolvedCatalogTable definition = FlinkTableDdl.alter(alter, previous, manager.getDataTypeFactory());
                    write = () -> catalog.alterTable(path, definition, alter.ifTableExists());
                }
            } else if (node instanceof SqlCreateFunction create) {
                ObjectPath path = objectPath(create.getFunctionIdentifier(), database);
                List<ResourceUri> resources = create.getResourceInfos().stream().map(SqlResource.class::cast)
                        .map(resource -> new ResourceUri(ResourceType.valueOf(
                                resource.getResourceType().getValueAs(SqlResourceType.class).name()),
                                literal(resource.getResourcePath()))).toList();
                var definition = new CatalogFunctionImpl(literal(create.getFunctionClassName()),
                        language(create.getFunctionLanguage()), resources);
                write = () -> catalog.createFunction(path, definition, create.isIfNotExists());
            } else if (node instanceof SqlAlterFunction alter) {
                ObjectPath path = objectPath(alter.getFunctionIdentifier(), database);
                var definition = new CatalogFunctionImpl(literal(alter.getFunctionClassName()), language(alter.getFunctionLanguage()));
                write = () -> catalog.alterFunction(path, definition, alter.isIfExists());
            } else if (node instanceof SqlDropFunction drop) {
                ObjectPath path = objectPath(drop.getFunctionIdentifier(), database);
                write = () -> catalog.dropFunction(path, drop.getIfExists());
            } else {
                throw unsupported(node);
            }
            submitting = true;
            write.execute();
        } catch (Exception e) {
            if (hasTransportFailure(e)) {
                discardCatalog(e);
                if (submitting) {
                    throw new IllegalStateException("METADATA_WRITE_OUTCOME_UNKNOWN: connection lost while writing HMS; "
                            + "read back the object before retrying this DDL", e);
                }
            }
            throw e instanceof RuntimeException runtime ? runtime : new TableException(e.getMessage(), e);
        }
    }

    public static synchronized SqlProcessResult executeQuery(String sql, String database) throws Exception {
        try {
            SqlNode node = CompileManager.parseSql(sql);
            CatalogManager manager = catalogs();
            HiveCatalog catalog = hiveCatalog(manager);
            if (node instanceof SqlShowCreateTable show) {
                ObjectPath path = objectPath(show.getTableName().names.toArray(String[]::new), database);
                var definition = FlinkTableDdl.resolve(catalog.getTable(path), manager.getDataTypeFactory());
                String ddl = ShowCreateUtil.buildShowCreateTableRow(definition,
                        ObjectIdentifier.of(Consts.HIVE_CATALOG_NAME, path.getDatabaseName(), path.getObjectName()), false);
                return result(List.of("result"), List.of("STRING"), Collections.singletonList(new Object[]{ddl}));
            } else if (node instanceof SqlRichDescribeTable describe) {
                ObjectPath path = objectPath(describe.fullTableName(), database);
                var schema = FlinkTableDdl.resolve(catalog.getTable(path), manager.getDataTypeFactory()).getResolvedSchema();
                List<String> names = new ArrayList<>(List.of("name", "type", "null", "key", "extras", "watermark"));
                List<String> types = new ArrayList<>(List.of("STRING", "STRING", "BOOLEAN", "STRING", "STRING", "STRING"));
                boolean comments = schema.getColumns().stream().anyMatch(column -> column.getComment().isPresent());
                if (comments) { names.add("comment"); types.add("STRING"); }
                List<Object[]> rows = new ArrayList<>();
                for (Column column : schema.getColumns()) {
                    String key = schema.getPrimaryKey().filter(pk -> pk.getColumns().contains(column.getName()))
                            .map(pk -> "PRI(" + String.join(", ", pk.getColumns()) + ")").orElse(null);
                    List<Object> values = new ArrayList<>(Arrays.asList(column.getName(),
                            column.getDataType().getLogicalType().copy(true).asSummaryString(),
                            column.getDataType().getLogicalType().isNullable(), key, column.explainExtras().orElse(null), null));
                    if (comments) values.add(column.getComment().orElse(null));
                    rows.add(values.toArray());
                }
                return result(names, types, rows);
            } else if (node instanceof SqlShowFunctions show) {
                String name = databaseName(show.fullDatabaseName(), database);
                Set<String> functions = new TreeSet<>(catalog.listFunctions(name));
                if (!show.requireUser()) functions.addAll(CoreModule.INSTANCE.listFunctions());
                List<Object[]> rows = functions.stream().filter(function -> !show.isWithLike()
                                || show.isNotLike() != ("ILIKE".equals(show.getLikeType())
                                ? SqlLikeUtils.ilike(function, show.getLikeSqlPattern(), "\\")
                                : SqlLikeUtils.like(function, show.getLikeSqlPattern(), "\\")))
                        .map(function -> new Object[]{function}).toList();
                return result(List.of("function name"), List.of("STRING"), rows);
            }
            throw unsupported(node);
        } catch (Exception e) {
            if (hasTransportFailure(e)) discardCatalog(e);
            throw e instanceof RuntimeException runtime ? runtime : new TableException(e.getMessage(), e);
        }
    }

    private static CatalogManager catalogs() {
        if (catalogs != null) return catalogs;
        HiveConf conf = new HiveConf();
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, SqlRecConfigs.HIVE_METASTORE_URI.getValue());
        conf.set("hive.metastore.execute.setugi", SqlRecConfigs.EXECUTE_SET_UGI.getValue());
        var catalog = new HiveCatalog(Consts.HIVE_CATALOG_NAME, Consts.DEFAULT_SCHEMA_NAME, conf, Consts.HIVE_CLIENT_VERSION);
        try {
            catalog.open();
            Configuration config = new Configuration();
            ClassLoader loader = FlinkHiveDdlAdapter.class.getClassLoader();
            // CatalogManager supplies Flink's type factory without initializing any planner.
            catalogs = CatalogManager.newBuilder().classLoader(loader).config(config)
                    .defaultCatalog(Consts.HIVE_CATALOG_NAME, catalog)
                    .catalogStoreHolder(CatalogStoreHolder.newBuilder().catalogStore(new GenericInMemoryCatalogStore())
                            .config(config).classloader(loader).build()).build();
            return catalogs;
        } catch (RuntimeException e) {
            try { catalog.close(); } catch (RuntimeException closeFailure) { e.addSuppressed(closeFailure); }
            throw e;
        }
    }

    private static HiveCatalog hiveCatalog(CatalogManager manager) {
        return (HiveCatalog) manager.getCatalog(Consts.HIVE_CATALOG_NAME).orElseThrow();
    }

    static void requirePersistentDdl(SqlNode node) {
        if (node instanceof SqlCreateTable create) {
            if (create.isTemporary() || (create.getClass() != SqlCreateTable.class && !(create instanceof SqlCreateTableLike))) {
                throw unsupported(node);
            }
            FlinkTableDdl.requireSimpleSchema(create.getColumnList(), create.getWatermark().isPresent());
        } else if (node instanceof SqlAlterTableSchema alter) {
            if (alter.getWatermark().isPresent()) FlinkTableDdl.requireSimpleSchema(SqlNodeList.EMPTY, true);
            for (SqlNode position : alter.getColumnPositions()) {
                var column = ((org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition) position).getColumn();
                FlinkTableDdl.requireSimpleSchema(new SqlNodeList(List.of(column), column.getParserPosition()), false);
            }
        } else if (node instanceof SqlDropTable drop) {
            if (drop.isTemporary()) throw unsupported(node);
        } else if (node instanceof SqlCreateFunction create) {
            if (create.isTemporary() || create.isSystemFunction()) throw unsupported(node);
        } else if (node instanceof SqlAlterFunction alter) {
            if (alter.isTemporary() || alter.isSystemFunction()) throw unsupported(node);
        } else if (node instanceof SqlDropFunction drop) {
            if (drop.isTemporary() || drop.isSystemFunction()) throw unsupported(node);
        } else if (!(node instanceof SqlCreateDatabase) && !(node instanceof SqlAlterDatabase)
                && !(node instanceof SqlDropDatabase) && !(node instanceof SqlAlterTable)) {
            throw unsupported(node);
        }
        // Reject foreign catalogs before opening the HMS connection.
        if (node instanceof SqlCreateTable create) objectPath(create.fullTableName(), Consts.DEFAULT_SCHEMA_NAME);
        else if (node instanceof SqlAlterTable alter) objectPath(alter.fullTableName(), Consts.DEFAULT_SCHEMA_NAME);
        else if (node instanceof SqlDropTable drop) objectPath(drop.fullTableName(), Consts.DEFAULT_SCHEMA_NAME);
        else if (node instanceof SqlCreateFunction create) objectPath(create.getFunctionIdentifier(), Consts.DEFAULT_SCHEMA_NAME);
        else if (node instanceof SqlAlterFunction alter) objectPath(alter.getFunctionIdentifier(), Consts.DEFAULT_SCHEMA_NAME);
        else if (node instanceof SqlDropFunction drop) objectPath(drop.getFunctionIdentifier(), Consts.DEFAULT_SCHEMA_NAME);
        else if (node instanceof SqlCreateDatabase create) databaseName(create.fullDatabaseName(), Consts.DEFAULT_SCHEMA_NAME);
        else if (node instanceof SqlAlterDatabase alter) databaseName(alter.fullDatabaseName(), Consts.DEFAULT_SCHEMA_NAME);
        else if (node instanceof SqlDropDatabase drop) databaseName(drop.fullDatabaseName(), Consts.DEFAULT_SCHEMA_NAME);
    }

    static ObjectPath objectPath(String[] parts, String database) {
        return switch (parts.length) {
            case 1 -> new ObjectPath(database, parts[0]);
            case 2 -> new ObjectPath(parts[0], parts[1]);
            case 3 -> { requireCatalog(parts[0]); yield new ObjectPath(parts[1], parts[2]); }
            default -> throw new IllegalArgumentException("Invalid object name: " + String.join(".", parts));
        };
    }

    private static String databaseName(String[] parts, String database) {
        if (parts.length == 0) return database;
        if (parts.length == 1) return parts[0];
        if (parts.length == 2) { requireCatalog(parts[0]); return parts[1]; }
        throw new IllegalArgumentException("Invalid database name");
    }

    private static void requireCatalog(String name) {
        if (!Consts.HIVE_CATALOG_NAME.equals(name)) {
            throw new UnsupportedOperationException("UNSUPPORTED_METADATA_DDL: catalog is not configured: " + name);
        }
    }

    static boolean requiresCurrentDatabase(SqlNode node) {
        if (node instanceof SqlCreateDatabase || node instanceof SqlAlterDatabase || node instanceof SqlDropDatabase) return false;
        if (node instanceof SqlCreateTable create) {
            return create.fullTableName().length == 1 || (create instanceof SqlCreateTableLike like
                    && like.getTableLike().getSourceTable().names.size() == 1);
        }
        if (node instanceof SqlAlterTable alter) return alter.fullTableName().length == 1;
        if (node instanceof SqlDropTable drop) return drop.fullTableName().length == 1;
        if (node instanceof SqlCreateFunction create) return create.getFunctionIdentifier().length == 1;
        if (node instanceof SqlAlterFunction alter) return alter.getFunctionIdentifier().length == 1;
        if (node instanceof SqlDropFunction drop) return drop.getFunctionIdentifier().length == 1;
        return false;
    }

    static String literal(SqlNode node) { return ((SqlLiteral) node).getValueAs(String.class); }

    private static FunctionLanguage language(String value) {
        return value == null ? FunctionLanguage.JAVA : FunctionLanguage.valueOf(value.toUpperCase(Locale.ROOT));
    }

    private static SqlProcessResult result(List<String> names, List<String> types, List<Object[]> rows) {
        List<RelDataTypeField> fields = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) fields.add(DataTypeUtils.getRelDataTypeField(names.get(i), i, types.get(i)));
        return SqlProcessResult.of(Linq4j.asEnumerable(rows), fields);
    }

    private static UnsupportedOperationException unsupported(SqlNode node) {
        return new UnsupportedOperationException("UNSUPPORTED_METADATA_DDL: " + node.getClass().getSimpleName()
                + "; only persistent table, database and UDF metadata operations are supported");
    }

    private static boolean hasTransportFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof TTransportException) return true;
        }
        return false;
    }

    private static void discardCatalog(Exception error) {
        try { closeCatalog(); } catch (RuntimeException closeFailure) { error.addSuppressed(closeFailure); }
    }

    private static synchronized void closeCatalog() {
        CatalogManager previous = catalogs;
        catalogs = null;
        if (previous != null) previous.close();
    }

    @FunctionalInterface
    private interface CatalogWrite { void execute() throws Exception; }

}
